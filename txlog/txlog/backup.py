"""Online backup repository and offline restore for txlog.

A *backup repository* is a self-contained directory on the local POSIX file
system.  It is independent of the live log directory and of any downstream
materialized database: no remote service is involved and the whole directory
can be moved to another disk as-is.

Design summary
--------------
* **One recovery point = one immutable manifest** (``points/<name>.json``),
  published atomically.  A name, once published, can never be overwritten;
  an unpublished point never appears in the listing, and a process exiting
  mid-publication leaves only a removable job directory behind.
* **Segment bodies are kept as content-addressed, bounded-size blocks**
  (``chunks/``).  Two recovery points of the same source log that include
  the same byte range share the exact same blocks; a grown active segment
  only writes the blocks that actually changed (its still-filling last
  block gets a new version).  Nothing is compressed, so "reused" bytes are
  real shared bytes, never a compression ratio.
* **Capture never blocks the source.**  The source metadata is read in one
  read-only SQLite snapshot transaction while a *shared* retain flock is
  held; every segment the view needs is preserved immediately with a
  ``link(2)`` hard link (falling back to a shared-lock bounded copy on
  cross-device setups).  The locks are released before any hashing or
  block copying, so the source can keep appending, rolling segments and
  deleting eligible old segments while the backup proceeds.  Only the
  bytes the captured view confirms are ever read; later appends, commits,
  acks, retention and new partitions cannot enter the point.
* **Raw bytes only.**  Segment bodies are archived and frame-validated but
  never interpreted into writes; ``read_committed`` results are never fed
  back into a log.  Pending transactions stay pending, abort holes keep
  their positions, and records appended after a pending transaction are
  preserved.
* **Restore uses the repository alone.**  The original directory, its WAL
  and its open file descriptors are not needed.  A fresh metadata database
  is rebuilt from the snapshot, the restored log gets a NEW durable
  ``log_id`` (the original identity and point name are reported), and the
  result reopens through the ordinary txlog API.
"""

import fcntl
import glob
import hashlib
import json
import os
import shutil
import sqlite3
import threading
import time
import urllib.parse
import uuid
from contextlib import contextmanager

from . import db, storage
from .errors import (BackupBusy, BackupPointExists, BackupPointNotFound,
                     BackupTargetExists, BackupUnsupportedVersion,
                     BackupVerificationError, CorruptionError)
from .faults import FaultInjector
from .log import FORMAT_VERSION as LOG_FORMAT_VERSION, Log

BACKUP_FORMAT = "1"
CHUNK_BYTES = 4 * 1024 * 1024  # bounded blocks; whole segments are never loaded

_POINTS = "points"
_JOBS = "jobs"
_CHUNKS = "chunks"
_META = "meta"
_LOCKS = "locks"
_LINKS = "links"

# Tables copied, in FK-independent order, with their column lists.
_SNAPSHOT_TABLES = (
    "meta", "producers", "transactions", "txn_parts", "dedup", "seq_state",
    "partitions", "segments", "readers", "reader_positions", "reader_acks",
)

_NAME_OK = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


# ---------------------------------------------------------------------------
# small helpers

def _validate_name(name):
    if not isinstance(name, str) or not name or name in (".", ".."):
        raise ValueError("recovery point name must be a non-empty string")
    if len(name) > 127 or any(c not in _NAME_OK for c in name):
        raise ValueError(
            "recovery point name may only contain letters, digits and ._- "
            "(max 127 chars)")
    if name[0] in ".-":
        raise ValueError("recovery point name must start with a letter/digit")


def _fsync_dir(path):
    storage.fsync_dir(path)


def _read_only_connect(db_path):
    uri = "file:%s?mode=ro" % urllib.parse.quote(db_path, safe="/")
    con = sqlite3.connect(uri, uri=True, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.isolation_level = None
    con.execute("PRAGMA busy_timeout = 30000")
    return con


def _problem(kind, point, message, **where):
    out = {"kind": kind, "point": point, "message": message}
    out.update(where)
    return out


# ---------------------------------------------------------------------------
# streaming frame validator over an exact byte range

class _FrameValidator:
    """Validate the length-framed CRC32 record stream of one segment view.

    Bytes are fed in bounded chunks (the same chunks that get archived), so
    a large segment is never read into memory at once.  The fed length must
    be exactly the view's confirmed byte count: a torn frame inside or at
    the end of that range is corruption, never a clean truncation.
    """

    def __init__(self, base_offset, view_bytes, point, partition, rel_path):
        self.base = base_offset
        self.view_bytes = view_bytes
        self.point = point
        self.partition = partition
        self.rel_path = rel_path
        self._buf = b""
        self._fed = 0
        self._next_offset = base_offset
        self.records = 0
        self.last_offset = None
        self.problems = []

    def _fail(self, msg, byte_range=None):
        kw = {"partition": self.partition, "segment": self.rel_path}
        if byte_range is not None:
            kw["byte_range"] = byte_range
        self.problems.append(
            _problem("corrupt", self.point, msg, **kw))

    def feed(self, data):
        self._fed += len(data)
        self._buf += data
        pos = 0
        while pos < len(self._buf):
            try:
                rec, end = storage.parse_one(self._buf, pos)
            except storage.TornRecord as e:
                msg = str(e)
                # A checksum mismatch or bad framing inside the confirmed
                # range is definite corruption: only a genuinely short tail
                # may be completed by a later feed.
                if not (msg.startswith("short header")
                        or msg.startswith("short record")):
                    self._fail(msg, byte_range=[
                        self._fed - len(self._buf) + pos, self._fed])
                    return
                break
            if rec.offset != self._next_offset:
                self._fail(
                    f"offset gap: expected {self._next_offset}, found "
                    f"{rec.offset}",
                    byte_range=[self._fed - len(self._buf) + pos,
                                self._fed - len(self._buf) + end])
                return
            self._next_offset = rec.offset + 1
            self.last_offset = rec.offset
            self.records += 1
            pos = end
        self._buf = self._buf[pos:]

    def finish(self):
        if self._fed != self.view_bytes:
            self._fail(
                f"confirmed bytes missing: view needs {self.view_bytes}, "
                f"read {self._fed}")
        if self._buf:
            self._fail(
                f"torn frame inside confirmed byte range "
                f"({len(self._buf)} trailing bytes)",
                byte_range=[self._fed - len(self._buf), self._fed])
        return self.problems


# ---------------------------------------------------------------------------
# repository

class BackupRepository:
    """A directory holding recovery points, shared blocks and metadata."""

    def __init__(self, path, *, create=True, fault_hook=None):
        self.path = os.path.abspath(path)
        self._fault = FaultInjector(fault_hook)
        self._mu = threading.RLock()
        if not os.path.isdir(self.path):
            if not create:
                raise BackupPointNotFound(
                    f"backup repository {self.path!r} does not exist")
            os.makedirs(self.path)
        for sub in (_POINTS, _JOBS, _CHUNKS, _META, _LOCKS):
            os.makedirs(os.path.join(self.path, sub), exist_ok=True)
        self._lock_path = os.path.join(self.path, "repo.lock")
        self._repo_json = os.path.join(self.path, "repo.json")
        if not os.path.exists(self._repo_json):
            doc = {"format": BACKUP_FORMAT,
                   "created_at": time.time(),
                   "repository_id": uuid.uuid4().hex}
            self._atomic_write(self._repo_json,
                               json.dumps(doc, indent=2, sort_keys=True))
        else:
            with open(self._repo_json, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            if doc.get("format") != BACKUP_FORMAT:
                raise BackupUnsupportedVersion(
                    f"repository format {doc.get('format')!r} is not "
                    f"supported (this build supports {BACKUP_FORMAT!r})")

    # -- locking -----------------------------------------------------------

    @contextmanager
    def _repo_lock(self, exclusive, block=True):
        """Repository-wide flock.  Exclusive for publishing/deletion/gc,
        shared for verify/restore, so bytes in use can never be removed."""
        flag = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not block:
            flag |= fcntl.LOCK_NB
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            try:
                fcntl.flock(fd, flag)
            except BlockingIOError:
                raise BackupBusy(
                    "another repository operation holds an incompatible "
                    "lock; retry once it finishes")
            with self._mu:
                yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def _lease(self, op, point):
        """Marker describing an in-progress verify/restore; deletes report
        live leases instead of removing referenced blocks.  A shared flock
        on the marker makes liveness exact: process death releases it, so a
        crashed lease can never block deletion permanently."""
        token = uuid.uuid4().hex
        marker = os.path.join(self.path, _LOCKS, token + ".json")
        doc = {"op": op, "point": point, "pid": os.getpid(),
               "started_at": time.time()}
        self._atomic_write(marker, json.dumps(doc, sort_keys=True))
        fd = os.open(marker, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield marker
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            try:
                os.unlink(marker)
            except FileNotFoundError:
                pass

    def _leases(self):
        """Only leases whose flock is currently held by a live process."""
        out = []
        ldir = os.path.join(self.path, _LOCKS)
        for fn in os.listdir(ldir):
            if not fn.endswith(".json"):
                continue
            marker = os.path.join(ldir, fn)
            try:
                fd = os.open(marker, os.O_RDWR)
            except OSError:
                continue
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    try:
                        with open(marker, "r", encoding="utf-8") as fh:
                            out.append(json.load(fh))
                    except (OSError, ValueError):
                        out.append({"op": "unknown", "point": None,
                                    "marker": fn})
                    continue
                # Not locked: leftover from a crashed process.
                fcntl.flock(fd, fcntl.LOCK_UN)
                try:
                    os.unlink(marker)
                except OSError:
                    pass
            finally:
                os.close(fd)
        return out

    # -- atomic json io ----------------------------------------------------

    def _atomic_write(self, full_path, text):
        d = os.path.dirname(full_path)
        os.makedirs(d, exist_ok=True)
        tmp = os.path.join(d, ".tmp-%s-%d" % (uuid.uuid4().hex, os.getpid()))
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.rename(tmp, full_path)
        _fsync_dir(d)

    def _manifest_path(self, name):
        return os.path.join(self.path, _POINTS, name + ".json")

    def _load_manifest(self, name):
        path = self._manifest_path(name)
        if not os.path.exists(path):
            raise BackupPointNotFound(
                f"recovery point {name!r} does not exist in {self.path!r}")
        try:
            with open(path, "r", encoding="utf-8") as fh:
                manifest = json.load(fh)
        except (OSError, ValueError) as e:
            raise BackupVerificationError(
                f"recovery point {name!r}: manifest unreadable: {e}",
                [_problem("corrupt", name, f"manifest unreadable: {e}")])
        if manifest.get("backup_format") != BACKUP_FORMAT:
            raise BackupUnsupportedVersion(
                f"recovery point {name!r} uses backup format "
                f"{manifest.get('backup_format')!r}; this build supports "
                f"{BACKUP_FORMAT!r}")
        return manifest

    # -- listing -----------------------------------------------------------

    def list_points(self):
        """All published recovery points with the source view each one
        captured (never the runtime's current progress)."""
        pdir = os.path.join(self.path, _POINTS)
        out = []
        for fn in sorted(os.listdir(pdir)):
            if not fn.endswith(".json"):
                continue
            try:
                with open(os.path.join(pdir, fn), "r",
                          encoding="utf-8") as fh:
                    m = json.load(fh)
            except (OSError, ValueError):
                continue
            out.append(self._summarize(m))
        return out

    @staticmethod
    def _summarize(m):
        return {
            "name": m["name"],
            "created_at": m["created_at"],
            "source": {
                "log_id": m["source"]["log_id"],
                "path": m["source"]["path"],
                "format": m["source"].get("format"),
                "segment_bytes": m["source"].get("segment_bytes"),
                "dedup_window": m["source"].get("dedup_window"),
            },
            "partitions": {
                str(p): {
                    "retained_below": v["retained_below"],
                    "next_offset": v["next_offset"],
                    "active_segment": v["active_segment"],
                    "last_stable_offset": v.get("last_stable_offset"),
                    "segments": v["segments"],
                }
                for p, v in m["partitions"].items()},
            "stats": m.get("stats", {}),
        }

    # -- capture -----------------------------------------------------------

    def create_backup(self, log, name, *, after_capture=None):
        """Capture an online recovery point from a running ``log``.

        The source keeps accepting appends, transaction decisions, reader
        acks and retention throughout.  ``after_capture`` (if given) is
        called with the captured view once the view is durable in the job
        directory and all source-coordinating locks have been released —
        this is the safe pause seam used by operators and tests.
        """
        _validate_name(name)
        # Authoritative name uniqueness is checked under the exclusive lock
        # at publication time; this early check is only a fast-path hint.
        if os.path.exists(self._manifest_path(name)):
            raise BackupPointExists(
                f"recovery point {name!r} already exists in "
                f"{self.path!r}; published points are immutable, choose "
                f"another name or delete it first")
        job_id = uuid.uuid4().hex
        job = os.path.join(self.path, _JOBS, job_id)
        links = os.path.join(job, _LINKS)
        tmp = os.path.join(job, "tmp")
        os.makedirs(links)
        os.makedirs(tmp)
        # new-vs-reused accounting is fixed at install time: by the time the
        # manifest is assembled every block exists, so disk checks then could
        # no longer tell shared bytes from freshly written ones.
        stats = {"blocks_new": 0, "blocks_reused": 0,
                 "bytes_new": 0, "bytes_reused": 0, "segment_bytes": 0}
        job_lock_fd = None
        try:
            # A flock on the job directory marks the publish as live; it is
            # released automatically if this process dies, so gc can tell a
            # stale job from an in-progress one without PID heuristics.
            job_lock_fd = os.open(os.path.join(job, "job.lock"),
                                  os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(job_lock_fd, fcntl.LOCK_EX)
            view, problems = self._capture_view(log, name, job, links)
            view["source_path"] = log.path
            view["stats"] = stats
            stats["segment_bytes"] = sum(
                s["bytes"] for e in view["partitions"].values()
                for s in e["segments"])
            if after_capture is not None:
                after_capture(view)
            self._fault.maybe("backup.after_capture")
            if problems:
                raise BackupVerificationError(
                    f"recovery point {name!r} not published: source view "
                    f"failed validation", problems)
            meta_sha, meta_bytes = self._build_metadata(log, name, job, tmp,
                                                        view["snapshot"])
            problems = self._archive_segments(log, name, job, tmp, view)
            if problems:
                raise BackupVerificationError(
                    f"recovery point {name!r} not published: segment bytes "
                    f"failed validation", problems)
            manifest = self._assemble_manifest(name, view, meta_sha,
                                               meta_bytes)
            self._fault.maybe("backup.before_publish")
            with self._repo_lock(exclusive=True, block=True):
                if os.path.exists(self._manifest_path(name)):
                    raise BackupPointExists(
                        f"recovery point {name!r} already exists in "
                        f"{self.path!r}")
                self._atomic_write(
                    self._manifest_path(name),
                    json.dumps(manifest, indent=2, sort_keys=True))
            self._fault.maybe("backup.after_publish")
            return self._summarize(manifest)
        finally:
            if job_lock_fd is not None:
                try:
                    fcntl.flock(job_lock_fd, fcntl.LOCK_UN)
                    os.close(job_lock_fd)
                except OSError:
                    pass
            shutil.rmtree(job, ignore_errors=True)

    def _capture_view(self, log, name, job, links):
        """Take the metadata snapshot and preserve every needed segment.

        Coordination is brief: one read-only SQLite snapshot transaction
        plus the shared retain flock.  Hard links make the view's bytes
        independent of the source directory before the flock is dropped.
        """
        problems = []
        con = _read_only_connect(log._dbpath)
        try:
            with log._retain_shared():
                con.execute("BEGIN")
                try:
                    snapshot = self._dump_snapshot(con)
                    # stat + preserve every live segment while the view is
                    # stable; no body bytes are copied under this lock.
                    ci = {t: snapshot[t]["columns"] for t in snapshot}
                    st_i = ci["segments"].index("state")
                    for s in snapshot["segments"]["rows"]:
                        if s[st_i] == "deleted":
                            continue
                        self._preserve_segment(log, name,
                                               self._row_dict(snapshot,
                                                              "segments", s),
                                               links, problems)
                    self._check_catalog_vs_dir(log, name, snapshot, problems)
                finally:
                    con.execute("ROLLBACK")
        finally:
            con.close()
        view = {"name": name, "snapshot": snapshot,
                "captured_at": time.time()}
        view.update(self._build_partition_view(name, snapshot, problems))
        return view, problems

    @staticmethod
    def _dump_snapshot(con):
        snap = {}
        for table in _SNAPSHOT_TABLES:
            cols = [r["name"] for r in con.execute(
                "PRAGMA table_info(%s)" % table)]
            rows = [list(r) for r in
                    con.execute("SELECT * FROM %s" % table)]
            snap[table] = {"columns": cols, "rows": rows}
        return snap

    @staticmethod
    def _row_dict(snapshot, table, row):
        return dict(zip(snapshot[table]["columns"], row))

    def _preserve_segment(self, log, name, seg, links, problems):
        rel = seg["path"]
        src = os.path.join(log.path, rel)
        dst = os.path.join(links, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        try:
            st = os.stat(src)
        except FileNotFoundError:
            problems.append(_problem(
                "missing", name,
                f"catalog references missing segment {rel}",
                partition=seg["partition"], segment=rel,
                byte_range=[0, seg["bytes"]]))
            return
        if st.st_size < seg["bytes"]:
            problems.append(_problem(
                "missing", name,
                f"confirmed bytes missing: catalog needs {seg['bytes']} "
                f"bytes but {rel} has {st.st_size}",
                partition=seg["partition"], segment=rel,
                byte_range=[0, seg["bytes"]]))
            return
        # Hardlink keeps the view's bytes independent of the source
        # directory without copying bodies under the coordination lock;
        # cross-device repositories fall back to a bounded shared-lock
        # copy (retain still cannot unlink the file underneath us, and
        # appends never take the retain flock).
        try:
            self._link_or_copy(src, dst)
        except OSError:
            with open(src, "rb") as inp, open(dst, "wb") as out:
                self._copy_range(inp, out, 0, seg["bytes"])
                out.flush()
                os.fsync(out.fileno())

    @staticmethod
    def _copy_range(inp, out, offset, length):
        inp.seek(offset)
        remaining = length
        while remaining:
            chunk = inp.read(min(CHUNK_BYTES, remaining))
            if not chunk:
                raise EOFError("short segment copy")
            out.write(chunk)
            remaining -= len(chunk)

    def _check_catalog_vs_dir(self, log, name, snapshot, problems):
        cols = snapshot["segments"]["columns"]
        path_i = cols.index("path")
        known_paths = {s[path_i] for s in snapshot["segments"]["rows"]}
        part_ids = {r[0] for r in snapshot["partitions"]["rows"]}
        for p in part_ids:
            pdir = os.path.join(log.path,
                                storage.partition_dirname(p))
            try:
                files = sorted(f for f in os.listdir(pdir)
                               if f.endswith(".seg"))
            except FileNotFoundError:
                problems.append(_problem(
                    "corrupt", name,
                    f"partition directory p{p} is missing",
                    partition=p))
                continue
            for fn in files:
                rel = os.path.join(storage.partition_dirname(p), fn)
                if rel in known_paths:
                    continue
                # A stray non-empty file not in the catalog means the view
                # and the directory do not correspond.
                full = os.path.join(pdir, fn)
                try:
                    size = os.path.getsize(full)
                except OSError:
                    size = -1
                if size > 0:
                    problems.append(_problem(
                        "corrupt", name,
                        f"segment file {rel} exists on disk but is not in "
                        f"the captured catalog", partition=p, segment=rel))

    def _build_partition_view(self, name, snapshot, problems):
        rows = {t: snapshot[t]["rows"] for t in snapshot}
        cols = {t: snapshot[t]["columns"] for t in snapshot}

        def idx(table, col):
            return cols[table].index(col)

        partitions = {}
        seg_by_part = {}
        for s in rows["segments"]:
            if s[idx("segments", "state")] == "deleted":
                continue
            p = s[idx("segments", "partition")]
            seg_by_part.setdefault(p, []).append(s)

        lso = {}
        open_first = {}
        for t in rows["txn_parts"]:
            txn_id = t[cols["txn_parts"].index("txn_id")]
            state = self._txn_state(rows, cols, txn_id)
            if state != "open":
                continue
            p = t[cols["txn_parts"].index("partition")]
            first = t[cols["txn_parts"].index("first_offset")]
            open_first[p] = min(open_first.get(p, first), first)

        for prow in rows["partitions"]:
            p = prow[idx("partitions", "partition")]
            next_off = prow[idx("partitions", "next_offset")]
            retained = prow[idx("partitions", "retained_below")]
            active = prow[idx("partitions", "active_segment")]
            segs = sorted(seg_by_part.get(p, []),
                          key=lambda s: s[idx("segments", "base_offset")])
            entry = {"retained_below": retained, "next_offset": next_off,
                     "active_segment": active,
                     "last_stable_offset": open_first.get(p, next_off),
                     "segments": []}
            expect = None
            for s in segs:
                base = s[idx("segments", "base_offset")]
                max_off = s[idx("segments", "max_offset")]
                nbytes = s[idx("segments", "bytes")]
                state = s[idx("segments", "state")]
                rel = s[idx("segments", "path")]
                if expect is None:
                    expect = base
                if base != expect:
                    problems.append(_problem(
                        "corrupt", name,
                        f"segment base gap: expected {expect}, found {base}",
                        partition=p, segment=rel))
                if state == "sealed" and (max_off < 0 or nbytes <= 0):
                    problems.append(_problem(
                        "corrupt", name,
                        "sealed segment is empty", partition=p,
                        segment=rel))
                entry["segments"].append({
                    "base_offset": base, "max_offset": max_off,
                    "bytes": nbytes, "state": state, "path": rel})
                expect = max_off + 1 if max_off >= 0 else base
            if not segs:
                problems.append(_problem(
                    "corrupt", name,
                    "partition has no live segments in the captured view",
                    partition=p))
            elif expect != next_off:
                problems.append(_problem(
                    "corrupt", name,
                    f"catalog next_offset={next_off} does not match segment "
                    f"contents ending at {expect}", partition=p))
            if retained > next_off:
                problems.append(_problem(
                    "corrupt", name,
                    f"retained_below={retained} past next_offset={next_off}",
                    partition=p))
            partitions[str(p)] = entry
        return {"partitions": partitions}

    @staticmethod
    def _txn_state(rows, cols, txn_id):
        ti = cols["transactions"].index("txn_id")
        si = cols["transactions"].index("state")
        for r in rows["transactions"]:
            if r[ti] == txn_id:
                return r[si]
        return None

    # -- metadata snapshot database ---------------------------------------

    def _build_metadata(self, log, name, job, tmp, snapshot):
        """Rebuild a clean, WAL-free SQLite database holding exactly the
        captured snapshot; it depends on nothing in the source directory."""
        meta_path = os.path.join(tmp, "meta-%s.sqlite3" % uuid.uuid4().hex)
        con = db.connect(meta_path)
        try:
            db.init_schema(con)
            con.execute("BEGIN IMMEDIATE")
            for table in _SNAPSHOT_TABLES:
                spec = snapshot[table]
                placeholders = ",".join("?" for _ in spec["columns"])
                collist = ",".join(spec["columns"])
                con.executemany(
                    "INSERT INTO %s (%s) VALUES (%s)"
                    % (table, collist, placeholders),
                    spec["rows"])
            con.execute("COMMIT")
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            con.close()
        h = hashlib.sha256()
        size = 0
        with open(meta_path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK_BYTES)
                if not chunk:
                    break
                h.update(chunk)
                size += len(chunk)
        sha = h.hexdigest()
        d = os.path.join(self.path, _META, sha[:2])
        os.makedirs(d, exist_ok=True)
        target = os.path.join(d, sha)
        if not os.path.exists(target):
            self._link_or_copy(meta_path, target)
            _fsync_dir(d)
        return sha, size

    # -- block archive -----------------------------------------------------

    def _archive_segments(self, log, name, job, tmp, view):
        problems = []
        links = os.path.join(job, _LINKS)
        stats = view["stats"]
        for p, entry in view["partitions"].items():
            for s in entry["segments"]:
                rel = s["path"]
                link_path = os.path.join(links, rel)
                if not os.path.exists(link_path):
                    problems.append(_problem(
                        "missing", name,
                        "preserved segment copy disappeared before archival",
                        partition=int(p), segment=rel,
                        byte_range=[0, s["bytes"]]))
                    continue
                validator = _FrameValidator(
                    s["base_offset"], s["bytes"], name, int(p), rel)
                blocks = self._archive_range(link_path, s["bytes"], job, tmp,
                                              validator, stats)
                problems.extend(validator.finish())
                if validator.last_offset != s["max_offset"] and \
                        s["max_offset"] >= 0:
                    problems.append(_problem(
                        "corrupt", name,
                        f"catalog max_offset={s['max_offset']} but frames "
                        f"end at {validator.last_offset}",
                        partition=int(p), segment=rel))
                if s["state"] == "active" and s["max_offset"] < 0 and \
                        validator.records:
                    problems.append(_problem(
                        "corrupt", name,
                        "active segment cataloged empty but contains frames",
                        partition=int(p), segment=rel))
                s["blocks"] = blocks
        return problems

    def _archive_range(self, path, length, job, tmp, validator=None,
                       stats=None):
        """Hash and install the bounded blocks covering [0, length)."""
        blocks = []
        fd = os.open(path, os.O_RDONLY)
        try:
            pos = 0
            while pos < length:
                take = min(CHUNK_BYTES, length - pos)
                data = os.pread(fd, take, pos)
                if len(data) != take:
                    raise CorruptionError(
                        f"short read in {path} at byte {pos}")
                if validator is not None:
                    validator.feed(data)
                sha = hashlib.sha256(data).hexdigest()
                reused = self._install_block(data, sha, job, tmp)
                if stats is not None:
                    if reused:
                        stats["blocks_reused"] += 1
                        stats["bytes_reused"] += len(data)
                    else:
                        stats["blocks_new"] += 1
                        stats["bytes_new"] += len(data)
                blocks.append({"sha256": sha, "offset": pos,
                               "length": len(data)})
                pos += take
        finally:
            os.close(fd)
        return blocks

    def _install_block(self, data, sha, job, tmp):
        """Write a content-addressed block once.  Returns True when the
        block already existed (shared with another recovery point)."""
        d = os.path.join(self.path, _CHUNKS, sha[:2])
        os.makedirs(d, exist_ok=True)
        target = os.path.join(d, sha)
        if os.path.exists(target):
            return True
        staged = os.path.join(tmp, "block-%s" % uuid.uuid4().hex)
        with open(staged, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(staged, target)
        except FileExistsError:
            os.unlink(staged)
            _fsync_dir(d)
            return True
        except OSError as e:
            # Filesystem without hardlink support: fall back to a copy.
            if e.errno not in (18, 1):
                raise
            shutil.copyfile(staged, target)
        os.unlink(staged)
        _fsync_dir(d)
        return False

    @staticmethod
    def _link_or_copy(src, dst):
        """Hardlink when possible; bounded-copy fallback otherwise."""
        try:
            os.link(src, dst)
        except FileExistsError:
            return
        except OSError as e:
            if e.errno not in (18, 1):  # EXDEV, EPERM
                raise
            shutil.copyfile(src, dst)

    # -- manifest ----------------------------------------------------------

    def _assemble_manifest(self, name, view, meta_sha, meta_bytes):
        snap = view["snapshot"]
        cols = {t: snap[t]["columns"] for t in snap}
        rows = {t: snap[t]["rows"] for t in snap}

        def meta_value(key):
            ki = cols["meta"].index("key")
            vi = cols["meta"].index("value")
            for r in rows["meta"]:
                if r[ki] == key:
                    return r[vi]
            return None

        log_id = meta_value("log_id")
        manifest = {
            "backup_format": BACKUP_FORMAT,
            "name": name,
            "created_at": view["captured_at"],
            "published_at": time.time(),
            "source": {
                "log_id": log_id,
                "path": view.get("source_path"),
                "format": meta_value("format"),
                "segment_bytes": int(meta_value("segment_bytes")),
                "dedup_window": int(meta_value("dedup_window")),
            },
            "metadata": {"sha256": meta_sha, "bytes": meta_bytes},
            "partitions": view["partitions"],
            "transactions": self._snapshot_table(snap, "transactions"),
            "txn_parts": self._snapshot_table(snap, "txn_parts"),
            "producers": self._snapshot_table(snap, "producers"),
            "readers": self._snapshot_table(snap, "readers"),
            "reader_positions":
                self._snapshot_table(snap, "reader_positions"),
            "reader_acks": self._snapshot_table(snap, "reader_acks"),
            "stats": view["stats"],
        }
        return manifest

    @staticmethod
    def _snapshot_table(snap, table):
        return {"columns": snap[table]["columns"],
                "rows": snap[table]["rows"]}

    # -- verification ------------------------------------------------------

    def verify_point(self, name, *, deep=True):
        """Offline verification of a published point.

        Checks manifest/format support, metadata-db block presence and
        hash, every segment block (presence + sha256), frame/CRC/offset
        validity over the exact captured byte ranges, and catalog
        correspondence.  Missing, corrupt and unsupported objects are
        reported separately with their partition/segment/byte range.
        """
        with self._repo_lock(exclusive=False, block=True):
            manifest = self._load_manifest(name)
            with self._lease("verify", name):
                self._fault.maybe("verify.entered")
                return self._verify_locked(manifest, deep=deep)

    def _verify_locked(self, manifest, deep=True):
        name = manifest["name"]
        problems = []
        blocks_checked = 0
        bytes_checked = 0
        segments_checked = 0

        src_fmt = manifest["source"].get("format")
        if src_fmt != LOG_FORMAT_VERSION:
            problems.append(_problem(
                "unsupported", name,
                f"source log format {src_fmt!r} is not supported "
                f"(need {LOG_FORMAT_VERSION!r})",
                object="source.format"))

        # metadata snapshot
        meta = manifest["metadata"]
        meta_path = self._block_path(meta["sha256"], kind=_META)
        if not os.path.exists(meta_path):
            problems.append(_problem(
                "missing", name,
                f"metadata snapshot block {meta['sha256']} is missing",
                object="metadata", byte_range=[0, meta["bytes"]]))
        else:
            got_sha_hex, got = self._hash_file(meta_path)
            bytes_checked += got
            if got != meta["bytes"] or got_sha_hex != meta["sha256"]:
                problems.append(_problem(
                    "corrupt", name,
                    "metadata snapshot checksum/size mismatch",
                    object="metadata",
                    byte_range=[0, meta["bytes"]]))

        for p, entry in manifest["partitions"].items():
            for s in entry["segments"]:
                segments_checked += 1
                blocks_checked, bytes_checked, covered = self._verify_segment(
                    name, int(p), s, deep, problems, blocks_checked,
                    bytes_checked)
                if covered != s["bytes"]:
                    problems.append(_problem(
                        "corrupt", name,
                        f"block list covers {covered} of {s['bytes']} "
                        f"confirmed bytes",
                        partition=int(p), segment=s["path"],
                        byte_range=[covered, s["bytes"]]))
        report = {"point": name, "ok": not problems, "problems": problems,
                  "segments_checked": segments_checked,
                  "blocks_checked": blocks_checked,
                  "bytes_checked": bytes_checked,
                  "metadata_sha256": meta["sha256"]}
        if problems:
            raise BackupVerificationError(
                f"recovery point {name!r} failed verification", problems)
        return report

    def _verify_segment(self, name, partition, seg, deep, problems,
                        blocks_checked, bytes_checked):
        validator = _FrameValidator(
            seg["base_offset"], seg["bytes"], name, partition, seg["path"]) \
            if deep else None
        covered = 0
        for b in seg["blocks"]:
            if b["offset"] != covered:
                problems.append(_problem(
                    "corrupt", name,
                    f"block list not contiguous: expected offset {covered}, "
                    f"found {b['offset']}", partition=partition,
                    segment=seg["path"],
                    byte_range=[b["offset"], b["offset"] + b["length"]]))
            path = self._block_path(b["sha256"])
            if not os.path.exists(path):
                problems.append(_problem(
                    "missing", name,
                    f"block {b['sha256']} is missing",
                    partition=partition, segment=seg["path"],
                    byte_range=[b["offset"], b["offset"] + b["length"]]))
                covered = b["offset"] + b["length"]
                continue
            got_sha_hex, got = self._hash_file(path)
            blocks_checked += 1
            bytes_checked += got
            if got != b["length"] or got_sha_hex != b["sha256"]:
                problems.append(_problem(
                    "corrupt", name,
                    "block checksum/size mismatch",
                    partition=partition, segment=seg["path"],
                    byte_range=[b["offset"], b["offset"] + b["length"]]))
            elif deep:
                # blocks are already bounded (CHUNK_BYTES); stream them into
                # the frame validator one block at a time.
                with open(path, "rb") as fh:
                    validator.feed(fh.read())
            covered = b["offset"] + b["length"]
        if deep:
            problems.extend(validator.finish())
            if seg["max_offset"] >= 0 and \
                    validator.last_offset != seg["max_offset"]:
                problems.append(_problem(
                    "corrupt", name,
                    f"catalog max_offset={seg['max_offset']} but frames end "
                    f"at {validator.last_offset}",
                    partition=partition, segment=seg["path"]))
        return blocks_checked, bytes_checked, covered

    def _block_path(self, sha, *, kind=_CHUNKS):
        return os.path.join(self.path, kind, sha[:2], sha)

    @staticmethod
    def _hash_file(path):
        h = hashlib.sha256()
        size = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK_BYTES)
                if not chunk:
                    break
                h.update(chunk)
                size += len(chunk)
        return h.hexdigest(), size

    # -- restore -----------------------------------------------------------

    def restore_point(self, name, target_path):
        """Restore a published point into a non-existing directory.

        Only the repository is used.  The target never appears as a
        finished log until every byte and the metadata database are staged,
        verified and recovered; failure or process exit leaves at most a
        removable ``<target>.restoring-*`` staging directory.
        """
        target_path = os.path.abspath(target_path)
        if os.path.exists(target_path):
            raise BackupTargetExists(
                f"restore target {target_path!r} already exists; restore "
                f"never overwrites")
        with self._repo_lock(exclusive=False, block=False):
            manifest = self._load_manifest(name)
            if manifest["source"].get("format") != LOG_FORMAT_VERSION:
                raise BackupUnsupportedVersion(
                    f"recovery point {name!r} source format "
                    f"{manifest['source'].get('format')!r} is unsupported")
            with self._lease("restore", name):
                return self._restore_locked(manifest, target_path)

    def _restore_locked(self, manifest, target_path):
        name = manifest["name"]
        parent = os.path.dirname(target_path) or "."
        os.makedirs(parent, exist_ok=True)
        lock_path = target_path + ".lock"
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BackupBusy(
                    f"a restore into {target_path!r} is already running")
            if os.path.exists(target_path):
                raise BackupTargetExists(
                    f"restore target {target_path!r} already exists")
            # We hold the exclusive target lock, so any earlier
            # <target>.restoring-* directory belongs to a crashed attempt
            # and can never be a live, competing restore.
            for stale in glob.glob(target_path + ".restoring-*"):
                shutil.rmtree(stale, ignore_errors=True)
            stage = ("%s.restoring-%s"
                     % (target_path, uuid.uuid4().hex[:12]))
            try:
                os.makedirs(stage)
                self._materialize_segments(manifest, stage)
                new_log_id = self._materialize_metadata(manifest, stage)
                # Standard open runs the ordinary catalog/disk recovery and
                # full byte validation; nothing about restore bypasses it.
                recovered = Log(stage, writable=True, create=False)
                report = list(recovered.recovery_report)
                recovered.close()
                self._fault.maybe("restore.before_rename")
                if os.path.exists(target_path):
                    raise BackupTargetExists(
                        f"restore target {target_path!r} appeared while "
                        f"staging")
                _fsync_dir(parent)
                os.rename(stage, target_path)
                _fsync_dir(parent)
                return {
                    "point": name, "target": target_path,
                    "source_log_id": manifest["source"]["log_id"],
                    "new_log_id": new_log_id,
                    "source_path_at_capture": manifest["source"].get("path"),
                    "partitions": {
                        p: {"retained_below": e["retained_below"],
                            "next_offset": e["next_offset"]}
                        for p, e in manifest["partitions"].items()},
                    "recovery_report": report}
            except BackupVerificationError:
                raise
            except CorruptionError as e:
                raise BackupVerificationError(
                    f"recovery point {name!r} produced a corrupt log: {e}",
                    [_problem("corrupt", name, str(e), object="restored_log")])
            finally:
                shutil.rmtree(stage, ignore_errors=True)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
            try:
                os.unlink(lock_path)
            except FileNotFoundError:
                pass

    def _materialize_segments(self, manifest, stage):
        for p, entry in manifest["partitions"].items():
            pdir = os.path.join(stage, storage.partition_dirname(int(p)))
            os.makedirs(pdir, exist_ok=True)
            for s in entry["segments"]:
                out_path = os.path.join(stage, s["path"])
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                with open(out_path, "wb") as out:
                    for b in s["blocks"]:
                        chunk_path = self._block_path(b["sha256"])
                        if not os.path.exists(chunk_path):
                            raise BackupVerificationError(
                                f"required block missing for point "
                                f"{manifest['name']!r}",
                                [_problem("missing", manifest["name"],
                                          f"block {b['sha256']} missing",
                                          partition=int(p),
                                          segment=s["path"],
                                          byte_range=[b["offset"],
                                                      b["offset"] + b["length"]])])
                        with open(chunk_path, "rb") as inp:
                            self._copy_range(inp, out, 0, b["length"])
                    out.flush()
                    os.fsync(out.fileno())
                _fsync_dir(pdir)

    def _materialize_metadata(self, manifest, stage):
        meta_sha = manifest["metadata"]["sha256"]
        src = self._block_path(meta_sha, kind=_META)
        if not os.path.exists(src):
            raise BackupVerificationError(
                f"metadata snapshot missing for point {manifest['name']!r}",
                [_problem("missing", manifest["name"],
                          f"metadata block {meta_sha} missing",
                          object="metadata")])
        db_path = os.path.join(stage, "meta.sqlite3")
        con = db.connect(db_path)
        try:
            # Re-attach the archived snapshot read-only/immutable and copy
            # it verbatim, then mint a NEW durable identity for the restored
            # branch.  The immutable URI makes SQLite create no -wal/-shm
            # side files next to the read-only repository object; the
            # staging DB temporarily uses rollback journaling so the
            # immutable attach does not make the connection read-only.
            # The full schema is created in main first and every target is
            # main-qualified, so an unqualified name can never resolve into
            # the read-only attached snapshot.
            con.execute("PRAGMA journal_mode=DELETE")
            db.init_schema(con)
            snap_uri = "file:%s?immutable=1" % urllib.parse.quote(src,
                                                                  safe="/")
            con.execute("ATTACH DATABASE ? AS snap", (snap_uri,))
            con.execute("BEGIN IMMEDIATE")
            for table in _SNAPSHOT_TABLES:
                con.execute("INSERT INTO main.%s SELECT * FROM snap.%s"
                            % (table, table))
            new_log_id = uuid.uuid4().hex
            con.execute("UPDATE main.meta SET value=? WHERE key='log_id'",
                        (new_log_id,))
            con.execute("COMMIT")
            con.execute("DETACH DATABASE snap")
            # Return the restored log to its normal WAL operating mode.
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            con.close()
        _fsync_dir(stage)
        return new_log_id

    # -- deletion / garbage collection ------------------------------------

    def delete_point(self, name):
        """Delete a published point and release the blocks only it used.

        Takes the exclusive repository lock and refuses (BackupBusy) while
        a verify/restore lease is active; blocks still referenced by
        another point are never touched.
        """
        _validate_name(name)
        with self._repo_lock(exclusive=True, block=False):
            manifest = self._load_manifest(name)
            leases = [l for l in self._leases()
                      if l.get("point") == name]
            if leases:
                raise BackupBusy(
                    f"recovery point {name!r} is in use: {leases}")
            referenced = self._referenced_blocks(exclude=name)
            released = []
            released_bytes = 0
            for p, entry in manifest["partitions"].items():
                for s in entry["segments"]:
                    for b in s["blocks"]:
                        if b["sha256"] in referenced:
                            continue
                        path = self._block_path(b["sha256"])
                        if os.path.exists(path):
                            os.unlink(path)
                            released.append({"sha256": b["sha256"],
                                             "bytes": b["length"]})
                            released_bytes += b["length"]
            meta_sha = manifest["metadata"]["sha256"]
            meta_path = self._block_path(meta_sha, kind=_META)
            removed_meta = False
            if meta_sha not in self._referenced_meta(exclude=name) and \
                    os.path.exists(meta_path):
                os.unlink(meta_path)
                removed_meta = True
            os.unlink(self._manifest_path(name))
            _fsync_dir(os.path.join(self.path, _POINTS))
            return {"point": name, "released_blocks": released,
                    "released_bytes": released_bytes,
                    "released_metadata": removed_meta}

    def gc(self):
        """Reclaim unreferenced blocks and leftovers of crashed publishes.

        Safe to run at any time: only objects that no published point
        references are removed.  While a publish is ACTIVELY running in
        another process, its not-yet-published blocks are indistinguishable
        from unreferenced ones, so block/meta sweeping is skipped (stale
        jobs of dead processes are still removed); a later gc once the
        publish finishes performs the full sweep.
        """
        with self._repo_lock(exclusive=True, block=False):
            stale_jobs = self._sweep_jobs()
            active = self._live_jobs()
            if active:
                return {"freed_blocks": [], "freed_bytes": 0,
                        "freed_metadata": [],
                        "removed_stale_jobs": stale_jobs,
                        "skipped_due_to_active_publish": True,
                        "active_jobs": active}
            published = self.list_points()
            live_blocks, live_meta = set(), set()
            for summary in published:
                m = self._load_manifest(summary["name"])
                live_blocks, live_meta = self._manifest_refs(m, live_blocks,
                                                             live_meta)
            freed_blocks, freed_bytes = self._sweep(_CHUNKS, live_blocks)
            freed_meta, _ = self._sweep(_META, live_meta)
            return {"freed_blocks": freed_blocks,
                    "freed_bytes": freed_bytes,
                    "freed_metadata": freed_meta,
                    "removed_stale_jobs": stale_jobs,
                    "skipped_due_to_active_publish": False}

    def _live_jobs(self):
        """Job directories whose publishing process still holds job.lock."""
        live = []
        root = os.path.join(self.path, _JOBS)
        for fn in os.listdir(root):
            lock = os.path.join(root, fn, "job.lock")
            if not os.path.exists(lock):
                continue
            fd = os.open(lock, os.O_RDWR)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    live.append(fn)
                else:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
        return live

    def _sweep(self, sub, live):
        freed, freed_bytes = [], 0
        root = os.path.join(self.path, sub)
        for shard in os.listdir(root):
            sd = os.path.join(root, shard)
            if not os.path.isdir(sd):
                continue
            for fn in os.listdir(sd):
                if fn in live:
                    continue
                full = os.path.join(sd, fn)
                try:
                    size = os.path.getsize(full)
                    os.unlink(full)
                except OSError:
                    continue
                freed.append(fn)
                freed_bytes += size
        return freed, freed_bytes

    def _sweep_jobs(self):
        """Remove only job directories whose publishing process is gone."""
        removed = []
        root = os.path.join(self.path, _JOBS)
        for fn in os.listdir(root):
            jp = os.path.join(root, fn)
            lock = os.path.join(jp, "job.lock")
            live = False
            if os.path.exists(lock):
                fd = os.open(lock, os.O_RDWR)
                try:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(fd, fcntl.LOCK_UN)
                    except BlockingIOError:
                        live = True
                finally:
                    os.close(fd)
            if live:
                continue
            shutil.rmtree(jp, ignore_errors=True)
            if not os.path.exists(jp):
                removed.append(fn)
        return removed

    def _referenced_blocks(self, exclude=None):
        live, _ = self._all_refs(exclude=exclude)
        return live

    def _referenced_meta(self, exclude=None):
        _, meta = self._all_refs(exclude=exclude)
        return meta

    def _all_refs(self, exclude=None):
        blocks, meta = set(), set()
        pdir = os.path.join(self.path, _POINTS)
        for fn in os.listdir(pdir):
            if not fn.endswith(".json"):
                continue
            name = fn[:-5]
            if name == exclude:
                continue
            with open(os.path.join(pdir, fn), "r", encoding="utf-8") as fh:
                m = json.load(fh)
            self._manifest_refs(m, blocks, meta)
        return blocks, meta

    @staticmethod
    def _manifest_refs(m, blocks, meta):
        meta.add(m["metadata"]["sha256"])
        for entry in m["partitions"].values():
            for s in entry["segments"]:
                for b in s["blocks"]:
                    blocks.add(b["sha256"])
        return blocks, meta


def open_repository(path, *, create=True, fault_hook=None):
    """Open (or initialize) a backup repository directory."""
    return BackupRepository(path, create=create, fault_hook=fault_hook)
