"""Core log implementation: segments + SQLite metadata + recovery.

Durability contract
-------------------
* ``append`` returns only after the record bytes are written to the segment
  file, the file is fsynced, and the metadata transaction (dedup identity,
  sequence state, high watermark, segment catalog) is committed in SQLite
  (synchronous=FULL).  An acknowledged record is never lost and never
  changes offset.
* ``commit``/``abort`` flip the transaction state in a single SQLite
  transaction, so a cross-partition transaction becomes visible (or
  invisible) atomically on every partition at once.  All data records were
  already fsynced by their appends before commit is even possible.
* Crash between segment write/fsync and metadata commit leaves an
  unacknowledged tail on disk; recovery truncates it.  The retried append
  reuses the same offset, so offsets stay contiguous and stable.

Concurrency
-----------
* One writer domain per log directory: a writable ``Log`` holds an
  exclusive flock on ``writer.lock`` for its lifetime (released on close or
  process death).  Other processes may open the log read-only and fetch
  concurrently.
* ``fetch``/``inspect`` hold a shared lock on ``retain.lock`` while reading
  segment files; ``retain`` takes the exclusive lock before unlinking, so
  record bodies can never be deleted under an in-flight read.
"""

import fcntl
import os
import threading
import time
import zlib
from contextlib import contextmanager

from . import db, storage
from .errors import (ContentConflict, CorruptionError, ProducerFenced,
                     ReaderConflict, SequenceGap, SequenceTooOld, TxLogError,
                     TxnStateError, UnknownReader, UnknownTransaction)
from .faults import FaultInjector

DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024
DEFAULT_DEDUP_WINDOW = 4096
FORMAT_VERSION = "1"

ISOLATION_READ_COMMITTED = "read_committed"
ISOLATION_READ_UNCOMMITTED = "read_uncommitted"


# ---------------------------------------------------------------------------
# locks

class RWLock:
    """Writer-preferring readers/writer lock: once a writer queues, new
    readers wait, so a steady stream of fetches cannot starve retain."""

    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False
        self._waiting_writers = 0

    @contextmanager
    def read(self):
        with self._cond:
            while self._writer or self._waiting_writers:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    @contextmanager
    def write(self):
        with self._cond:
            self._waiting_writers += 1
            try:
                while self._writer or self._readers:
                    self._cond.wait()
            finally:
                self._waiting_writers -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._cond:
                self._writer = False
                self._cond.notify_all()


class _PathLocks:
    """Per-log-directory, per-process lock state shared by Log instances."""

    def __init__(self):
        self.write_mu = threading.RLock()   # serializes write ops in-process
        self.rw = RWLock()                  # readers vs retain, in-process
        self.writer_fd = None               # held while any writable Log lives
        self.refcount = 0


_REGISTRY = {}
_REGISTRY_MU = threading.Lock()


def _path_locks(path):
    with _REGISTRY_MU:
        pl = _REGISTRY.get(path)
        if pl is None:
            pl = _PathLocks()
            _REGISTRY[path] = pl
        return pl


@contextmanager
def _flock(path, flag):
    """ flock on a fresh fd (flock state is per open-file-description)."""
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, flag)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# Log

class Log:
    """A transactional partitioned log rooted at a directory."""

    def __init__(self, path, *, writable=True, create=True,
                 segment_bytes=None, dedup_window=None, fault_hook=None,
                 lock_timeout=30.0):
        self.path = os.path.abspath(path)
        self.writable = writable
        self._fault = FaultInjector(fault_hook)
        self.recovery_report = []
        self._fds = {}            # partition -> (fd, segment base) writer fds
        self._seg_cache = {}      # (path, size, mtime_ns) -> [Record]
        self._cache_mu = threading.Lock()
        self._closed = False

        if not os.path.isdir(self.path):
            if not writable or not create:
                raise TxLogError(f"log directory {self.path!r} does not exist")
            os.makedirs(self.path)
        self._pl = _path_locks(self.path)
        self._retain_lock_path = os.path.join(self.path, "retain.lock")
        self._dbpath = os.path.join(self.path, "meta.sqlite3")

        if writable:
            self._acquire_writer(lock_timeout)
        try:
            self._init_db(create, segment_bytes, dedup_window)
            if writable:
                self._recover()
        except Exception:
            if writable:
                self._release_writer()
            raise

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def open(cls, path, **kw):
        return cls(path, **kw)

    def close(self):
        if self._closed:
            return
        self._closed = True
        for fd, _base in self._fds.values():
            try:
                os.close(fd)
            except OSError:
                pass
        self._fds.clear()
        if self.writable:
            self._release_writer()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _check_open(self):
        if self._closed:
            raise TxLogError("log is closed")

    def _check_writable(self):
        self._check_open()
        if not self.writable:
            raise TxLogError("log was opened read-only")

    def _acquire_writer(self, timeout):
        pl = self._pl
        with _REGISTRY_MU:
            if pl.refcount == 0:
                fd = os.open(os.path.join(self.path, "writer.lock"),
                             os.O_CREAT | os.O_RDWR, 0o644)
                deadline = time.monotonic() + timeout
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() > deadline:
                            os.close(fd)
                            raise TxLogError(
                                "writer lock is held by another process")
                        time.sleep(0.05)
                pl.writer_fd = fd
            pl.refcount += 1

    def _release_writer(self):
        pl = self._pl
        with _REGISTRY_MU:
            pl.refcount -= 1
            if pl.refcount == 0 and pl.writer_fd is not None:
                try:
                    fcntl.flock(pl.writer_fd, fcntl.LOCK_UN)
                finally:
                    os.close(pl.writer_fd)
                pl.writer_fd = None

    @contextmanager
    def _write_mu(self):
        with self._pl.write_mu:
            yield

    @contextmanager
    def _retain_shared(self):
        with self._pl.rw.read():
            with _flock(self._retain_lock_path, fcntl.LOCK_SH):
                yield

    @contextmanager
    def _retain_exclusive(self):
        with self._pl.rw.write():
            with _flock(self._retain_lock_path, fcntl.LOCK_EX):
                yield

    # -- db helpers --------------------------------------------------------

    def _connect(self):
        return db.connect(self._dbpath)

    def _init_db(self, create, segment_bytes, dedup_window):
        fresh = not os.path.exists(self._dbpath)
        if fresh and (not create or not self.writable):
            raise TxLogError(f"{self.path!r} is not an initialized txlog")
        con = self._connect()
        try:
            db.init_schema(con)
            row = con.execute("SELECT value FROM meta WHERE key='format'").fetchone()
            if row is None:
                self.segment_bytes = segment_bytes or DEFAULT_SEGMENT_BYTES
                self.dedup_window = (dedup_window if dedup_window is not None
                                     else DEFAULT_DEDUP_WINDOW)
                con.execute("INSERT INTO meta VALUES ('format', ?)",
                            (FORMAT_VERSION,))
                con.execute("INSERT INTO meta VALUES ('segment_bytes', ?)",
                            (str(self.segment_bytes),))
                con.execute("INSERT INTO meta VALUES ('dedup_window', ?)",
                            (str(self.dedup_window),))
            else:
                if row["value"] != FORMAT_VERSION:
                    raise TxLogError(f"unsupported format version {row['value']}")
                self.segment_bytes = int(con.execute(
                    "SELECT value FROM meta WHERE key='segment_bytes'").fetchone()["value"])
                self.dedup_window = int(con.execute(
                    "SELECT value FROM meta WHERE key='dedup_window'").fetchone()["value"])
        finally:
            con.close()

    # -- recovery ----------------------------------------------------------

    def _recover(self):
        con = self._connect()
        try:
            parts = {r["partition"]: dict(r) for r in
                     con.execute("SELECT * FROM partitions")}
            live, deleted = {}, {}
            for r in con.execute(
                    "SELECT * FROM segments WHERE state != 'deleted' "
                    "ORDER BY partition, base_offset"):
                live.setdefault(r["partition"], []).append(dict(r))
            for r in con.execute(
                    "SELECT * FROM segments WHERE state = 'deleted'"):
                deleted.setdefault(r["partition"], set()).add(r["path"])

            # 1. reconcile files vs catalog
            on_disk = {}
            for name in os.listdir(self.path):
                if not name.startswith("p"):
                    continue
                pdir = os.path.join(self.path, name)
                if not os.path.isdir(pdir):
                    continue
                try:
                    p = int(name[1:])
                except ValueError:
                    continue
                on_disk[p] = {f for f in os.listdir(pdir) if f.endswith(".seg")}

            for p in set(parts) | set(live) | set(deleted) | set(on_disk):
                known = {s["path"] for s in live.get(p, [])}
                gone = deleted.get(p, set())
                for fn in on_disk.get(p, ()):
                    rel = os.path.join(storage.partition_dirname(p), fn)
                    full = os.path.join(self.path, rel)
                    if rel in gone:
                        os.unlink(full)
                        self.recovery_report.append(
                            f"finished deletion of segment {rel}")
                    elif rel not in known:
                        # leftover of a roll that crashed before its catalog
                        # transaction committed; it cannot contain
                        # acknowledged data.
                        os.unlink(full)
                        self.recovery_report.append(
                            f"removed stray segment {rel} (not in catalog)")
                for s in live.get(p, []):
                    if not os.path.exists(os.path.join(self.path, s["path"])):
                        raise CorruptionError(
                            f"catalog references missing segment {s['path']} "
                            f"(partition {p})")

            # 2. validate bytes, truncate unacknowledged tail
            for p, prow in sorted(parts.items()):
                self._recover_partition(con, p, prow, live.get(p, []))
        finally:
            con.close()

    def _recover_partition(self, con, p, prow, seg_rows):
        if not seg_rows:
            raise CorruptionError(
                f"partition {p}: catalog has no live segments")
        next_offset = prow["next_offset"]
        expect = seg_rows[0]["base_offset"]
        trunc = None  # (seg_row, byte position)
        for i, s in enumerate(seg_rows):
            if s["base_offset"] != expect:
                raise CorruptionError(
                    f"partition {p}: segment {s['path']} starts at "
                    f"{s['base_offset']}, expected {expect}")
            full = os.path.join(self.path, s["path"])
            with open(full, "rb") as fh:
                data = fh.read()
            try:
                recs, good = storage.parse_segment(data)
            except storage.SegmentCorruption as e:
                raise CorruptionError(
                    f"partition {p} segment {s['path']}: {e}")
            if good < len(data) and i != len(seg_rows) - 1:
                raise CorruptionError(
                    f"partition {p}: torn/corrupt tail in sealed segment "
                    f"{s['path']} at byte {good}")
            for rec in recs:
                if rec.offset != expect:
                    raise CorruptionError(
                        f"partition {p}: offset gap in {s['path']}: expected "
                        f"{expect}, found {rec.offset}")
                expect = rec.offset + 1
            if i == len(seg_rows) - 1:
                trunc = (s, recs, good, len(data))
            else:
                # sealed segment: catalog must agree with the bytes
                if not recs:
                    raise CorruptionError(
                        f"partition {p}: sealed segment {s['path']} is empty")
                if s["max_offset"] != recs[-1].offset or s["bytes"] != good:
                    raise CorruptionError(
                        f"partition {p}: catalog/segment mismatch for "
                        f"{s['path']} (catalog max_offset={s['max_offset']} "
                        f"bytes={s['bytes']}; disk max_offset="
                        f"{recs[-1].offset} bytes={good})")
        disk_next = expect
        if disk_next < next_offset:
            raise CorruptionError(
                f"partition {p}: acknowledged records missing — catalog "
                f"next_offset={next_offset} but disk ends at {disk_next}")
        s, recs, good, fsize = trunc
        if disk_next > next_offset or good < fsize:
            # unacknowledged tail (crashed between fsync and meta commit, or
            # torn mid-append): truncate back to the acknowledged frontier.
            cut = 0
            for rec in recs:
                if rec.offset < next_offset:
                    cut = rec.end
                else:
                    break
            full = os.path.join(self.path, s["path"])
            os.truncate(full, cut)
            self.recovery_report.append(
                f"partition {p}: truncated unacknowledged tail of "
                f"{s['path']} to {cut} bytes (next_offset={next_offset})")
            self._cache_drop(s["path"])
            max_off = next_offset - 1 if next_offset > s["base_offset"] else -1
            con.execute(
                "UPDATE segments SET max_offset=?, bytes=? "
                "WHERE partition=? AND base_offset=?",
                (max_off, cut, p, s["base_offset"]))
        else:
            # heal informational drift of the active segment, if any
            max_off = recs[-1].offset if recs else -1
            if s["max_offset"] != max_off or s["bytes"] != good:
                con.execute(
                    "UPDATE segments SET max_offset=?, bytes=? "
                    "WHERE partition=? AND base_offset=?",
                    (max_off, good, p, s["base_offset"]))
                self.recovery_report.append(
                    f"partition {p}: healed catalog size of {s['path']}")

    # -- segment io ----------------------------------------------------------

    def _read_segment(self, rel_path, sealed):
        full = os.path.join(self.path, rel_path)
        try:
            fd = os.open(full, os.O_RDONLY)
        except FileNotFoundError:
            raise CorruptionError(f"segment file {rel_path} is missing")
        try:
            st = os.fstat(fd)
            key = (rel_path, st.st_size, st.st_mtime_ns)
            with self._cache_mu:
                hit = self._seg_cache.get(key)
            if hit is not None:
                return hit
            chunks = []
            got = 0
            while got < st.st_size:
                chunk = os.pread(fd, st.st_size - got, got)
                if not chunk:
                    break
                chunks.append(chunk)
                got += len(chunk)
            data = b"".join(chunks)
        finally:
            os.close(fd)
        try:
            recs, good = storage.parse_segment(data)
        except storage.SegmentCorruption as e:
            raise CorruptionError(f"segment {rel_path}: {e}")
        if sealed and good != len(data):
            raise CorruptionError(
                f"sealed segment {rel_path} has a torn/corrupt tail at "
                f"byte {good}")
        with self._cache_mu:
            if len(self._seg_cache) > 128:
                self._seg_cache.clear()
            self._seg_cache[key] = recs
        return recs

    def _cache_drop(self, rel_path):
        with self._cache_mu:
            for k in [k for k in self._seg_cache if k[0] == rel_path]:
                del self._seg_cache[k]

    def _seg_fd(self, partition, base, rel_path):
        cur = self._fds.get(partition)
        if cur is not None and cur[1] == base:
            return cur[0]
        if cur is not None:
            os.close(cur[0])
        fd = os.open(os.path.join(self.path, rel_path),
                     os.O_WRONLY | os.O_APPEND)
        self._fds[partition] = (fd, base)
        return fd

    # -- write path ----------------------------------------------------------

    def _ensure_partition(self, con, partition):
        row = con.execute("SELECT * FROM partitions WHERE partition=?",
                          (partition,)).fetchone()
        if row is not None:
            return row
        pdir_name = storage.partition_dirname(partition)
        pdir = os.path.join(self.path, pdir_name)
        os.makedirs(pdir, exist_ok=True)
        rel = os.path.join(pdir_name, storage.segment_name(0))
        fd = os.open(os.path.join(self.path, rel),
                     os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        storage.fsync_dir(pdir)
        with self._txn(con):
            con.execute(
                "INSERT INTO partitions VALUES (?, 0, 0, 0)", (partition,))
            con.execute(
                "INSERT INTO segments VALUES (?, ?, ?, -1, 0, 'active')",
                (partition, 0, rel))
        return con.execute("SELECT * FROM partitions WHERE partition=?",
                           (partition,)).fetchone()

    @contextmanager
    def _txn(self, con):
        con.execute("BEGIN IMMEDIATE")
        try:
            yield
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise

    def _roll(self, con, partition, new_base):
        """Seal the active segment and open a new one (inside caller's txn,
        but the new file is created first so a crash leaves only a stray
        empty file that recovery removes)."""
        pdir_name = storage.partition_dirname(partition)
        rel = os.path.join(pdir_name, storage.segment_name(new_base))
        fd = os.open(os.path.join(self.path, rel),
                     os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        storage.fsync_dir(os.path.join(self.path, pdir_name))
        self._fault.maybe("roll.after_file")
        con.execute(
            "UPDATE segments SET state='sealed', max_offset=? "
            "WHERE partition=? AND state='active'", (new_base - 1, partition))
        con.execute(
            "INSERT INTO segments VALUES (?, ?, ?, -1, 0, 'active')",
            (partition, new_base, rel))
        con.execute("UPDATE partitions SET active_segment=? WHERE partition=?",
                    (new_base, partition))
        self._fault.maybe("roll.after_catalog")

    def _append_physical(self, con, partition, rec_bytes, offset):
        """Write record bytes, fsync, and stage metadata updates in ``con``'s
        open transaction.  Caller commits."""
        seg = con.execute(
            "SELECT * FROM segments WHERE partition=? AND state='active'",
            (partition,)).fetchone()
        if seg["bytes"] > 0 and seg["bytes"] + len(rec_bytes) > self.segment_bytes:
            self._roll(con, partition, offset)
            seg = con.execute(
                "SELECT * FROM segments WHERE partition=? AND state='active'",
                (partition,)).fetchone()
        fd = self._seg_fd(partition, seg["base_offset"], seg["path"])
        storage.write_all(fd, rec_bytes)
        self._fault.maybe("append.after_write")
        os.fsync(fd)
        self._fault.maybe("append.after_fsync")
        con.execute("UPDATE partitions SET next_offset=? WHERE partition=?",
                    (offset + 1, partition))
        con.execute(
            "UPDATE segments SET max_offset=?, bytes=? "
            "WHERE partition=? AND base_offset=?",
            (offset, seg["bytes"] + len(rec_bytes), partition,
             seg["base_offset"]))

    def append(self, partition, data):
        """Non-transactional append.

        Semantics: the record is visible to read_committed as soon as it is
        durable (subject to the partition's stable frontier).  There is no
        retry identity: if the caller retries after a lost receipt it may
        create a duplicate.  Returns the assigned offset.
        """
        self._check_writable()
        payload = _as_bytes(data)
        partition = _as_partition(partition)
        with self._write_mu():
            con = self._connect()
            try:
                self._ensure_partition(con, partition)
                with self._txn(con):
                    prow = con.execute(
                        "SELECT * FROM partitions WHERE partition=?",
                        (partition,)).fetchone()
                    offset = prow["next_offset"]
                    rec = storage.encode_record(offset, 0, -1, -1, "", payload)
                    self._append_physical(con, partition, rec, offset)
                self._fault.maybe("append.after_meta")
                return offset
            finally:
                con.close()

    # -- producers / transactions --------------------------------------------

    def _check_epoch(self, con, producer_id, epoch):
        row = con.execute("SELECT epoch FROM producers WHERE producer_id=?",
                          (producer_id,)).fetchone()
        cur = row["epoch"] if row else None
        if cur is None or epoch != cur:
            raise ProducerFenced(
                f"producer {producer_id!r} fenced: current epoch is "
                f"{cur}, requested {epoch}")

    def _open_producer(self, producer_id):
        """Bump the producer's epoch and durably fence the old one.

        The epoch bump and the abort of every leftover open transaction of
        this producer happen in ONE SQLite transaction: that is the explicit
        persistent flow that undoes pending transactions of a dead epoch.
        """
        self._check_writable()
        with self._write_mu():
            con = self._connect()
            try:
                with self._txn(con):
                    row = con.execute(
                        "SELECT epoch FROM producers WHERE producer_id=?",
                        (producer_id,)).fetchone()
                    new_epoch = (row["epoch"] + 1) if row else 1
                    if row:
                        con.execute(
                            "UPDATE producers SET epoch=? WHERE producer_id=?",
                            (new_epoch, producer_id))
                    else:
                        con.execute("INSERT INTO producers VALUES (?, ?)",
                                    (producer_id, new_epoch))
                    cur = con.execute(
                        "UPDATE transactions SET state='aborted', decided_at=? "
                        "WHERE producer_id=? AND state='open'",
                        (time.time(), producer_id))
                    aborted = cur.rowcount
                    # retry identity of dead epochs is useless (any retry is
                    # fenced); drop it so it cannot pin retention forever.
                    con.execute(
                        "DELETE FROM dedup WHERE producer_id=? AND epoch<?",
                        (producer_id, new_epoch))
                    con.execute(
                        "DELETE FROM seq_state WHERE producer_id=? AND epoch<?",
                        (producer_id, new_epoch))
                self._fault.maybe("open_producer.after_fence")
                return new_epoch, aborted
            finally:
                con.close()

    def producer(self, producer_id):
        from .producer import Producer
        epoch, _ = self._open_producer(producer_id)
        return Producer(self, producer_id, epoch)

    # -- named readers --------------------------------------------------------

    def create_reader(self, reader_id, start=None):
        """Create (or explicitly reuse) a durable named reader.

        ``start=None``  — the reader starts at each partition's current
        retention boundary on first poll (a "tail" reader).
        ``start=int``   — the reader starts at that still-readable offset.

        Re-creating an existing reader_id is NEVER a silent reset:
        * identical configuration -> the existing reader is returned with
          ``reused=True``, all progress untouched (idempotent);
        * different configuration -> ReaderConflict, progress untouched.
        """
        self._check_writable()
        if not isinstance(reader_id, str) or not reader_id:
            raise ValueError("reader_id must be a non-empty string")
        if start is None:
            mode, start_offset = "boundary", None
        else:
            start_offset = _as_offset(start)
            mode = "offset"
        with self._write_mu():
            con = self._connect()
            try:
                with self._txn(con):
                    row = con.execute(
                        "SELECT * FROM readers WHERE reader_id=?",
                        (reader_id,)).fetchone()
                    reused = row is not None
                    if row is None:
                        con.execute(
                            "INSERT INTO readers VALUES (?,?,?,?)",
                            (reader_id, mode, start_offset, time.time()))
                    elif (row["start_mode"] != mode
                          or row["start_offset"] != start_offset):
                        raise ReaderConflict(
                            f"reader {reader_id!r} already exists with "
                            f"start_mode={row['start_mode']!r} "
                            f"start_offset={row['start_offset']}; requested "
                            f"start_mode={mode!r} start_offset={start_offset}. "
                            f"Existing progress was not modified")
                from .reader import Reader
                return Reader(self, reader_id, reused=reused)
            finally:
                con.close()

    def open_reader(self, reader_id):
        """Open a handle onto an existing durable reader."""
        self._check_writable()
        con = self._connect()
        try:
            row = con.execute(
                "SELECT 1 FROM readers WHERE reader_id=?",
                (reader_id,)).fetchone()
            if row is None:
                raise UnknownReader(f"unknown reader {reader_id!r}")
        finally:
            con.close()
        from .reader import Reader
        return Reader(self, reader_id, reused=True)

    def list_readers(self):
        """All durable readers with their per-partition progress."""
        self._check_open()
        con = self._connect()
        try:
            out = {}
            for r in con.execute(
                    "SELECT * FROM readers ORDER BY reader_id"):
                out[r["reader_id"]] = {
                    "start_mode": r["start_mode"],
                    "start_offset": r["start_offset"],
                    "partitions": {}}
            for r in con.execute(
                    "SELECT * FROM reader_positions ORDER BY reader_id, "
                    "partition"):
                gaps = [x["offset"] for x in con.execute(
                    "SELECT offset FROM reader_acks WHERE reader_id=? AND "
                    "partition=? AND offset>=? ORDER BY offset",
                    (r["reader_id"], r["partition"], r["safe_offset"]))]
                out[r["reader_id"]]["partitions"][r["partition"]] = {
                    "read_offset": r["read_offset"],
                    "safe_offset": r["safe_offset"],
                    "blocking_gaps": gaps}
            return out
        finally:
            con.close()

    def delete_reader(self, reader_id, handle=None):
        """Explicitly delete a reader and ALL of its progress.

        Returns the reader's previous per-partition safe cursors together
        with the retention frontier now in force: the caller can see exactly
        which reader-imposed holds were released and which cleanup becomes
        possible on the next ``retain``.  Nothing is deleted immediately —
        the effect is recomputed by the next retain.
        """
        self._check_writable()
        with self._write_mu():
            con = self._connect()
            try:
                row = con.execute(
                    "SELECT * FROM readers WHERE reader_id=?",
                    (reader_id,)).fetchone()
                if row is None:
                    raise UnknownReader(f"unknown reader {reader_id!r}")
                positions = {r["partition"]:
                             {"read_offset": r["read_offset"],
                              "safe_offset": r["safe_offset"]}
                             for r in con.execute(
                                 "SELECT * FROM reader_positions "
                                 "WHERE reader_id=?", (reader_id,))}
                retained = {r["partition"]: r["retained_below"] for r in
                            con.execute("SELECT partition, retained_below "
                                        "FROM partitions")}
                with self._txn(con):
                    con.execute("DELETE FROM reader_acks WHERE reader_id=?",
                                (reader_id,))
                    con.execute(
                        "DELETE FROM reader_positions WHERE reader_id=?",
                        (reader_id,))
                    con.execute("DELETE FROM readers WHERE reader_id=?",
                                (reader_id,))
                if handle is not None and getattr(handle, "reader_id",
                                                   None) == reader_id:
                    handle._closed = True
                    handle._poll_pos.clear()
                return {"reader_id": reader_id,
                        "start_mode": row["start_mode"],
                        "start_offset": row["start_offset"],
                        "former_positions": positions,
                        "retained_below": retained,
                        "retention_effect": (
                            "reader hold released; run retain() on each "
                            "partition to recompute the cleanable boundary")}
            finally:
                con.close()

    @staticmethod
    def _reader_floor(con, partition):
        """Smallest live reader safe cursor on a partition; hw when no
        reader tracks it.  Retention may never cross this value."""
        row = con.execute(
            "SELECT MIN(safe_offset) AS f FROM reader_positions "
            "WHERE partition=?", (partition,)).fetchone()
        return row["f"] if row and row["f"] is not None else None

    def _open_txn(self, con, producer_id):
        return con.execute(
            "SELECT * FROM transactions WHERE producer_id=? AND state='open'",
            (producer_id,)).fetchone()

    def _begin(self, producer_id, epoch):
        self._check_writable()
        with self._write_mu():
            con = self._connect()
            try:
                self._check_epoch(con, producer_id, epoch)
                row = self._open_txn(con, producer_id)
                if row is not None:
                    # one transaction at a time: a repeated begin() confirms
                    # the still-open transaction (retry-safe).
                    return row["txn_id"]
                with self._txn(con):
                    cur = con.execute(
                        "INSERT INTO transactions "
                        "(producer_id, epoch, state, created_at) "
                        "VALUES (?, ?, 'open', ?)",
                        (producer_id, epoch, time.time()))
                    txn_id = cur.lastrowid
                self._fault.maybe("begin.after_meta")
                return txn_id
            finally:
                con.close()

    def _append_txn(self, producer_id, epoch, partition, data, seq):
        """Transactional append; returns (offset, is_duplicate_retry)."""
        self._check_writable()
        payload = _as_bytes(data)
        partition = _as_partition(partition)
        seq = _as_seq(seq)
        with self._write_mu():
            con = self._connect()
            try:
                self._check_epoch(con, producer_id, epoch)
                txn = self._open_txn(con, producer_id)
                if txn is None:
                    raise TxnStateError(
                        f"producer {producer_id!r} has no open transaction; "
                        f"call begin() first")
                if txn["epoch"] != epoch:  # defensive; fencing prevents this
                    raise ProducerFenced(
                        f"open transaction belongs to epoch {txn['epoch']}")
                crc = zlib.crc32(payload) & 0xFFFFFFFF
                ss = con.execute(
                    "SELECT * FROM seq_state WHERE producer_id=? AND epoch=? "
                    "AND partition=?", (producer_id, epoch, partition)).fetchone()
                if ss is None:
                    if seq != 0:
                        raise SequenceGap(
                            f"producer {producer_id!r} epoch {epoch} "
                            f"partition {partition}: expected sequence 0, "
                            f"got {seq}")
                    with self._txn(con):
                        con.execute(
                            "INSERT INTO seq_state VALUES (?, ?, ?, 0, 0)",
                            (producer_id, epoch, partition))
                    ss = con.execute(
                        "SELECT * FROM seq_state WHERE producer_id=? AND "
                        "epoch=? AND partition=?",
                        (producer_id, epoch, partition)).fetchone()
                if seq < ss["min_retained_seq"]:
                    raise SequenceTooOld(
                        f"sequence {seq} is below the dedup retention window "
                        f"lower bound {ss['min_retained_seq']} for producer "
                        f"{producer_id!r} epoch {epoch} partition {partition}; "
                        f"retries outside the window are rejected, not "
                        f"re-appended")
                dup = con.execute(
                    "SELECT * FROM dedup WHERE producer_id=? AND epoch=? AND "
                    "partition=? AND seq=?",
                    (producer_id, epoch, partition, seq)).fetchone()
                if dup is not None:
                    if dup["crc"] != crc:
                        raise ContentConflict(
                            f"(producer={producer_id!r}, epoch={epoch}, "
                            f"partition={partition}, seq={seq}) was already "
                            f"appended with different content at offset "
                            f"{dup['offset']}")
                    # idempotent retry of an already-durable append
                    return dup["offset"], True
                if seq != ss["next_seq"]:
                    raise SequenceGap(
                        f"producer {producer_id!r} epoch {epoch} partition "
                        f"{partition}: expected sequence {ss['next_seq']}, "
                        f"got {seq}")
                self._ensure_partition(con, partition)
                with self._txn(con):
                    prow = con.execute(
                        "SELECT * FROM partitions WHERE partition=?",
                        (partition,)).fetchone()
                    offset = prow["next_offset"]
                    rec = storage.encode_record(
                        offset, txn["txn_id"], epoch, seq, producer_id, payload)
                    self._append_physical(con, partition, rec, offset)
                    con.execute(
                        "INSERT INTO dedup VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (producer_id, epoch, partition, seq, offset, crc,
                         txn["txn_id"]))
                    new_next = seq + 1
                    new_min = max(0, new_next - self.dedup_window)
                    con.execute(
                        "UPDATE seq_state SET next_seq=?, min_retained_seq=? "
                        "WHERE producer_id=? AND epoch=? AND partition=?",
                        (new_next, new_min, producer_id, epoch, partition))
                    if new_min > ss["min_retained_seq"]:
                        con.execute(
                            "DELETE FROM dedup WHERE producer_id=? AND "
                            "epoch=? AND partition=? AND seq<?",
                            (producer_id, epoch, partition, new_min))
                    con.execute(
                        "INSERT INTO txn_parts (txn_id, partition, "
                        "first_offset, last_offset) VALUES (?, ?, ?, ?) "
                        "ON CONFLICT (txn_id, partition) DO UPDATE SET "
                        "last_offset=excluded.last_offset",
                        (txn["txn_id"], partition, offset, offset))
                self._fault.maybe("append.after_meta")
                return offset, False
            finally:
                con.close()

    def _decide(self, producer_id, epoch, txn_id, state):
        """commit/abort core: one durable SQLite state flip."""
        self._check_writable()
        verb = "commit" if state == "committed" else "abort"
        with self._write_mu():
            con = self._connect()
            try:
                self._check_epoch(con, producer_id, epoch)
                txn = self._open_txn(con, producer_id)
                if txn is None:
                    if txn_id is not None:
                        t = con.execute(
                            "SELECT * FROM transactions WHERE txn_id=? AND "
                            "producer_id=?", (txn_id, producer_id)).fetchone()
                        if t is None:
                            raise UnknownTransaction(
                                f"transaction {txn_id} not found for "
                                f"producer {producer_id!r}")
                        if t["state"] == state:
                            return txn_id  # idempotent retry-confirm
                        raise TxnStateError(
                            f"transaction {txn_id} is already {t['state']}")
                    raise TxnStateError(
                        f"producer {producer_id!r} has no open transaction")
                if txn_id is not None and txn["txn_id"] != txn_id:
                    raise TxnStateError(
                        f"transaction {txn_id} is not the open one "
                        f"({txn['txn_id']})")
                self._fault.maybe(f"{verb}.before_decision")
                with self._txn(con):
                    con.execute(
                        "UPDATE transactions SET state=?, decided_at=? "
                        "WHERE txn_id=?", (state, time.time(), txn["txn_id"]))
                self._fault.maybe(f"{verb}.after_decision")
                return txn["txn_id"]
            finally:
                con.close()

    def _commit(self, producer_id, epoch, txn_id=None):
        return self._decide(producer_id, epoch, txn_id, "committed")

    def _abort(self, producer_id, epoch, txn_id=None):
        return self._decide(producer_id, epoch, txn_id, "aborted")

    def txn_state(self, txn_id):
        """Durable state of a transaction: 'open' | 'committed' | 'aborted'
        | None (unknown or garbage-collected).  Retry-confirm entry point
        for calls whose receipt was lost."""
        self._check_open()
        con = self._connect()
        try:
            t = con.execute("SELECT * FROM transactions WHERE txn_id=?",
                            (txn_id,)).fetchone()
            if t is None:
                return None
            parts = {r["partition"]: [r["first_offset"], r["last_offset"]]
                     for r in con.execute(
                         "SELECT * FROM txn_parts WHERE txn_id=?", (txn_id,))}
            return {"txn_id": t["txn_id"], "producer_id": t["producer_id"],
                    "epoch": t["epoch"], "state": t["state"],
                    "partitions": parts}
        finally:
            con.close()

    # -- fetch ---------------------------------------------------------------

    def fetch(self, partition, offset=0, max_records=100,
              isolation=ISOLATION_READ_COMMITTED, max_scan=None):
        """Fetch a bounded page of records from ``partition`` at ``offset``.

        Returns a dict:
          records:           delivered records (business data).  For
                             read_uncommitted each carries txn_status
                             ('open'|'committed'|'aborted'|None).
          next_offset:       where the next fetch should start (advances
                             past skipped abort holes).
          reason:            'page_full' | 'log_end' | 'stable_frontier' |
                             'scan_limit' | 'retained_boundary'
          high_watermark:    next offset to be assigned (durable end)
          last_stable_offset: read_committed may not pass this offset
          retained_below:    offsets below this have been retained away

        One fetch evaluates the whole page against ONE snapshot of the
        transaction states; a commit landing mid-fetch cannot mix two
        states into the page.  Page length counts delivered business
        records; the physical scan is bounded by max_scan so long abort
        holes cannot accumulate work without bound.
        """
        self._check_open()
        partition = _as_partition(partition)
        offset = _as_offset(offset)
        if isolation not in (ISOLATION_READ_COMMITTED,
                             ISOLATION_READ_UNCOMMITTED):
            raise ValueError(f"bad isolation {isolation!r}")
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        if max_scan is None:
            max_scan = max(64, max_records * 8)
        if max_scan < 1:
            raise ValueError("max_scan must be >= 1")
        with self._retain_shared():
            con = self._connect()
            try:
                con.execute("BEGIN")
                prow = con.execute(
                    "SELECT * FROM partitions WHERE partition=?",
                    (partition,)).fetchone()
                hw = prow["next_offset"] if prow else 0
                retained = prow["retained_below"] if prow else 0
                lso = self._lso(con, partition, hw)
                segs = [dict(r) for r in con.execute(
                    "SELECT * FROM segments WHERE partition=? AND "
                    "state != 'deleted' ORDER BY base_offset", (partition,))]
                txn_states = {}
                for r in con.execute(
                        "SELECT DISTINCT t.txn_id, t.state FROM transactions t "
                        "JOIN txn_parts tp ON tp.txn_id = t.txn_id "
                        "WHERE tp.partition=? AND tp.last_offset>=? AND "
                        "tp.first_offset<?",
                        (partition, retained, max(hw, 1))):
                    txn_states[r["txn_id"]] = r["state"]
                self._fault.maybe("fetch.after_snapshot")
                page = self._scan(partition, offset, max_records, isolation,
                                  max_scan, hw, retained, lso, segs,
                                  txn_states)
                con.execute("ROLLBACK")
                return page
            finally:
                con.close()

    @staticmethod
    def _lso(con, partition, hw):
        row = con.execute(
            "SELECT MIN(tp.first_offset) AS f FROM txn_parts tp "
            "JOIN transactions t ON t.txn_id = tp.txn_id "
            "WHERE tp.partition=? AND t.state='open'", (partition,)).fetchone()
        return row["f"] if row and row["f"] is not None else hw

    @staticmethod
    def _dedup_floor(con, partition, hw):
        row = con.execute(
            "SELECT MIN(offset) AS o FROM dedup WHERE partition=?",
            (partition,)).fetchone()
        return row["o"] if row and row["o"] is not None else hw

    def _scan(self, partition, offset, max_records, isolation, max_scan,
              hw, retained, lso, segs, txn_states):
        base = {"partition": partition, "isolation": isolation,
                "high_watermark": hw, "last_stable_offset": lso,
                "retained_below": retained}
        if offset < retained:
            return dict(base, records=[], next_offset=retained,
                        reason="retained_boundary")
        limit = hw if isolation == ISOLATION_READ_UNCOMMITTED else min(lso, hw)
        pos = min(offset, limit)
        records = []
        scanned = 0
        stop = False
        for seg in segs:
            if stop:
                break
            if seg["max_offset"] < pos:
                continue
            if seg["base_offset"] >= limit:
                break
            if len(records) >= max_records or scanned >= max_scan:
                break
            recs = self._read_segment(seg["path"],
                                      sealed=(seg["state"] == "sealed"))
            for rec in recs:
                if rec.offset < pos:
                    continue
                if rec.offset >= limit:
                    stop = True
                    break
                scanned += 1
                pos = rec.offset + 1
                status = None
                deliver = False
                if rec.txn_id == 0:
                    deliver = True
                else:
                    st = txn_states.get(rec.txn_id)
                    if st is None:
                        raise TxLogError(
                            f"transaction state for txn {rec.txn_id} "
                            f"(offset {rec.offset}) is missing")
                    if isolation == ISOLATION_READ_UNCOMMITTED:
                        deliver = True
                        status = st
                    elif st == "committed":
                        deliver = True
                        status = st
                    elif st == "aborted":
                        deliver = False  # abort hole: skip, keep position
                    else:
                        raise TxLogError(
                            f"open transaction record at offset {rec.offset} "
                            f"below the stable frontier {lso}")
                if deliver:
                    records.append({
                        "offset": rec.offset,
                        "payload": rec.payload,
                        "txn_id": rec.txn_id or None,
                        "txn_status": status,
                        "producer_id": rec.producer_id or None,
                        "epoch": rec.epoch if rec.epoch >= 0 else None,
                        "seq": rec.seq if rec.seq >= 0 else None,
                    })
                if len(records) >= max_records or scanned >= max_scan:
                    break
        if pos >= limit:
            reason = "log_end" if limit == hw else "stable_frontier"
        elif len(records) >= max_records:
            reason = "page_full"
        else:
            reason = "scan_limit"
        return dict(base, records=records, next_offset=pos, reason=reason)

    # -- retain ----------------------------------------------------------------

    def retain(self, partition, before_offset):
        """Request removal of old data of ``partition`` below ``before_offset``.

        The actually applied boundary is the minimum of the request and the
        hard constraints: the partition's stable frontier (pending
        transactions keep their data), the dedup retention floor (retry
        identity is never lost while inside its window) and every LIVE
        reader's contiguous safe cursor on this partition (a slow reader
        keeps the segments it has not finished processing).  Only sealed
        segments fully below the applied boundary are unlinked; the
        retention frontier itself is logical and exact.  Returns the
        evidence: applied boundary, deleted segments, binding constraint.
        """
        self._check_writable()
        partition = _as_partition(partition)
        before_offset = _as_offset(before_offset)
        with self._write_mu():
            with self._retain_exclusive():
                con = self._connect()
                try:
                    prow = con.execute(
                        "SELECT * FROM partitions WHERE partition=?",
                        (partition,)).fetchone()
                    if prow is None:
                        return {"partition": partition,
                                "requested": before_offset, "applied": 0,
                                "retained_below": 0, "deleted_segments": [],
                                "constrained_by": "empty", "lso": 0,
                                "dedup_floor": 0, "reader_floor": None,
                                "reader_holds": [], "high_watermark": 0}
                    hw = prow["next_offset"]
                    retained = prow["retained_below"]
                    lso = self._lso(con, partition, hw)
                    floor = self._dedup_floor(con, partition, hw)
                    reader_floor = self._reader_floor(con, partition)
                    candidates = [before_offset, lso, floor, hw]
                    if reader_floor is not None:
                        candidates.append(reader_floor)
                    applied = min(candidates)
                    new_retained = max(retained, applied)
                    reader_holds = [dict(r) for r in con.execute(
                        "SELECT reader_id, safe_offset FROM reader_positions "
                        "WHERE partition=? "
                        "ORDER BY safe_offset, reader_id",
                        (partition,))]
                    doomed = [dict(r) for r in con.execute(
                        "SELECT * FROM segments WHERE partition=? AND "
                        "state='sealed' AND max_offset < ? "
                        "ORDER BY base_offset", (partition, new_retained))]
                    with self._txn(con):
                        for s in doomed:
                            con.execute(
                                "UPDATE segments SET state='deleted' WHERE "
                                "partition=? AND base_offset=?",
                                (partition, s["base_offset"]))
                        if new_retained > retained:
                            con.execute(
                                "UPDATE partitions SET retained_below=? "
                                "WHERE partition=?", (new_retained, partition))
                        self._gc_transactions(con)
                    self._fault.maybe("retain.after_catalog")
                    deleted = []
                    for s in doomed:
                        try:
                            os.unlink(os.path.join(self.path, s["path"]))
                        except FileNotFoundError:
                            pass
                        self._cache_drop(s["path"])
                        deleted.append({"base_offset": s["base_offset"],
                                        "path": s["path"]})
                    self._fault.maybe("retain.after_delete")
                    constrained = _constraint(before_offset, applied, lso,
                                              floor, hw, reader_floor)
                    return {"partition": partition,
                            "requested": before_offset, "applied": applied,
                            "retained_below": new_retained,
                            "deleted_segments": deleted,
                            "constrained_by": constrained, "lso": lso,
                            "dedup_floor": floor,
                            "reader_floor": reader_floor,
                            "reader_holds": reader_holds,
                            "high_watermark": hw}
                finally:
                    con.close()

    def _gc_transactions(self, con):
        """Drop decided-transaction metadata once EVERY partition it touched
        has retained past the transaction's records.  Cross-partition
        visibility evidence can therefore never be lost by one partition
        cleaning ahead of another."""
        retained = {r["partition"]: r["retained_below"] for r in
                    con.execute("SELECT partition, retained_below "
                                "FROM partitions")}
        for t in con.execute(
                "SELECT txn_id FROM transactions WHERE state != 'open'"):
            parts = con.execute(
                "SELECT partition, last_offset FROM txn_parts WHERE txn_id=?",
                (t["txn_id"],)).fetchall()
            if all(retained.get(pp, 0) > lo for pp, lo in parts):
                con.execute("DELETE FROM txn_parts WHERE txn_id=?",
                            (t["txn_id"],))
                con.execute("DELETE FROM transactions WHERE txn_id=?",
                            (t["txn_id"],))

    # -- inspect ---------------------------------------------------------------

    def inspect(self, partition=None, raw=False):
        """Diagnostic snapshot: producers/epochs, transactions with their
        per-partition offset ranges, per-partition watermarks, retention
        evidence (retained_below, dedup floor, sequence windows, segment
        catalog incl. deleted entries).  raw=True additionally parses every
        live segment and lists each record with its transaction state."""
        self._check_open()
        with self._retain_shared():
            con = self._connect()
            try:
                out = {"config": {"format": FORMAT_VERSION,
                              "segment_bytes": self.segment_bytes,
                              "dedup_window": self.dedup_window},
                       "producers": {}, "transactions": [],
                       "partitions": {}, "readers": {},
                       "recovery_report": list(self.recovery_report)}
                for r in con.execute(
                        "SELECT * FROM producers ORDER BY producer_id"):
                    out["producers"][r["producer_id"]] = r["epoch"]
                parts_by_txn = {}
                for r in con.execute("SELECT * FROM txn_parts"):
                    parts_by_txn.setdefault(r["txn_id"], {})[r["partition"]] = \
                        [r["first_offset"], r["last_offset"]]
                txn_states = {}
                for r in con.execute(
                        "SELECT * FROM transactions ORDER BY txn_id"):
                    txn_states[r["txn_id"]] = r["state"]
                    out["transactions"].append({
                        "txn_id": r["txn_id"], "producer_id": r["producer_id"],
                        "epoch": r["epoch"], "state": r["state"],
                        "partitions": parts_by_txn.get(r["txn_id"], {})})
                q = "SELECT * FROM partitions"
                args = ()
                if partition is not None:
                    q += " WHERE partition=?"
                    args = (partition,)
                for r in con.execute(q, args):
                    p = r["partition"]
                    segs = [dict(s) for s in con.execute(
                        "SELECT * FROM segments WHERE partition=? "
                        "ORDER BY base_offset", (p,))]
                    pinfo = {
                        "next_offset": r["next_offset"],
                        "retained_below": r["retained_below"],
                        "last_stable_offset": self._lso(con, p,
                                                      r["next_offset"]),
                        "dedup_floor": self._dedup_floor(con, p,
                                                       r["next_offset"]),
                        "reader_floor": self._reader_floor(con, p),
                        "reader_holds": [dict(s) for s in con.execute(
                            "SELECT reader_id, safe_offset FROM "
                            "reader_positions WHERE partition=? "
                            "ORDER BY safe_offset, reader_id", (p,))],
                        "sequences": [dict(s) for s in con.execute(
                            "SELECT * FROM seq_state WHERE partition=? "
                            "ORDER BY producer_id", (p,))],
                        "segments": segs,
                    }
                    if raw:
                        for s in segs:
                            if s["state"] == "deleted":
                                continue
                            s["records"] = self._raw_records(s, txn_states)
                    out["partitions"][p] = pinfo
                for r in con.execute(
                        "SELECT * FROM readers ORDER BY reader_id"):
                    entry = {"start_mode": r["start_mode"],
                             "start_offset": r["start_offset"],
                             "partitions": {}}
                    for pr in con.execute(
                            "SELECT * FROM reader_positions WHERE reader_id=? "
                            "ORDER BY partition", (r["reader_id"],)):
                        gaps = [x["offset"] for x in con.execute(
                            "SELECT offset FROM reader_acks WHERE reader_id=? "
                            "AND partition=? AND offset>=? ORDER BY offset",
                            (r["reader_id"], pr["partition"],
                             pr["safe_offset"]))]
                        entry["partitions"][pr["partition"]] = {
                            "read_offset": pr["read_offset"],
                            "safe_offset": pr["safe_offset"],
                            "blocking_gaps": gaps}
                    out["readers"][r["reader_id"]] = entry
                return out
            finally:
                con.close()

    def _raw_records(self, seg, txn_states):
        full = os.path.join(self.path, seg["path"])
        if not os.path.exists(full):
            return {"error": f"segment file {seg['path']} is missing"}
        try:
            with open(full, "rb") as fh:
                data = fh.read()
            recs, good = storage.parse_segment(data)
            out = {"parsed": len(recs), "good_end": good,
                   "file_bytes": len(data),
                   "torn_tail": good < len(data), "records": []}
            for rec in recs:
                out["records"].append({
                    "offset": rec.offset, "txn_id": rec.txn_id,
                    "txn_state": (txn_states.get(rec.txn_id)
                                  if rec.txn_id else None),
                    "producer_id": rec.producer_id or None,
                    "epoch": rec.epoch if rec.epoch >= 0 else None,
                    "seq": rec.seq if rec.seq >= 0 else None,
                    "payload_len": len(rec.payload),
                    "payload_preview": rec.payload[:24].hex(),
                    "crc_ok": True,
                })
            return out
        except (storage.SegmentCorruption, OSError) as e:
            return {"error": str(e)}


def _constraint(requested, applied, lso, floor, hw, reader_floor=None):
    if applied >= requested:
        return "requested"
    # a live reader's safe cursor pins retention; report it first when it
    # is the binding minimum so diagnostics name the blocking reader.
    if reader_floor is not None and applied == reader_floor and reader_floor < hw:
        return "reader"
    if applied == lso and lso < hw:
        return "lso"
    if applied == floor and floor < hw:
        return "dedup_window"
    if applied == hw:
        return "high_watermark"
    return "requested"


def _as_bytes(data):
    if isinstance(data, str):
        return data.encode("utf-8")
    return bytes(data)


def _as_partition(partition):
    p = int(partition)
    if p < 0:
        raise ValueError("partition must be >= 0")
    return p


def _as_offset(offset):
    o = int(offset)
    if o < 0:
        raise ValueError("offset must be >= 0")
    return o


def _as_seq(seq):
    s = int(seq)
    if s < 0:
        raise ValueError("seq must be >= 0")
    return s
