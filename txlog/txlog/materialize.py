"""Local incremental materialization: event assignment details.

A :class:`MaterializedStore` binds ONE separate SQLite database to ONE txlog
source (identified by the source's durable ``log_id``) and turns the source's
committed ``event`` / ``assignment`` JSON records into a directly usable
"event ownership" detail table.

What is guaranteed
------------------
* **Transaction-atomic prefixes.**  A refresh takes one source snapshot and
  chooses, per partition, the maximal prefix vector bounded by the stable
  frontiers such that every *committed* transaction is either fully inside
  the prefix or fully outside it.  A pending transaction on another
  partition can never make half of a cross-partition transaction visible.
* **All safe prefixes are published.**  One blocked partition never stalls
  the others; each partition independently advances as far as the
  fixed-point prefix allows.  Abort records produce no details but keep
  their offset-advance semantics.
* **Business time, not ingest time.**  An event takes the most recent
  assignment of the same entity whose ``effective_from`` is not later than
  the event's ``event_time``.  Late-arriving rules re-assign already
  published events on the next refresh; events with no matching rule stay
  published, explicitly marked unassigned.
* **Versioned releases with net changes.**  Details, changes, the release
  number and the published source positions commit in ONE SQLite
  transaction.  A refresh with no new positions creates no release and never
  wipes the previous release's changes.  Changes are computed against the
  final state of the whole round — intermediate values are never reported.
* **Incremental.**  Only not-yet-materialized source bodies are read;
  re-evaluation is scoped to the entities whose rules changed.  Source
  payloads are never mirrored into this database — only parsed facts.
* **Identity / gap safety.**  A different log at the same path, or a source
  that has already retained away unprocessed offsets, refuses the refresh
  while keeping the previous result readable.

The store never writes to the source log and uses only the public
``read_committed`` fetch contract, so the producer/fetch/reader/retain
contracts are untouched.
"""

import base64
import fcntl
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from .errors import (HistoryGapError, MaterializationBusy,
                     MaterializationDataError, MaterializationError,
                     RuleConflictError, SourceIdentityError, StaleCursorError,
                     UnknownRelease)
from .faults import FaultInjector
from .log import ISOLATION_READ_COMMITTED, Log

MAT_SCHEMA_VERSION = "1"

MAT_SCHEMA = """
CREATE TABLE IF NOT EXISTS mat_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per published release.  Releases are gapless: 1, 2, 3, ...
CREATE TABLE IF NOT EXISTS releases (
    release_no   INTEGER PRIMARY KEY,
    committed_at REAL NOT NULL
);

-- The published source-position vector OF EACH release.
CREATE TABLE IF NOT EXISTS release_positions (
    release_no INTEGER NOT NULL,
    partition  INTEGER NOT NULL,
    offset     INTEGER NOT NULL,
    PRIMARY KEY (release_no, partition)
);

-- Latest published vector (convenience mirror of the last release).
CREATE TABLE IF NOT EXISTS positions (
    partition INTEGER PRIMARY KEY,
    offset    INTEGER NOT NULL
);

-- Every assignment version fact parsed from the source.  Payloads are NOT
-- stored; only the parsed business fields and the rule's source location.
-- Same (entity, effective_from, revision, group) arriving from several
-- locations is the same version; evaluation uses the smallest location.
CREATE TABLE IF NOT EXISTS assignments (
    entity         TEXT NOT NULL,
    effective_from INTEGER NOT NULL,
    revision       INTEGER NOT NULL,
    grp            TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset  INTEGER NOT NULL,
    PRIMARY KEY (entity, effective_from, revision,
                 source_partition, source_offset)
);
CREATE INDEX IF NOT EXISTS idx_assignments_entity
    ON assignments(entity, effective_from);

-- Current materialized detail of every published event.  An event exists
-- exactly once, keyed by its business identity (source partition, offset):
-- identical bodies at different coordinates stay two distinct events.
CREATE TABLE IF NOT EXISTS details (
    event_partition    INTEGER NOT NULL,
    event_offset       INTEGER NOT NULL,
    entity             TEXT NOT NULL,
    event_time         INTEGER NOT NULL,
    assigned           INTEGER NOT NULL CHECK (assigned IN (0, 1)),
    grp                TEXT,
    rule_effective_from INTEGER,
    rule_revision      INTEGER,
    rule_partition     INTEGER,
    rule_offset        INTEGER,
    PRIMARY KEY (event_partition, event_offset)
);
CREATE INDEX IF NOT EXISTS idx_details_entity ON details(entity);

-- Net changes of each release, computed against the final round state.
-- before_* columns are NULL for change_type='inserted'.
CREATE TABLE IF NOT EXISTS changes (
    release_no     INTEGER NOT NULL,
    event_partition INTEGER NOT NULL,
    event_offset   INTEGER NOT NULL,
    change_type    TEXT NOT NULL CHECK (change_type IN ('inserted','corrected')),
    before_assigned INTEGER,
    before_grp     TEXT,
    before_rule_effective_from INTEGER,
    before_rule_revision INTEGER,
    before_rule_partition INTEGER,
    before_rule_offset INTEGER,
    after_assigned  INTEGER NOT NULL,
    after_grp       TEXT,
    after_rule_effective_from INTEGER,
    after_rule_revision INTEGER,
    after_rule_partition INTEGER,
    after_rule_offset INTEGER,
    PRIMARY KEY (release_no, event_partition, event_offset)
);
"""

# In-process refresh mutual exclusion, keyed by the absolute mat-db path.
_REFRESH_LOCKS = {}
_REFRESH_LOCKS_MU = threading.Lock()


def _refresh_lock_for(path):
    with _REFRESH_LOCKS_MU:
        lk = _REFRESH_LOCKS.get(path)
        if lk is None:
            lk = threading.RLock()
            _REFRESH_LOCKS[path] = lk
        return lk


def connect_mat(db_path):
    """Open the materialization SQLite database (WAL, synchronous=FULL)."""
    con = sqlite3.connect(db_path, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.isolation_level = None
    con.execute("PRAGMA busy_timeout = 30000")
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = FULL")
    return con


def open_materialized(mat_path, source_path=None, *, create=True,
                      lock_timeout=30.0):
    """Open (and optionally create) a materialized store.

    ``mat_path`` is the SQLite file of the materialized store.  When
    creating, ``source_path`` must point at the bound txlog directory; the
    store is permanently bound to that log's durable ``log_id``.
    """
    return MaterializedStore(mat_path, source_path, create=create,
                             lock_timeout=lock_timeout)


# ---------------------------------------------------------------------------
# record validation
# ---------------------------------------------------------------------------

# SQLite caps bound variables per statement; chunk every large IN-clause.
_SQL_VAR_LIMIT = 900


def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class _BadRecord(Exception):
    def __init__(self, reason):
        self.reason = reason


def _require_object(doc):
    if not isinstance(doc, dict):
        raise _BadRecord("body_must_be_json_object")


def _require_string(doc, key):
    val = doc.get(key)
    if not isinstance(val, str) or val == "":
        raise _BadRecord(f"field_{key}_must_be_nonempty_string")
    return val


def _require_integer(doc, key, *, nonneg=False):
    val = doc.get(key)
    # bool is an int subclass in Python; it is not an acceptable integer here.
    if isinstance(val, bool) or not isinstance(val, int):
        raise _BadRecord(f"field_{key}_must_be_integer")
    if nonneg and val < 0:
        raise _BadRecord(f"field_{key}_must_be_nonnegative")
    return val


def _decode_body(payload, partition, offset):
    """Validate one committed source body against the event/assignment
    contract.  Returns ``(kind, fields)`` or raises MaterializationDataError
    with the exact source coordinate."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MaterializationDataError(
            f"partition {partition} offset {offset}: body is not valid "
            f"UTF-8: {exc}", partition=partition, offset=offset,
            reason="invalid_utf8")
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MaterializationDataError(
            f"partition {partition} offset {offset}: body is not valid "
            f"JSON: {exc}", partition=partition, offset=offset,
            reason="invalid_json")
    try:
        _require_object(doc)
        kind = doc.get("kind")
        if kind == "event":
            entity = _require_string(doc, "entity")
            event_time = _require_integer(doc, "event_time")
            return "event", {"entity": entity, "event_time": event_time}
        if kind == "assignment":
            entity = _require_string(doc, "entity")
            effective_from = _require_integer(doc, "effective_from")
            revision = _require_integer(doc, "revision", nonneg=True)
            group = _require_string(doc, "group")
            return "assignment", {"entity": entity,
                                  "effective_from": effective_from,
                                  "revision": revision, "group": group}
        if not isinstance(kind, str) or not kind:
            raise _BadRecord("missing_kind")
        raise _BadRecord(f"unknown_kind_{kind}")
    except _BadRecord as exc:
        raise MaterializationDataError(
            f"partition {partition} offset {offset}: {exc.reason}",
            partition=partition, offset=offset, reason=exc.reason)


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------

class MaterializedStore:
    """A versioned, incrementally refreshed materialization of one txlog."""

    def __init__(self, mat_path, source_path=None, *, create=True,
                 lock_timeout=30.0):
        self.mat_path = os.path.abspath(mat_path)
        self.lock_timeout = lock_timeout
        parent = os.path.dirname(self.mat_path)
        if parent and not os.path.isdir(parent):
            if not create:
                raise MaterializationError(
                    f"materialized store directory {parent!r} does not exist")
            os.makedirs(parent, exist_ok=True)
        self._lock_path = self.mat_path + ".refresh.lock"
        self._refresh_mu = _refresh_lock_for(self.mat_path)
        self._closed = False
        self._fault = FaultInjector()

        fresh = not os.path.exists(self.mat_path)
        if fresh and not create:
            raise MaterializationError(
                f"materialized store {self.mat_path!r} does not exist")
        con = connect_mat(self.mat_path)
        try:
            con.executescript(MAT_SCHEMA)
            row = con.execute(
                "SELECT value FROM mat_meta WHERE key='schema_version'").fetchone()
            if row is None:
                if source_path is None:
                    raise MaterializationError(
                        "creating a materialized store requires source_path "
                        "of the bound txlog")
                log_id, abs_source = self._read_source_identity(source_path)
                con.execute("INSERT INTO mat_meta VALUES ('schema_version', ?)",
                            (MAT_SCHEMA_VERSION,))
                con.execute("INSERT INTO mat_meta VALUES ('log_id', ?)",
                            (log_id,))
                con.execute("INSERT INTO mat_meta VALUES ('source_path', ?)",
                            (abs_source,))
                con.execute("INSERT INTO mat_meta VALUES ('created_at', ?)",
                            (str(time.time()),))
            else:
                if row["value"] != MAT_SCHEMA_VERSION:
                    raise MaterializationError(
                        f"unsupported materialization schema version "
                        f"{row['value']!r}")
        finally:
            con.close()

    # -- lifecycle ----------------------------------------------------------

    def close(self):
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _check_open(self):
        if self._closed:
            raise MaterializationError("materialized store is closed")

    @contextmanager
    def _refresh_lock(self):
        """Process-wide (RLock) and cross-process (flock) exclusion."""
        deadline = time.monotonic() + self.lock_timeout
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        acquired = False
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError:
                    if time.monotonic() > deadline:
                        raise MaterializationBusy(
                            "another process is refreshing this store; "
                            "concurrent refreshes must not build on the "
                            "same old release")
                    time.sleep(0.05)
            with self._refresh_mu:
                yield
        finally:
            if acquired:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
            else:
                os.close(fd)

    # -- source binding -----------------------------------------------------

    @staticmethod
    def _read_source_identity(source_path):
        log = Log(source_path, writable=False)
        try:
            if not getattr(log, "log_id", None):
                raise MaterializationError(
                    "source log has no durable log_id; open it once writable "
                    "so its persistent identity is minted before binding a "
                    "materialized store")
            return log.log_id, log.path
        finally:
            log.close()

    def _open_source(self, source_path=None):
        """Open the bound source read-only and verify its durable identity."""
        con = connect_mat(self.mat_path)
        try:
            bound_id = con.execute(
                "SELECT value FROM mat_meta WHERE key='log_id'").fetchone()["value"]
            bound_path = con.execute(
                "SELECT value FROM mat_meta WHERE key='source_path'").fetchone()["value"]
        finally:
            con.close()
        path = os.path.abspath(source_path) if source_path else bound_path
        log = Log(path, writable=False)
        try:
            actual = getattr(log, "log_id", None)
            if actual != bound_id:
                raise SourceIdentityError(
                    f"refusing refresh: source at {path!r} has log_id "
                    f"{actual!r}, but this store is bound to {bound_id!r} "
                    f"(bound path {bound_path!r}); previous materialized "
                    f"result left untouched",
                    expected_log_id=bound_id, actual_log_id=actual,
                    expected_path=bound_path, actual_path=path)
            return log
        except Exception:
            log.close()
            raise

    # -- public status ------------------------------------------------------

    def status(self):
        """Publish state: binding, latest release, position vector, counts."""
        self._check_open()
        con = connect_mat(self.mat_path)
        try:
            meta = {r["key"]: r["value"] for r in
                    con.execute("SELECT * FROM mat_meta")}
            latest = con.execute(
                "SELECT MAX(release_no) AS m FROM releases").fetchone()["m"]
            positions = {r["partition"]: r["offset"] for r in
                         con.execute("SELECT * FROM positions ORDER BY partition")}
            n_events = con.execute(
                "SELECT COUNT(*) AS c FROM details").fetchone()["c"]
            n_assigned = con.execute(
                "SELECT COUNT(*) AS c FROM details WHERE assigned=1").fetchone()["c"]
            return {
                "mat_path": self.mat_path,
                "source_path": meta.get("source_path"),
                "source_log_id": meta.get("log_id"),
                "latest_release": latest,
                "positions": positions,
                "event_count": n_events,
                "assigned_count": n_assigned,
                "unassigned_count": n_events - n_assigned,
            }
        finally:
            con.close()

    # -- refresh ------------------------------------------------------------

    def refresh(self, source_path=None, *, fault_hook=None):
        """Run one incremental refresh; returns the publish report.

        Atomic: any invalid selected body, rule conflict or incomplete
        source range aborts the whole round — no half-published state, no
        new release number.  ``fault_hook`` is a test-only callable invoked
        at the materialization fault points; the ``TXLOG_FAULT`` environment
        variable drives real process exits at the same points.
        """
        self._check_open()
        injector = FaultInjector(fault_hook)
        old_injector = self._fault
        self._fault = injector
        try:
            with self._refresh_lock():
                return self._refresh_locked(source_path)
        finally:
            self._fault = old_injector

    def _fault_point(self, point):
        self._fault.maybe(point)

    def _refresh_locked(self, source_path):
        log = self._open_source(source_path)
        try:
            # The retain shared lock is held for the whole scan: a segment we
            # are reading cannot be unlinked under us, and the source cannot
            # swap a live segment for empty data mid-refresh.
            with log._retain_shared():
                return self._refresh_under_lock(log)
        finally:
            log.close()

    def _refresh_under_lock(self, log):
        mat = connect_mat(self.mat_path)
        try:
            # 1. durable starting vector
            last_vec = {r["partition"]: r["offset"] for r in
                        mat.execute("SELECT * FROM positions")}

            # 2. one source snapshot: partitions, committed/open txn parts,
            #    segment catalog and the transaction-state map used by _scan
            snap = self._source_snapshot(log)
            try:
                partitions = sorted(snap["parts"])
                hw = {p: snap["parts"][p]["next_offset"] for p in partitions}
                retained = {p: snap["parts"][p]["retained_below"]
                            for p in partitions}

                # 3. history-gap / identity-of-progress check: every position
                #    still owed to us must still be readable.
                for p in partitions:
                    if last_vec.get(p, 0) < retained[p]:
                        raise HistoryGapError(
                            f"partition {p}: source retained offsets "
                            f"[{last_vec.get(p, 0)}, {retained[p]}) that "
                            f"were never materialized; refusing to skip them",
                            partition=p, missing_from=last_vec.get(p, 0),
                            missing_to=retained[p])
                for p, off in last_vec.items():
                    if p not in snap["parts"]:
                        raise HistoryGapError(
                            f"partition {p} previously published to {off} is "
                            f"gone from the source view",
                            partition=p, missing_from=0, missing_to=off)

                # 4. stable frontier vector U (exclusive upper bounds)
                upper = {}
                for p in partitions:
                    lso = self._lso(snap, p, hw[p])
                    upper[p] = min(lso, hw[p])
                    if upper[p] < last_vec.get(p, 0):
                        raise HistoryGapError(
                            f"partition {p}: stable frontier {upper[p]} is "
                            f"behind the published position "
                            f"{last_vec.get(p, 0)}",
                            partition=p, missing_from=upper[p],
                            missing_to=last_vec.get(p, 0))

                # 5. maximal txn-atomic prefix vector V (fixed point).
                # A record at first offset f is inside a prefix exactly when
                # f < vec[p].  Hence every committed transaction is either:
                #   all-visible  every part's first offset below its prefix,
                #   all-hidden   every part's first offset at/above it, or
                #   split        visible on some partition(s), hidden on
                #                 others (this includes a part the prefix
                #                 cuts through, e.g. a stable frontier that
                #                 stops in the middle of the part).
                # A split transaction is excluded entirely: every visible
                # part pulls its partition back to that part's first offset.
                # The pull can make another transaction split in turn, hence
                # the fixed-point loop.  This is the maximal vector bounded
                # by U and never below the previously published position.
                vec = dict(upper)
                txn_parts = self._committed_parts(snap)
                while True:
                    cut = False
                    for txn_id, parts in txn_parts.items():
                        visible = [p for p, first, _last in parts
                                   if first < vec.get(p, 0)]
                        hidden = [p for p, first, _last in parts
                                  if first >= vec.get(p, 0)]
                        if not visible or not hidden:
                            continue  # all-in or all-out already
                        for p, first, _last in parts:
                            if p in visible and first < vec[p]:
                                vec[p] = first
                                cut = True
                    if not cut:
                        break
                # post-condition: every committed txn is all-in or all-out
                for txn_id, parts in txn_parts.items():
                    visibility = {("in" if first < vec.get(p, 0) else "out")
                                  for p, first, _last in parts}
                    assert len(visibility) == 1, (
                        txn_id, visibility, parts, vec)
                for p in partitions:
                    assert vec[p] >= last_vec.get(p, 0)

                deferred = self._deferred(snap, partitions, upper, vec)

                # 6. nothing new -> no release, previous changes untouched
                if all(vec.get(p, 0) == last_vec.get(p, 0)
                       for p in set(vec) | set(last_vec)):
                    return self._no_publish_report(mat, partitions, vec,
                                                   upper, deferred)

                # 7. read + validate every newly reachable source body
                events, assignments, batch_keys = self._ingest(
                    log, snap, partitions, last_vec, vec)

                # 8. rule-version conflicts against batch + stored versions
                self._check_conflicts(mat, assignments, batch_keys)

                # 9. scoped re-evaluation and one atomic publish
                report = self._publish(
                    mat, partitions, vec, upper, deferred,
                    events, assignments)
                self._fault_point("materialize.after_commit")
                return report
            finally:
                try:
                    snap["con"].execute("ROLLBACK")
                finally:
                    snap["con"].close()
        finally:
            mat.close()

    @staticmethod
    def _source_snapshot(log):
        """One read snapshot of partition watermarks, transaction parts, the
        segment catalog and the per-partition transaction-state map.  All
        reads of this refresh resolve against this single snapshot."""
        con = log._connect()
        con.execute("BEGIN")
        parts = {r["partition"]: dict(r) for r in
                 con.execute("SELECT * FROM partitions")}
        committed, open_parts = [], []
        for r in con.execute(
                "SELECT t.txn_id AS txn_id, t.state AS state, "
                "tp.partition AS partition, tp.first_offset AS first_offset, "
                "tp.last_offset AS last_offset "
                "FROM transactions t JOIN txn_parts tp "
                "ON tp.txn_id = t.txn_id"):
            if r["state"] == "committed":
                committed.append((r["txn_id"], r["partition"],
                                  r["first_offset"], r["last_offset"]))
            elif r["state"] == "open":
                open_parts.append((r["txn_id"], r["partition"],
                                   r["first_offset"], r["last_offset"]))
        segments, txn_states = {}, {}
        for p in parts:
            segments[p] = [dict(s) for s in con.execute(
                "SELECT * FROM segments WHERE partition=? AND state != 'deleted' "
                "ORDER BY base_offset", (p,))]
            for r in con.execute(
                    "SELECT DISTINCT t.txn_id AS txn_id, t.state AS state "
                    "FROM transactions t JOIN txn_parts tp "
                    "ON tp.txn_id = t.txn_id WHERE tp.partition=? "
                    "AND tp.last_offset>=? AND tp.first_offset<?",
                    (p, parts[p]["retained_below"],
                     max(parts[p]["next_offset"], 1))):
                txn_states[(p, r["txn_id"])] = r["state"]
        return {"con": con, "parts": parts, "committed": committed,
                "open": open_parts, "segments": segments,
                "txn_states": txn_states}

    @staticmethod
    def _committed_parts(snap):
        """txn_id -> list of (partition, first_offset, last_offset) parts."""
        out = {}
        for txn_id, p, first, last in snap["committed"]:
            out.setdefault(txn_id, []).append((p, first, last))
        return out

    @staticmethod
    def _lso(snap, partition, hw):
        starts = [first for _tid, p, first, _last in snap["open"]
                  if p == partition]
        return min(starts) if starts else hw

    @staticmethod
    def _deferred(snap, partitions, upper, vec):
        """Explain every partition that could not reach its stable upper bound."""
        out = []
        for p in partitions:
            if vec[p] >= upper[p]:
                continue
            reason = MaterializedStore._block_reason(snap, p, vec)
            out.append({"partition": p, "requested_upper": upper[p],
                        "published_offset": vec[p], **reason})
        return out

    @staticmethod
    def _block_reason(snap, partition, vec):
        """Walk the committed-transaction wait chain to the open transaction
        that ultimately pins ``partition`` at its published offset."""
        v = vec[partition]
        # direct open blocker starting exactly at the cut
        for tid, q, first, _last in snap["open"]:
            if q == partition and first == v:
                return {"reason": "pending_transaction",
                        "transaction": tid, "blocking_partition": q,
                        "blocking_offset": first}
        # committed txns whose part on this partition starts at the cut
        chain = [tid for tid, q, first, last in snap["committed"]
                 if q == partition and first == v and last >= v]
        seen = set()
        while chain:
            tid = chain.pop()
            if tid in seen:
                continue
            seen.add(tid)
            for _t, q, first, last in snap["committed"]:
                if _t != tid:
                    continue
                if last < vec[q]:
                    continue  # this part is fully inside, not the blocker
                for otid, oq, ofirst, _olast in snap["open"]:
                    if oq == q and ofirst <= vec[q]:
                        return {"reason": "pending_transaction",
                                "transaction": otid,
                                "waiting_transaction": tid,
                                "blocking_partition": oq,
                                "blocking_offset": ofirst}
                chain.extend(t for t, qq, ff, ll in snap["committed"]
                             if qq == q and ff == vec[q] and ll >= vec[q])
        return {"reason": "transaction_waiting",
                "blocking_partition": partition, "blocking_offset": v}

    def _ingest(self, log, snap, partitions, last_vec, vec):
        """Read all committed bodies in the new prefixes; validate them.

        Uses the snapshot's segment catalog/txn map via ``Log._scan`` under
        the retain shared lock already held by the caller, so a concurrent
        retain cannot unlink a segment mid-read and every page resolves
        against the same transaction-state snapshot.
        """
        events = []        # dicts: partition, offset, entity, event_time
        assignments = []   # dicts: entity, effective_from, revision, group,
                           #        source_partition, source_offset
        batch_keys = set()
        for p in partitions:
            start = last_vec.get(p, 0)
            end = vec[p]
            if start == end:
                continue
            part_hw = snap["parts"][p]["next_offset"]
            part_retained = snap["parts"][p]["retained_below"]
            part_states = {tid: st for (pp, tid), st in
                           snap["txn_states"].items() if pp == p}
            off = start
            while off < end:
                page = log._scan(
                    p, off, 1000, ISOLATION_READ_COMMITTED, 4096,
                    part_hw, part_retained, end, snap["segments"][p],
                    part_states)
                for rec in page["records"]:
                    if rec["offset"] >= end:
                        break
                    kind, fields = _decode_body(rec["payload"], p,
                                                rec["offset"])
                    if kind == "event":
                        events.append({"partition": p, "offset": rec["offset"],
                                       "entity": fields["entity"],
                                       "event_time": fields["event_time"]})
                    else:
                        row = {"entity": fields["entity"],
                               "effective_from": fields["effective_from"],
                               "revision": fields["revision"],
                               "group": fields["group"],
                               "source_partition": p,
                               "source_offset": rec["offset"]}
                        assignments.append(row)
                        batch_keys.add((row["entity"],
                                        row["effective_from"], row["revision"]))
                nxt = page["next_offset"]
                if nxt <= off:
                    raise MaterializationError(
                        f"partition {p}: scan made no progress at offset "
                        f"{off} while reading prefix [{start}, {end})")
                off = nxt
            if off < end:
                # Defensive: the stable snapshot promised every offset below
                # vec[p]; an incomplete segment set must not publish a gap.
                raise HistoryGapError(
                    f"partition {p}: source range [{off}, {end}) is not "
                    f"completely readable", partition=p, missing_from=off,
                    missing_to=end)
        return events, assignments, batch_keys

    def _check_conflicts(self, mat, assignments, batch_keys):
        """Reject the round if one (entity, effective_from, revision) ever
        carries more than one distinct group — in the batch or against
        already stored versions."""
        versions = {}  # key -> {group: smallest (p, off)}
        keys = list(batch_keys)
        for key_chunk in _chunks(keys, _SQL_VAR_LIMIT // 3):
            params = [v for key in key_chunk for v in key]
            where = " OR ".join(
                "(entity=? AND effective_from=? AND revision=?)"
                for _ in key_chunk)
            for r in mat.execute(
                    "SELECT * FROM assignments WHERE " + where, params):
                key = (r["entity"], r["effective_from"], r["revision"])
                loc = versions.setdefault(key, {})
                cur = loc.get(r["grp"])
                cand = (r["source_partition"], r["source_offset"])
                if cur is None or cand < cur:
                    loc[r["grp"]] = cand
        for row in assignments:
            key = (row["entity"], row["effective_from"], row["revision"])
            loc = versions.setdefault(key, {})
            cand = (row["source_partition"], row["source_offset"])
            cur = loc.get(row["group"])
            if cur is None or cand < cur:
                loc[row["group"]] = cand
        for (entity, eff, rev), groups in versions.items():
            if len(groups) > 1:
                locations = [
                    {"group": g, "partition": p, "offset": o}
                    for g, (p, o) in sorted(groups.items())]
                raise RuleConflictError(
                    f"rule version conflict: entity={entity!r} "
                    f"effective_from={eff} revision={rev} carries distinct "
                    f"groups {sorted(groups)} at {locations}",
                    entity=entity, effective_from=eff, revision=rev,
                    locations=locations)

    # -- evaluation ---------------------------------------------------------

    @staticmethod
    def _active_rules(mat, entities):
        """entity -> {effective_from: (revision, group, part, off)}.

        For one (entity, effective_from) the highest revision wins; equal
        revision/group duplicates resolve to the smallest source location.
        Conflicts were rejected before this point.
        """
        active = {e: {} for e in entities}
        if not entities:
            return active
        for chunk in _chunks(list(entities), _SQL_VAR_LIMIT):
            marks = ",".join("?" for _ in chunk)
            for r in mat.execute(
                    "SELECT * FROM assignments WHERE entity IN (" + marks + ")",
                    chunk):
                per_eff = active[r["entity"]]
                cur = per_eff.get(r["effective_from"])
                cand = (r["revision"], r["grp"], r["source_partition"],
                        r["source_offset"])
                if cur is None or cand[0] > cur[0] or (
                        cand[0] == cur[0] and
                        (cand[2], cand[3]) < (cur[2], cur[3])):
                    per_eff[r["effective_from"]] = cand
        return active

    @staticmethod
    def _pick_rule(per_eff, event_time):
        """Most recent rule not later than event_time; None if unmatched."""
        best_eff = None
        for eff in per_eff:
            if eff <= event_time and (best_eff is None or eff > best_eff):
                best_eff = eff
        if best_eff is None:
            return None
        return (best_eff,) + per_eff[best_eff]

    def _publish(self, mat, partitions, vec, upper, deferred,
                 events, assignments):
        mat.execute("BEGIN IMMEDIATE")
        try:
            # store assignment facts (identical duplicates are ignored)
            for row in assignments:
                mat.execute(
                    "INSERT OR IGNORE INTO assignments VALUES (?,?,?,?,?,?)",
                    (row["entity"], row["effective_from"], row["revision"],
                     row["group"], row["source_partition"],
                     row["source_offset"]))

            # entities whose effective rule set or event set changed
            affected = {e["entity"] for e in events}
            affected.update(r["entity"] for r in assignments)
            active = self._active_rules(mat, affected)

            changes = []
            # new events first
            new_coords = set()
            for e in events:
                row = mat.execute(
                    "SELECT 1 FROM details WHERE event_partition=? AND "
                    "event_offset=?", (e["partition"], e["offset"])).fetchone()
                if row is not None:
                    continue  # impossible under monotonic offsets, defensive
                rule = self._pick_rule(active[e["entity"]], e["event_time"])
                detail = self._detail_tuple(e, rule)
                mat.execute(
                    "INSERT INTO details VALUES (?,?,?,?,?,?,?,?,?,?)",
                    detail)
                new_coords.add((e["partition"], e["offset"]))
                changes.append(("inserted", e, None, detail))

            # re-evaluate previously stored events of affected entities.
            # The events inserted above are skipped: their first evaluation
            # already used the round's final rule set, so they can never be a
            # same-round correction.
            if affected:
                for chunk in _chunks(list(affected), _SQL_VAR_LIMIT):
                    marks = ",".join("?" for _ in chunk)
                    for row in mat.execute(
                            "SELECT * FROM details WHERE entity IN ("
                            + marks + ")", chunk):
                        if (row["event_partition"],
                                row["event_offset"]) in new_coords:
                            continue
                        e = {"partition": row["event_partition"],
                             "offset": row["event_offset"],
                             "entity": row["entity"],
                             "event_time": row["event_time"]}
                        rule = self._pick_rule(active[row["entity"]],
                                               row["event_time"])
                        new_detail = self._detail_tuple(e, rule)
                        old_detail = self._detail_tuple_from_row(row)
                        if new_detail != old_detail:
                            mat.execute(
                                "UPDATE details SET entity=?, event_time=?, "
                                "assigned=?, grp=?, rule_effective_from=?, "
                                "rule_revision=?, rule_partition=?, "
                                "rule_offset=? "
                                "WHERE event_partition=? AND event_offset=?",
                                (new_detail[2], new_detail[3], new_detail[4],
                                 new_detail[5], new_detail[6], new_detail[7],
                                 new_detail[8], new_detail[9],
                                 new_detail[0], new_detail[1]))
                            changes.append(("corrected", e, old_detail,
                                            new_detail))

            release_no = mat.execute(
                "SELECT COALESCE(MAX(release_no), 0) + 1 AS n "
                "FROM releases").fetchone()["n"]
            mat.execute("INSERT INTO releases VALUES (?, ?)",
                        (release_no, time.time()))
            for p in partitions:
                mat.execute(
                    "INSERT INTO release_positions VALUES (?,?,?)",
                    (release_no, p, vec[p]))
                mat.execute(
                    "INSERT INTO positions VALUES (?,?) "
                    "ON CONFLICT(partition) DO UPDATE SET offset=excluded.offset",
                    (p, vec[p]))
            for change_type, e, old, new in changes:
                params = [release_no, e["partition"], e["offset"], change_type]
                if old is None:
                    params += [None, None, None, None, None, None]
                else:
                    params += [old[4], old[5], old[6], old[7], old[8], old[9]]
                params += [new[4], new[5], new[6], new[7], new[8], new[9]]
                mat.execute(
                    "INSERT INTO changes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params)
            self._fault_point("materialize.before_commit")
            mat.execute("COMMIT")
        except BaseException:
            mat.execute("ROLLBACK")
            raise

        return {
            "published": True,
            "release_no": release_no,
            "positions": {p: vec[p] for p in partitions},
            "stable_upper": {p: upper[p] for p in partitions},
            "deferred": deferred,
            "events_ingested": len(events),
            "assignments_ingested": len(assignments),
            "changes": [self._change_summary(c) for c in changes],
            "change_count": len(changes),
        }

    @staticmethod
    def _detail_tuple(e, rule):
        # (part, off, entity, event_time, assigned, grp, eff, rev, rpart, roff)
        if rule is None:
            return (e["partition"], e["offset"], e["entity"],
                    e["event_time"], 0, None, None, None, None, None)
        eff, rev, grp, rpart, roff = rule
        return (e["partition"], e["offset"], e["entity"], e["event_time"],
                1, grp, eff, rev, rpart, roff)

    @staticmethod
    def _detail_tuple_from_row(row):
        return (row["event_partition"], row["event_offset"], row["entity"],
                row["event_time"], row["assigned"], row["grp"],
                row["rule_effective_from"], row["rule_revision"],
                row["rule_partition"], row["rule_offset"])

    @staticmethod
    def _detail_dict(t):
        out = {"event": {"partition": t[0], "offset": t[1]},
               "entity": t[2], "event_time": t[3],
               "assigned": bool(t[4]), "group": t[5]}
        if t[4]:
            out["rule"] = {"effective_from": t[6], "revision": t[7],
                           "source": {"partition": t[8], "offset": t[9]}}
        else:
            out["rule"] = None
        return out

    def _change_summary(self, change):
        change_type, e, old, new = change
        out = {"event": {"partition": e["partition"], "offset": e["offset"]},
               "change_type": change_type,
               "after": self._detail_dict(new)}
        out["before"] = None if old is None else self._detail_dict(old)
        return out

    def _no_publish_report(self, mat, partitions, vec, upper, deferred):
        latest = mat.execute(
            "SELECT MAX(release_no) AS m FROM releases").fetchone()["m"]
        return {"published": False, "release_no": latest,
                "positions": {p: vec.get(p, 0) for p in partitions},
                "stable_upper": {p: upper[p] for p in partitions},
                "deferred": deferred,
                "events_ingested": 0, "assignments_ingested": 0,
                "changes": [], "change_count": 0,
                "reason": ("no_new_positions" if not deferred
                           else "frontier_unchanged")}

    # -- reads --------------------------------------------------------------

    @staticmethod
    def _encode_cursor(release_no, partition, offset, limit):
        raw = json.dumps({"r": release_no, "p": partition, "o": offset,
                          "n": limit}, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii")

    @staticmethod
    def _decode_cursor(cursor):
        try:
            raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
            doc = json.loads(raw.decode("utf-8"))
            return doc["r"], doc["p"], doc["o"], doc["n"]
        except Exception as exc:
            raise MaterializationError(f"invalid pagination cursor: {exc}")

    def _latest_release(self, con):
        row = con.execute(
            "SELECT MAX(release_no) AS m FROM releases").fetchone()
        return row["m"]

    def read_details(self, *, limit=100, cursor=None):
        """Page through the CURRENT release's details.

        A cursor is pinned to the release it was created on; continuing after
        a newer release raises StaleCursorError instead of silently mixing
        two versions.
        """
        self._check_open()
        if limit < 1:
            raise ValueError("limit must be >= 1")
        con = connect_mat(self.mat_path)
        try:
            latest = self._latest_release(con)
            if latest is None:
                return {"release_no": None, "rows": [], "next_cursor": None,
                        "has_more": False}
            if cursor is None:
                rel, p, o = latest, 0, -1
                page_limit = limit
            else:
                rel, p, o, page_limit = self._decode_cursor(cursor)
                if rel != latest:
                    raise StaleCursorError(
                        f"cursor belongs to release {rel}; current release "
                        f"is {latest}. Restart paging on the current release "
                        "instead of concatenating two versions",
                        cursor_release=rel, current_release=latest)
            rows = []
            for r in con.execute(
                    "SELECT * FROM details WHERE "
                    "(event_partition > ?) OR "
                    "(event_partition = ? AND event_offset > ?) "
                    "ORDER BY event_partition, event_offset LIMIT ?",
                    (p, p, o, page_limit)):
                rows.append(self._detail_dict(self._detail_tuple_from_row(r)))
            has_more = len(rows) == page_limit
            next_cursor = None
            if has_more:
                last = rows[-1]["event"]
                next_cursor = self._encode_cursor(
                    latest, last["partition"], last["offset"], page_limit)
            return {"release_no": latest, "rows": rows,
                    "next_cursor": next_cursor, "has_more": has_more}
        finally:
            con.close()

    def read_all_details(self):
        """Convenience: every detail row of the current release."""
        out = []
        cursor = None
        while True:
            page = self.read_details(limit=1000, cursor=cursor)
            if page["release_no"] is None:
                return []
            out.extend(page["rows"])
            if not page["has_more"]:
                return out
            cursor = page["next_cursor"]

    def read_changes(self, release_no=None, *, limit=100, cursor=None):
        """Page through one release's net changes (default: latest release)."""
        self._check_open()
        if limit < 1:
            raise ValueError("limit must be >= 1")
        con = connect_mat(self.mat_path)
        try:
            latest = self._latest_release(con)
            if latest is None:
                return {"release_no": None, "rows": [], "next_cursor": None,
                        "has_more": False}
            rel = latest if release_no is None else release_no
            if con.execute("SELECT 1 FROM releases WHERE release_no=?",
                           (rel,)).fetchone() is None:
                raise UnknownRelease(f"unknown release {rel}")
            if cursor is None:
                p, o, page_limit = 0, -1, limit
            else:
                crel, p, o, page_limit = self._decode_cursor(cursor)
                if crel != rel:
                    raise StaleCursorError(
                        f"cursor belongs to release {crel}; this read is "
                        f"pinned to release {rel}",
                        cursor_release=crel, current_release=rel)
            rows = []
            for r in con.execute(
                    "SELECT * FROM changes WHERE release_no=? AND "
                    "(event_partition > ? OR "
                    "(event_partition = ? AND event_offset > ?)) "
                    "ORDER BY event_partition, event_offset LIMIT ?",
                    (rel, p, p, o, page_limit)):
                rows.append(self._change_row(r))
            has_more = len(rows) == page_limit
            next_cursor = None
            if has_more:
                last = rows[-1]["event"]
                next_cursor = self._encode_cursor(
                    rel, last["partition"], last["offset"], page_limit)
            return {"release_no": rel, "rows": rows,
                    "next_cursor": next_cursor, "has_more": has_more}
        finally:
            con.close()

    def _change_row(self, r):
        def detail(assigned, grp, eff, rev, rpart, roff):
            if assigned is None:
                return None
            t = (r["event_partition"], r["event_offset"], None, None,
                 assigned, grp, eff, rev, rpart, roff)
            d = self._detail_dict(t)
            d.pop("entity")
            d.pop("event_time")
            return d
        return {
            "event": {"partition": r["event_partition"],
                      "offset": r["event_offset"]},
            "change_type": r["change_type"],
            "before": detail(r["before_assigned"], r["before_grp"],
                             r["before_rule_effective_from"],
                             r["before_rule_revision"],
                             r["before_rule_partition"],
                             r["before_rule_offset"]),
            "after": detail(r["after_assigned"], r["after_grp"],
                            r["after_rule_effective_from"],
                            r["after_rule_revision"],
                            r["after_rule_partition"],
                            r["after_rule_offset"]),
        }
