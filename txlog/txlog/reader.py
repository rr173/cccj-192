"""Named, resumable application readers.

A *reader* is a durable, named read position over the log with explicit
per-record acknowledgement.  Unlike a bare ``fetch`` — whose position the
caller must save itself — a reader survives process restarts and never
confuses "highest offset seen" with "highest offset safely processed".

Durable state (SQLite, metadata only — payloads stay in segment files):

* ``read_offset``  — every read_committed-visible record below this offset
  has been *delivered* at least once by a ``poll`` of this reader.
* ``reader_acks``  — the acknowledgement *set*: one row per acked
  (reader, partition, offset).  There is deliberately no "max offset".
* ``safe_offset``  — the contiguous-acknowledgement cursor.  Every
  delivered record in ``[start, safe_offset)`` is acked; abort holes are
  skipped.  An unacked record before later acks blocks advancement, so
  out-of-order acks can never jump the cursor over a gap.

Guarantees:

* ``poll`` always uses read_committed semantics and never crosses the
  partition's stable frontier; the returned records carry the reader id,
  partition and offset needed to ack them.
* ``ack`` is idempotent; unknown readers, wrong partitions, undelivered
  positions, abort holes and positions beyond the stable frontier are
  rejected with a classified ``ReaderAckError``.
* A record that was delivered but not acked is delivered again after the
  reader handle is closed and reopened (at-least-once).
* Retention may not cross any live reader's ``safe_offset``; deleting a
  reader is the explicit operation that releases that hold.
* ``replay`` only moves the in-memory poll position: it never erases acks
  and can never move the safe cursor backwards.
"""

import time

from .errors import (ReaderAckError, ReaderConflict, ReaderDataUnavailable,
                     UnknownReader)
from .log import ISOLATION_READ_COMMITTED, _as_offset, _as_partition


class Reader:
    """A handle onto one durable named reader.

    Handles are cheap and per-process: the durable state lives in SQLite.
    Two handles for the same reader_id are independent *pollers* over the
    same durable progress (competing consumers); every redelivery is
    at-least-once and acks converge on the same contiguous cursor.
    """

    def __init__(self, log, reader_id, reused=False):
        self.log = log
        self.reader_id = reader_id
        self.reused = reused
        # Per-handle poll position.  Initialised lazily from the durable
        # read_offset on first touch of a partition; never persisted here
        # (the DB row is the durable truth, advanced inside poll's txn).
        self._poll_pos = {}
        self._closed = False

    # -- helpers -----------------------------------------------------------

    def _check(self):
        if self._closed:
            raise UnknownReader(
                f"reader {self.reader_id!r} handle is closed (the reader "
                f"was deleted or this handle was closed)")
        self.log._check_writable()

    def _reader_row(self, con):
        row = con.execute(
            "SELECT * FROM readers WHERE reader_id=?",
            (self.reader_id,)).fetchone()
        if row is None:
            raise UnknownReader(f"unknown reader {self.reader_id!r}")
        return row

    def _aborted_ranges(self, con, partition, low, high):
        """Offset ranges of aborted transactions on this partition whose
        holes intersect [low, high)."""
        return [(r["first_offset"], r["last_offset"]) for r in con.execute(
            "SELECT tp.first_offset, tp.last_offset FROM txn_parts tp "
            "JOIN transactions t ON t.txn_id = tp.txn_id "
            "WHERE tp.partition=? AND t.state='aborted' "
            "AND tp.last_offset>=? AND tp.first_offset<?",
            (partition, low, high))]

    @staticmethod
    def _in_ranges(offset, ranges):
        return any(lo <= offset <= hi for lo, hi in ranges)

    # -- poll --------------------------------------------------------------

    def poll(self, partition, max_records=100, max_scan=None, offset=None):
        """Read one bounded page of committed records from ``partition``.

        Without ``offset`` the page continues from this handle's poll
        position, which starts at the durable **safe cursor** when the
        handle is opened — so records delivered but not acked before a
        crash are redelivered after reopen.  An explicit ``offset`` only
        moves this handle's poll position for the call (it does not change
        the durable reader progress, acks or the safe cursor).

        Returns the normal fetch page plus reader identity and the durable
        ``read_offset`` / ``safe_offset`` after the page.  Raises
        ReaderDataUnavailable if the position is below the retention
        frontier — never an empty page masquerading as the log end.
        """
        self._check()
        partition = _as_partition(partition)
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        if max_scan is None:
            max_scan = max(64, max_records * 8)
        if max_scan < 1:
            raise ValueError("max_scan must be >= 1")
        if offset is not None:
            offset = _as_offset(offset)
        with self.log._retain_shared():
            con = self.log._connect()
            try:
                con.execute("BEGIN IMMEDIATE")
                try:
                    rdr = self._reader_row(con)
                    prow = con.execute(
                        "SELECT * FROM partitions WHERE partition=?",
                        (partition,)).fetchone()
                    hw = prow["next_offset"] if prow else 0
                    retained = prow["retained_below"] if prow else 0
                    pos_row = con.execute(
                        "SELECT * FROM reader_positions WHERE reader_id=? "
                        "AND partition=?",
                        (self.reader_id, partition)).fetchone()
                    if pos_row is None:
                        if rdr["start_mode"] == "boundary":
                            start = retained
                        else:
                            start = rdr["start_offset"]
                            if start < retained:
                                raise ReaderDataUnavailable(
                                    self.reader_id, partition, start,
                                    retained)
                        con.execute(
                            "INSERT INTO reader_positions VALUES (?,?,?,?)",
                            (self.reader_id, partition, start, start))
                        durable_read = start
                    else:
                        durable_read = pos_row["read_offset"]
                        durable_safe = pos_row["safe_offset"]
                    pos = self._poll_pos.get(
                        partition,
                        durable_safe if pos_row is not None else durable_read)
                    if offset is not None:
                        if offset < retained:
                            raise ReaderDataUnavailable(
                                self.reader_id, partition, offset, retained)
                        pos = offset
                    lso = self.log._lso(con, partition, hw)
                    segs = [dict(s) for s in con.execute(
                        "SELECT * FROM segments WHERE partition=? AND "
                        "state != 'deleted' ORDER BY base_offset",
                        (partition,))]
                    txn_states = {}
                    for r in con.execute(
                            "SELECT DISTINCT t.txn_id, t.state "
                            "FROM transactions t "
                            "JOIN txn_parts tp ON tp.txn_id = t.txn_id "
                            "WHERE tp.partition=? AND tp.last_offset>=? "
                            "AND tp.first_offset<?",
                            (partition, retained, max(hw, 1))):
                        txn_states[r["txn_id"]] = r["state"]
                    page = self.log._scan(
                        partition, pos, max_records,
                        ISOLATION_READ_COMMITTED, max_scan, hw, retained,
                        lso, segs, txn_states)
                    scanned_to = page["next_offset"]
                    # Only a continuation poll advances the durable
                    # delivery high-water.  An explicit offset= poll is a
                    # handle-local repositioning (diagnostics/replay): it
                    # must never mark skipped positions as delivered, or
                    # they could be acked without ever being seen.
                    if offset is None and scanned_to > durable_read:
                        con.execute(
                            "UPDATE reader_positions SET read_offset=? "
                            "WHERE reader_id=? AND partition=?",
                            (scanned_to, self.reader_id, partition))
                    # re-read the durable truth: another handle on the same
                    # reader may have advanced it concurrently, so the
                    # reported high-water must never be this handle's local
                    # (possibly stale or smaller) scan position.
                    final_row = con.execute(
                        "SELECT read_offset, safe_offset FROM "
                        "reader_positions WHERE reader_id=? AND partition=?",
                        (self.reader_id, partition)).fetchone()
                    new_read = final_row["read_offset"]
                    safe = final_row["safe_offset"]
                    con.execute("COMMIT")
                except BaseException:
                    con.execute("ROLLBACK")
                    raise
                if offset is None:
                    self._poll_pos[partition] = new_read
                # explicit polls leave the handle's continuation position
                # untouched: the next plain poll resumes durable progress.
                self.log._fault.maybe("reader.poll.after_meta")
            finally:
                con.close()
        page["reader_id"] = self.reader_id
        page["read_offset"] = new_read
        page["safe_offset"] = safe
        for rec in page["records"]:
            rec["reader_id"] = self.reader_id
            rec["partition"] = partition
        return page

    # -- ack ---------------------------------------------------------------

    def ack(self, partition, offset):
        """Acknowledge one delivered record.

        Idempotent: re-acking an already-contiguous position returns
        ``duplicate=True`` without changing anything.  Rejections:

        * unknown reader / closed handle      -> UnknownReader
        * partition never opened by reader    -> ReaderAckError(wrong_partition)
        * offset never delivered              -> ReaderAckError(not_delivered)
        * offset past the stable frontier     -> ReaderAckError(beyond_stable)
        * offset is an abort hole             -> ReaderAckError(abort_hole)

        After inserting the ack the contiguous safe cursor is recomputed
        from the ack *set* (abort holes skipped); it stops at the first
        unacked delivered record, so the highest acked offset can never be
        mistaken for the safe cursor.
        """
        self._check()
        partition = _as_partition(partition)
        offset = _as_offset(offset)
        con = self.log._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                self._reader_row(con)
                pos = con.execute(
                    "SELECT * FROM reader_positions WHERE reader_id=? "
                    "AND partition=?",
                    (self.reader_id, partition)).fetchone()
                if pos is None:
                    raise ReaderAckError(
                        "wrong_partition",
                        f"reader {self.reader_id!r} has no position for "
                        f"partition {partition}; poll it before acking")
                safe = pos["safe_offset"]
                read = pos["read_offset"]
                prow = con.execute(
                    "SELECT * FROM partitions WHERE partition=?",
                    (partition,)).fetchone()
                hw = prow["next_offset"] if prow else 0
                retained = prow["retained_below"] if prow else 0
                # Already folded into the contiguous cursor: the durable
                # effect happened earlier, so a retried ack after a lost
                # receipt is an idempotent success.
                if offset < safe:
                    con.execute("COMMIT")
                    return {"reader_id": self.reader_id,
                            "partition": partition, "offset": offset,
                            "duplicate": True, "safe_offset": safe,
                            "read_offset": read}
                if offset >= read:
                    lso = self.log._lso(con, partition, hw)
                    if offset >= min(lso, hw):
                        reason = "beyond_stable"
                        detail = (f"offset {offset} is beyond the stable "
                                  f"readable frontier {min(lso, hw)}")
                    else:
                        reason = "not_delivered"
                        detail = (f"offset {offset} was never delivered to "
                                  f"reader {self.reader_id!r} (read_offset="
                                  f"{read})")
                    raise ReaderAckError(reason, detail)
                if self._in_ranges(
                        offset,
                        self._aborted_ranges(con, partition, retained, read)):
                    raise ReaderAckError(
                        "abort_hole",
                        f"offset {offset} on partition {partition} is an "
                        f"aborted-transaction hole: no record was ever "
                        f"delivered there")
                existing = con.execute(
                    "SELECT 1 FROM reader_acks WHERE reader_id=? AND "
                    "partition=? AND offset=?",
                    (self.reader_id, partition, offset)).fetchone()
                duplicate = existing is not None
                if not duplicate:
                    con.execute(
                        "INSERT INTO reader_acks VALUES (?,?,?)",
                        (self.reader_id, partition, offset))
                new_safe, pruned = self._advance(con, partition, safe, read,
                                                  retained)
                if pruned:
                    con.executemany(
                        "DELETE FROM reader_acks WHERE reader_id=? AND "
                        "partition=? AND offset=?",
                        [(self.reader_id, partition, o) for o in pruned])
                if new_safe != safe:
                    con.execute(
                        "UPDATE reader_positions SET safe_offset=? "
                        "WHERE reader_id=? AND partition=?",
                        (new_safe, self.reader_id, partition))
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
            self.log._fault.maybe("reader.ack.after_meta")
            return {"reader_id": self.reader_id, "partition": partition,
                    "offset": offset, "duplicate": duplicate,
                    "safe_offset": new_safe, "read_offset": read}
        finally:
            con.close()

    def _advance(self, con, partition, safe, read, retained):
        """Walk the contiguous frontier from ``safe`` using the ack set and
        the abort-hole map.  Returns (new_safe, ack_offsets_now_below_it)."""
        acks = {r["offset"] for r in con.execute(
            "SELECT offset FROM reader_acks WHERE reader_id=? AND "
            "partition=? AND offset>=? AND offset<?",
            (self.reader_id, partition, safe, read))}
        holes = self._aborted_ranges(con, partition, retained, read)
        pos = safe
        pruned = []
        while pos < read:
            if pos in acks:
                acks.remove(pos)
                pruned.append(pos)
                pos += 1
            elif self._in_ranges(pos, holes):
                # abort hole: nothing was delivered here, offset semantics
                # are preserved but it cannot block the safe cursor.
                pos += 1
            else:
                break
        return pos, pruned

    # -- replay -------------------------------------------------------------

    def replay(self, partition, offset=None):
        """Re-open delivery at an explicitly retained position.

        Only this handle's poll position changes: durable read_offset, the
        ack set and safe_offset are untouched, so replay never erases
        confirmations and old acks can never pull the new progress
        backwards.  ``offset=None`` replays from the reader's creation
        start (boundary readers: the current retention frontier).
        """
        self._check()
        partition = _as_partition(partition)
        if offset is not None:
            offset = _as_offset(offset)
        con = self.log._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                rdr = self._reader_row(con)
                prow = con.execute(
                    "SELECT * FROM partitions WHERE partition=?",
                    (partition,)).fetchone()
                retained = prow["retained_below"] if prow else 0
                if offset is None:
                    offset = (retained if rdr["start_mode"] == "boundary"
                              else rdr["start_offset"])
                if offset < retained:
                    raise ReaderDataUnavailable(
                        self.reader_id, partition, offset, retained)
                pos_row = con.execute(
                    "SELECT * FROM reader_positions WHERE reader_id=? "
                    "AND partition=?",
                    (self.reader_id, partition)).fetchone()
                if pos_row is None:
                    # First touch of this partition.  An offset-mode reader
                    # whose configured start is already unreachable can be
                    # explicitly recovered here: the row is materialized at
                    # the validated replay position, since nothing below
                    # the frontier can ever be delivered or acked anyway.
                    if rdr["start_mode"] == "boundary":
                        row_start = retained
                    elif rdr["start_offset"] >= retained:
                        row_start = rdr["start_offset"]
                    else:
                        row_start = offset
                    con.execute(
                        "INSERT INTO reader_positions VALUES (?,?,?,?)",
                        (self.reader_id, partition, row_start, row_start))
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
            self._poll_pos[partition] = offset
        finally:
            con.close()
        st = self.status(partition)
        st["replay_from_offset"] = offset
        return st

    # -- diagnostics / lifecycle -------------------------------------------

    def status(self, partition=None):
        """Durable progress: per partition the safe cursor, poll high-water
        and the out-of-order acked offsets blocking the gap.  Read-only."""
        self._check()
        if partition is not None:
            partition = _as_partition(partition)
        con = self.log._connect()
        try:
            rdr = self._reader_row(con)
            out = {"reader_id": self.reader_id,
                   "start_mode": rdr["start_mode"],
                   "start_offset": rdr["start_offset"],
                   "partitions": {}}
            q = ("SELECT * FROM reader_positions WHERE reader_id=?")
            args = [self.reader_id]
            if partition is not None:
                q += " AND partition=?"
                args.append(partition)
            for r in con.execute(q, args):
                p = r["partition"]
                gaps = [x["offset"] for x in con.execute(
                    "SELECT offset FROM reader_acks WHERE reader_id=? AND "
                    "partition=? AND offset>=? ORDER BY offset",
                    (self.reader_id, p, r["safe_offset"]))]
                out["partitions"][p] = {
                    "read_offset": r["read_offset"],
                    "safe_offset": r["safe_offset"],
                    "blocking_gaps": gaps}
            return out
        finally:
            con.close()

    def close(self):
        """Release this handle.  Durable reader progress is unaffected."""
        self._closed = True
        self._poll_pos.clear()

    def delete(self):
        """Permanently delete this reader and all of its progress.  See
        Log.delete_reader for the retention-effect report."""
        return self.log.delete_reader(self.reader_id, handle=self)

    def __repr__(self):
        return f"Reader({self.reader_id!r}, reused={self.reused})"
