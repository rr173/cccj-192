"""Acceptance tests for named, resumable application readers.

Covers the full contract:

* two readers with different starts progress independently;
* out-of-order acks cannot move the safe cursor over a gap (ack 2, then 0
  leaves the cursor at 0 until 1 is acked); abort holes never block it;
* an unacked failed record is redelivered after the handle is reopened;
* duplicate / illegal acks and stale reader identities never move cursors;
* a slow reader blocks retain past its safe position; deleting it releases
  the hold and the boundary can advance;
* a cursor overtaken by a legal retain reports an explicit unrecoverable
  boundary, never an empty page or a silent jump;
* replay cannot erase acks or move the safe cursor backwards;
* all progress survives a full process-close/reopen.
"""

import os
import unittest

import txlog
from tests.helpers import LogTestCase
from txlog.errors import (ReaderAckError, ReaderConflict,
                          ReaderDataUnavailable, UnknownReader)


class TestReaderLifecycle(LogTestCase):
    def _fill(self, log, n, partition=0):
        for i in range(n):
            log.append(partition, f"rec-{i}".encode())

    def test_create_reuse_and_conflict_never_resets(self):
        log = self.make_log()
        self._fill(log, 4)
        r = log.create_reader("r", start=0)
        self.assertFalse(r.reused)
        r.poll(0, max_records=4)
        r.ack(0, 0)
        self.assertEqual(r.status(0)["partitions"][0]["safe_offset"], 1)
        # identical create is an idempotent reuse, progress untouched
        again = log.create_reader("r", start=0)
        self.assertTrue(again.reused)
        self.assertEqual(again.status(0)["partitions"][0]["safe_offset"], 1)
        # different configuration is a conflict, not a silent reset
        with self.assertRaises(ReaderConflict):
            log.create_reader("r", start=3)
        with self.assertRaises(ReaderConflict):
            log.create_reader("r")
        # progress is still exactly where it was
        self.assertEqual(log.open_reader("r").status(0)["partitions"][0]
                         ["safe_offset"], 1)
        # boundary-mode create works independently
        b = log.create_reader("tail")
        pg = b.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in pg["records"]],
                         list(range(4)))

    def test_unknown_reader_and_stale_handle_rejected(self):
        log = self.make_log()
        with self.assertRaises(UnknownReader):
            log.open_reader("ghost")
        r = log.create_reader("r")
        self._fill(log, 2)
        r.poll(0)
        log.delete_reader("r")
        with self.assertRaises(UnknownReader):
            log.open_reader("r")
        with self.assertRaises(UnknownReader):
            r.poll(0)
        with self.assertRaises(UnknownReader):
            r.ack(0, 0)
        with self.assertRaises(UnknownReader):
            r.status(0)

    def test_poll_is_read_committed_and_identifies_records(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"open", seq=0)          # offset 0, pending
        log.append(0, b"plain")              # offset 1, visible
        p2 = log.producer("q")
        p2.begin()
        p2.append(0, b"dead", seq=0)         # offset 2 -> aborted hole
        p2.abort()
        r = log.create_reader("r", start=0)
        pg = r.poll(0, max_records=10)
        # pending txn pins the stable frontier at offset 0: nothing visible
        self.assertEqual(pg["records"], [])
        self.assertEqual(pg["reason"], "stable_frontier")
        self.assertEqual(pg["reader_id"], "r")
        p.commit()
        pg = r.poll(0, max_records=10)
        # committed offset 0, plain offset 1; aborted offset 2 is a hole
        self.assertEqual([x["offset"] for x in pg["records"]], [0, 1])
        self.assertTrue(all(x["reader_id"] == "r"
                            for x in pg["records"]))
        self.assertEqual(pg["next_offset"], 3)
        for rec in pg["records"]:
            self.assertIn("partition", rec)
            self.assertEqual(rec["partition"], 0)


class TestSafeCursor(LogTestCase):
    def test_out_of_order_acks_gap_blocks_cursor(self):
        log = self.make_log()
        for i in range(5):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        r.poll(0, max_records=10)
        # deliver offsets 0..4; ack 2 first: cursor must stay at 0
        r.ack(0, 2)
        st = r.status(0)["partitions"][0]
        self.assertEqual(st["safe_offset"], 0)
        self.assertEqual(st["blocking_gaps"], [2])
        # requirement's exact sequence: ack 2 then ack 0 -> still stopped 0
        r.ack(0, 0)
        st = r.status(0)["partitions"][0]
        self.assertEqual(st["safe_offset"], 1)
        self.assertEqual(st["blocking_gaps"], [2])
        # fill the missing 1: contiguous advance jumps over 2 to 3
        r.ack(0, 1)
        st = r.status(0)["partitions"][0]
        self.assertEqual(st["safe_offset"], 3)
        self.assertEqual(st["blocking_gaps"], [])
        # 3 and 4 still unacked: 3 now blocks
        r.ack(0, 4)
        st = r.status(0)["partitions"][0]
        self.assertEqual(st["safe_offset"], 3)
        self.assertEqual(st["blocking_gaps"], [4])
        r.ack(0, 3)
        st = r.status(0)["partitions"][0]
        self.assertEqual(st["safe_offset"], 5)
        self.assertEqual(st["blocking_gaps"], [])

    def test_abort_hole_does_not_block_safe_cursor(self):
        log = self.make_log()
        p = log.producer("p")
        log.append(0, b"r0")                 # 0
        p.begin()
        p.append(0, b"hole-1", seq=0)
        p.append(0, b"hole-2", seq=1)
        p.abort()                            # offsets 1,2 = abort holes
        log.append(0, b"r3")                 # 3
        log.append(0, b"r4")                 # 4
        r = log.create_reader("r", start=0)
        pg = r.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in pg["records"]], [0, 3, 4])
        # acking the hole is rejected
        with self.assertRaises(ReaderAckError) as cm:
            r.ack(0, 1)
        self.assertEqual(cm.exception.reason, "abort_hole")
        # ack every DELIVERED record: cursor reaches the end, skipping 1,2
        for off in (0, 3, 4):
            r.ack(0, off)
        st = r.status(0)["partitions"][0]
        self.assertEqual(st["safe_offset"], 5)
        self.assertEqual(st["blocking_gaps"], [])
        # and retention is free to delete up to the safe cursor despite
        # the unacked (un-deliverable) holes.  The producer's dedup
        # identity still pins its retry window; bumping the epoch releases
        # it, leaving the reader cursor as the only reader constraint.
        log.producer("p")
        res = log.retain(0, 100)
        self.assertEqual(res["reader_floor"], 5)
        self.assertEqual(res["applied"], 5)

    def test_duplicate_ack_idempotent_and_illegal_acks_rejected(self):
        log = self.make_log()
        for i in range(4):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        # deliver only the first two; offsets 2,3 stay below the stable
        # frontier but were never delivered by this reader
        r.poll(0, max_records=2)
        r.ack(0, 0)
        again = r.ack(0, 0)
        self.assertTrue(again["duplicate"])
        self.assertEqual(again["safe_offset"], 1)
        # stable, readable, but never delivered by this reader
        with self.assertRaises(ReaderAckError) as cm:
            r.ack(0, 3)
        self.assertEqual(cm.exception.reason, "not_delivered")
        # wrong partition the reader never opened
        with self.assertRaises(ReaderAckError) as cm:
            r.ack(7, 0)
        self.assertEqual(cm.exception.reason, "wrong_partition")
        # beyond the stable frontier: open txn at the tail
        p = log.producer("p")
        p.begin()
        p.append(0, b"pending", seq=0)     # offset 4, open
        with self.assertRaises(ReaderAckError) as cm:
            r.ack(0, 4)
        self.assertEqual(cm.exception.reason, "beyond_stable")

    def test_highest_acked_offset_is_never_the_cursor(self):
        log = self.make_log()
        for i in range(6):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        r.poll(0, max_records=10)
        r.ack(0, 5)
        r.ack(0, 3)
        # high offsets acked, but 0..2 unacked: cursor cannot move at all
        self.assertEqual(r.status(0)["partitions"][0]["safe_offset"], 0)
        res = log.retain(0, 100)
        self.assertEqual(res["reader_floor"], 0)
        self.assertEqual(res["applied"], 0)


class TestRedeliveryAndReplay(LogTestCase):
    def test_unacked_record_redelivered_after_reopen(self):
        log = self.make_log()
        for i in range(6):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        pg = r.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in pg["records"]],
                         list(range(6)))
        r.ack(0, 0)
        r.ack(0, 1)
        # process dies after processing 2..5 but before acking them:
        # close and reopen the whole log
        log2 = self.reopen(log)
        r2 = log2.open_reader("r")
        pg = r2.poll(0, max_records=10)
        # resume at the safe cursor; 2..5 are delivered again
        self.assertEqual([x["offset"] for x in pg["records"]],
                         [2, 3, 4, 5])
        # acking 0 again is still the idempotent duplicate path
        self.assertTrue(r2.ack(0, 0)["duplicate"])
        # and progress remains durable
        self.assertEqual(r2.status(0)["partitions"][0]["safe_offset"], 2)

    def test_replay_keeps_acks_and_never_regresses(self):
        log = self.make_log()
        for i in range(8):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        r.poll(0, max_records=10)
        for off in range(4):
            r.ack(0, off)
        self.assertEqual(r.status(0)["partitions"][0]["safe_offset"], 4)
        # replay from an earlier retained position
        st = r.replay(0, 2)
        self.assertEqual(st["replay_from_offset"], 2)
        pg = r.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in pg["records"]],
                         list(range(2, 8)))
        # durable acks and safe cursor are untouched by replay
        self.assertEqual(r.status(0)["partitions"][0]["safe_offset"], 4)
        # old ack cannot pull the new progress backwards
        out = r.ack(0, 2)
        self.assertTrue(out["duplicate"])
        self.assertEqual(out["safe_offset"], 4)
        # acking freshly redelivered records advances normally
        for off in range(4, 8):
            r.ack(0, off)
        self.assertEqual(r.status(0)["partitions"][0]["safe_offset"], 8)

    def test_explicit_poll_offset_does_not_skip_delivery(self):
        log = self.make_log()
        for i in range(6):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        # peek ahead with an explicit offset: must NOT mark 0..3 delivered
        r.poll(0, max_records=2, offset=4)
        with self.assertRaises(ReaderAckError) as cm:
            r.ack(0, 4)
        self.assertEqual(cm.exception.reason, "not_delivered")
        # normal continuation still starts from the durable position
        pg = r.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in pg["records"]],
                         list(range(6)))


class TestMultipleReaders(LogTestCase):
    def test_two_readers_different_starts_independent_progress(self):
        log = self.make_log()
        for i in range(10):
            log.append(0, f"r{i}".encode())
        early = log.create_reader("early", start=0)
        late = log.create_reader("late", start=6)
        epg = early.poll(0, max_records=10)
        lpg = late.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in epg["records"]],
                         list(range(10)))
        self.assertEqual([x["offset"] for x in lpg["records"]],
                         [6, 7, 8, 9])
        # late reader acks everything it got; early reader only a few
        for off in range(6, 10):
            late.ack(0, off)
        early.ack(0, 0)
        early.ack(0, 1)
        self.assertEqual(late.status(0)["partitions"][0]["safe_offset"], 10)
        self.assertEqual(early.status(0)["partitions"][0]["safe_offset"], 2)
        # independent readers on the same partition do not interfere
        res = log.retain(0, 100)
        self.assertEqual(res["constrained_by"], "reader")
        self.assertEqual(res["reader_floor"], 2)

    def test_readers_on_different_partitions_are_independent(self):
        log = self.make_log()
        for i in range(5):
            log.append(0, f"p0-{i}".encode())
            log.append(1, f"p1-{i}".encode())
        a = log.create_reader("a", start=0)
        b = log.create_reader("b", start=0)
        a.poll(0, max_records=10)
        b.poll(1, max_records=10)
        for off in range(5):
            a.ack(0, off)
            b.ack(1, off)
        self.assertEqual(a.status()["partitions"][0]["safe_offset"], 5)
        self.assertEqual(b.status()["partitions"][1]["safe_offset"], 5)


class TestReaderRetention(LogTestCase):
    SEGMENT_BYTES = 200

    def test_slow_reader_blocks_retain_then_delete_releases(self):
        log = self.make_log()
        for i in range(20):
            log.append(0, f"rec-{i}".encode())
        slow = log.create_reader("slow", start=0)
        fast = log.create_reader("fast", start=0)
        slow.poll(0, max_records=20)
        fast.poll(0, max_records=20)
        for off in range(0, 4):
            slow.ack(0, off)
        for off in range(0, 14):
            fast.ack(0, off)
        res = log.retain(0, 100)
        # the slow reader's safe cursor (4) is the binding constraint
        self.assertEqual(res["constrained_by"], "reader")
        self.assertEqual(res["reader_floor"], 4)
        self.assertEqual(res["applied"], 4)
        holds = {h["reader_id"]: h["safe_offset"]
                 for h in res["reader_holds"]}
        self.assertEqual(holds, {"slow": 4, "fast": 14})
        # segments below 4 may be deleted; nothing at/above 4 is touched
        for s in res["deleted_segments"]:
            self.assertFalse(os.path.exists(
                os.path.join(log.path, s["path"])))
        # deleting the slow reader releases the hold
        report = log.delete_reader("slow")
        self.assertEqual(report["former_positions"][0]["safe_offset"], 4)
        res = log.retain(0, 100)
        self.assertEqual(res["applied"], 14)
        self.assertEqual(res["reader_floor"], 14)

    def test_cursor_overtaken_by_retain_reports_boundary(self):
        log = self.make_log()
        for i in range(20):
            log.append(0, f"rec-{i}".encode())
        # no readers: retain advances the frontier freely
        log.retain(0, 12)
        r = log.create_reader("late", start=0)
        # first poll hits the unrecoverable boundary explicitly
        with self.assertRaises(ReaderDataUnavailable) as cm:
            r.poll(0)
        self.assertEqual(cm.exception.retained_below, 12)
        self.assertEqual(cm.exception.requested_offset, 0)
        # it is NOT an empty page / permanent end: explicit resume works
        r.replay(0, 12)
        pg = r.poll(0, max_records=3)
        self.assertEqual([x["offset"] for x in pg["records"]], [12, 13, 14])
        # a boundary-mode reader created afterwards starts at the frontier
        tail = log.create_reader("tail")
        pg = tail.poll(0, max_records=3)
        self.assertEqual([x["offset"] for x in pg["records"]], [12, 13, 14])

    def test_reader_hold_visible_in_inspect(self):
        log = self.make_log()
        for i in range(6):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        r.poll(0)
        r.ack(0, 0)
        info = log.inspect(partition=0)
        self.assertEqual(info["partitions"][0]["reader_floor"], 1)
        self.assertEqual(info["readers"]["r"]["partitions"][0]
                         ["safe_offset"], 1)
        all_info = log.inspect()
        self.assertIn("r", all_info["readers"])


class TestReaderPersistence(LogTestCase):
    def test_full_progress_survives_reopen_with_gaps(self):
        log = self.make_log()
        for i in range(8):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("r", start=0)
        r.poll(0, max_records=10)
        r.ack(0, 0)
        r.ack(0, 2)      # gap: 1 unacked
        r.ack(0, 5)
        log2 = self.reopen(log)
        readers = log2.list_readers()
        pos = readers["r"]["partitions"][0]
        self.assertEqual(pos["safe_offset"], 1)
        self.assertEqual(pos["blocking_gaps"], [2, 5])
        r2 = log2.open_reader("r")
        # unacked 1..7 redelivered (0 already contiguous)
        pg = r2.poll(0, max_records=10)
        self.assertEqual([x["offset"] for x in pg["records"]],
                         list(range(1, 8)))
        r2.ack(0, 1)
        self.assertEqual(r2.status(0)["partitions"][0]["safe_offset"], 3)


if __name__ == "__main__":
    unittest.main()
