"""API semantics: transactions, dedup identity, sequences, epochs."""

import unittest

import txlog
from tests.helpers import LogTestCase
from txlog.errors import (ContentConflict, ProducerFenced, SequenceGap,
                          SequenceTooOld, TxnStateError, UnknownTransaction)


class TestTxnLifecycle(LogTestCase):
    def test_nontxn_append_immediately_visible(self):
        log = self.make_log()
        off = log.append(0, b"a")
        self.assertEqual(off, 0)
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual([r["offset"] for r in page["records"]], [0])
        self.assertIsNone(page["records"][0]["txn_status"])
        self.assertEqual(page["reason"], "log_end")

    def test_txn_invisible_until_commit(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"x", seq=0)
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual(page["records"], [])
        self.assertEqual(page["reason"], "stable_frontier")
        p.commit()
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual([r["offset"] for r in page["records"]], [0])
        self.assertEqual(page["records"][0]["txn_status"], "committed")

    def test_abort_leaves_hole_offsets_not_reordered(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"dead-0", seq=0)
        p.append(0, b"dead-1", seq=1)
        p.abort()
        p.begin()
        p.append(0, b"live-2", seq=2)
        p.commit()
        page = log.fetch(0, 0, isolation="read_committed")
        # aborted offsets are skipped but positions are preserved
        self.assertEqual([r["offset"] for r in page["records"]], [2])
        self.assertEqual(page["next_offset"], 3)
        # read_uncommitted shows the holes as aborted evidence
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["offset"] for r in page["records"]], [0, 1, 2])
        self.assertEqual([r["txn_status"] for r in page["records"]],
                         ["aborted", "aborted", "committed"])

    def test_cross_partition_commit_is_atomic(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"a0", seq=0)
        p.append(1, b"a1", seq=0)
        p.append(0, b"a2", seq=1)
        for part in (0, 1):
            page = log.fetch(part, 0, isolation="read_committed")
            self.assertEqual(page["records"], [])
        p.commit()
        self.assertEqual([r["offset"] for r in
                          log.fetch(0, 0)["records"]], [0, 1])
        self.assertEqual([r["offset"] for r in
                          log.fetch(1, 0)["records"]], [0])

    def test_begin_is_idempotent_single_txn(self):
        log = self.make_log()
        p = log.producer("p")
        t1 = p.begin()
        t2 = p.begin()
        self.assertEqual(t1, t2)
        self.assertEqual(p.current_txn, t1)

    def test_commit_idempotent_and_confirmable(self):
        log = self.make_log()
        p = log.producer("p")
        t = p.begin()
        p.append(0, b"x", seq=0)
        got = p.commit()
        self.assertEqual(got, t)
        # retry-confirm of an already decided call
        self.assertEqual(p.commit(txn_id=t), t)
        self.assertEqual(log.txn_state(t)["state"], "committed")
        with self.assertRaises(TxnStateError):
            p.abort(txn_id=t)
        with self.assertRaises(TxnStateError):
            p.commit()  # nothing open
        with self.assertRaises(UnknownTransaction):
            p.commit(txn_id=999)

    def test_sequences_continue_across_txns_in_epoch(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"a", seq=0)
        p.commit()
        p.begin()
        p.append(0, b"b", seq=1)   # continuity is per epoch, not per txn
        p.commit()
        with self.assertRaises(SequenceGap):
            p.begin()
            p.append(0, b"c", seq=5)


class TestDedup(LogTestCase):
    DEDUP_WINDOW = 4

    def test_retry_same_content_returns_original_offset(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        off = p.append(0, b"data", seq=0)
        again = p.append(0, b"data", seq=0)
        self.assertEqual(off, again)
        # no second record was written
        self.assertEqual(log.fetch(0, 0, isolation="read_uncommitted")
                         ["high_watermark"], 1)

    def test_lost_receipt_retry_after_meta_crash(self):
        # append's metadata commits, then the receipt is "lost" (injected
        # failure); the retry must confirm the original offset, not rewrite.
        fired = {"n": 0}

        def hook(point):
            if point == "append.after_meta" and fired["n"] == 0:
                fired["n"] += 1
                raise RuntimeError("simulated lost receipt")

        log = self.make_log(fault_hook=hook)
        p = log.producer("p")
        p.begin()
        with self.assertRaises(RuntimeError):
            p.append(0, b"data", seq=0)
        # the append was durable despite the lost receipt
        self.assertEqual(log.fetch(0, 0, isolation="read_uncommitted")
                         ["high_watermark"], 1)
        again = p.append(0, b"data", seq=0)
        self.assertEqual(again, 0)
        self.assertEqual(log.fetch(0, 0, isolation="read_uncommitted")
                         ["high_watermark"], 1)

    def test_content_conflict_rejected(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"data", seq=0)
        with self.assertRaises(ContentConflict):
            p.append(0, b"different", seq=0)

    def test_sequence_gap_rejected(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        with self.assertRaises(SequenceGap):
            p.append(0, b"x", seq=3)
        p.append(0, b"x", seq=0)
        with self.assertRaises(SequenceGap):
            p.append(0, b"y", seq=2)

    def test_sequence_window_lower_bound_is_contract(self):
        log = self.make_log()
        p = log.producer("p")
        for i in range(10):
            p.begin()
            p.append(0, f"rec-{i}".encode(), seq=i)
            p.commit()
        # window is 4: min_retained_seq = 10 - 4 = 6
        info = log.inspect(partition=0)["partitions"][0]
        win = info["sequences"][0]
        self.assertEqual(win["min_retained_seq"], 6)
        self.assertEqual(win["next_seq"], 10)
        p.begin()
        # outside the window: explicitly rejected, never re-appended
        with self.assertRaises(SequenceTooOld):
            p.append(0, b"rec-5", seq=5)
        # inside the window: deduped to the original offset
        self.assertEqual(p.append(0, b"rec-7", seq=7), 7)
        with self.assertRaises(ContentConflict):
            p.append(0, b"forged", seq=7)
        # next new sequence works
        self.assertEqual(p.append(0, b"rec-10", seq=10), 10)
        self.assertEqual(log.fetch(0, 0, isolation="read_uncommitted")
                         ["high_watermark"], 11)


class TestEpochFencing(LogTestCase):
    def test_reopen_fences_old_epoch_and_aborts_pending(self):
        log = self.make_log()
        p1 = log.producer("p")
        self.assertEqual(p1.epoch, 1)
        p1.begin()
        p1.append(0, b"pending", seq=0)
        t_pending = p1.current_txn
        p1.begin()
        # reopen the same producer id: epoch bumps, old epoch is dead
        p2 = log.producer("p")
        self.assertEqual(p2.epoch, 2)
        with self.assertRaises(ProducerFenced):
            p1.append(0, b"x", seq=1)
        with self.assertRaises(ProducerFenced):
            p1.commit()
        with self.assertRaises(ProducerFenced):
            p1.abort()
        with self.assertRaises(ProducerFenced):
            p1.begin()
        # the leftover pending txn was aborted by the persistent fencing flow
        st = log.txn_state(t_pending)
        self.assertEqual(st["state"], "aborted")
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual(page["records"][0]["txn_status"], "aborted")
        # new epoch: sequences restart from 0
        p2.begin()
        self.assertEqual(p2.append(0, b"new-epoch", seq=0), 1)
        p2.commit()
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual([r["offset"] for r in page["records"]], [1])

    def test_committed_data_of_old_epoch_survives_fencing(self):
        log = self.make_log()
        p1 = log.producer("p")
        p1.begin()
        p1.append(0, b"committed", seq=0)
        p1.commit()
        p2 = log.producer("p")
        self.assertEqual(p2.epoch, 2)
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual([r["payload"] for r in page["records"]],
                         [b"committed"])

    def test_old_epoch_dedup_identity_cannot_pin_retention(self):
        log = self.make_log()
        p1 = log.producer("p")
        for i in range(3):
            p1.begin()
            p1.append(0, f"e1-{i}".encode(), seq=i)
            p1.commit()
        log.producer("p")  # epoch 2; old dedup identity is dropped
        res = log.retain(0, 100)
        # nothing may constrain retention to the dead epoch's retry identity
        self.assertEqual(res["constrained_by"], "high_watermark")
        self.assertEqual(res["applied"], 3)


class TestMultiPartitionTxn(LogTestCase):
    def test_many_partitions_one_txn(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        for part in range(5):
            for i in range(3):
                p.append(part, f"p{part}-{i}".encode(), seq=i)
        p.commit()
        for part in range(5):
            page = log.fetch(part, 0)
            self.assertEqual(len(page["records"]), 3)
            self.assertEqual(page["reason"], "log_end")


if __name__ == "__main__":
    unittest.main()
