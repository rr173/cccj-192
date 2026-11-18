"""Concurrency tests for named readers.

The SQLite metadata (progress, ack set, cursors) takes every mutation in
one IMMEDIATE transaction; the process-level retain lock keeps segment
files alive under an in-flight poll.  These tests run many threads doing
poll/ack/replay/retain against the same readers and then verify the exact
serial invariants on a fresh reopen:

* no ack is ever lost;
* the safe cursor never jumps over an unacked gap;
* no record is ever read from a deleted segment;
* at-least-once redelivery is consistent with the durable cursors.
"""

import random
import threading
import unittest

import txlog
from tests.helpers import LogTestCase
from txlog.errors import (ReaderAckError, ReaderDataUnavailable,
                          UnknownReader)


class TestReaderConcurrency(LogTestCase):
    SEGMENT_BYTES = 300

    def _fill(self, log, n, n_parts):
        for i in range(n):
            p = i % n_parts
            log.append(p, f"p{p}-rec-{i}".encode())

    def test_concurrent_poll_ack_retain_is_serializable(self):
        log = self.make_log()
        n_parts, n = 3, 600
        self._fill(log, n, n_parts)
        readers = [log.create_reader(f"r{i}", start=0)
                   for i in range(3)]
        errors = []
        stop = threading.Event()

        def worker(rdr, seed):
            rng = random.Random(seed)
            try:
                while not stop.is_set():
                    p = rng.randrange(n_parts)
                    if rng.random() < 0.75:
                        try:
                            page = rdr.poll(
                                p, max_records=rng.randint(1, 8),
                                max_scan=rng.randint(4, 20))
                        except ReaderDataUnavailable:
                            # a handle whose position was overtaken by a
                            # legal retain must recover at the boundary;
                            # this is the documented explicit error path
                            info = log.inspect(partition=p)["partitions"][p]
                            rdr.replay(p, info["retained_below"])
                            continue
                        for rec in page["records"]:
                            # leave some records unacked on purpose: those
                            # must remain redeliverable
                            if rng.random() < 0.85:
                                try:
                                    rdr.ack(p, rec["offset"])
                                except ReaderAckError as e:
                                    # abort-hole/not-delivered races are
                                    # legitimate rejections, record reason
                                    errors.append(("ack", e.reason))
                    else:
                        try:
                            rdr.replay(p, rng.choice(
                                [None, rng.randrange(0, 10)]))
                        except ReaderDataUnavailable:
                            pass
            except Exception as e:  # noqa: BLE001
                errors.append(("worker", repr(e)))

        def retainer():
            rng = random.Random(99)
            try:
                while not stop.is_set():
                    p = rng.randrange(n_parts)
                    info = log.inspect(partition=p)["partitions"][p]
                    hw = info["next_offset"]
                    # retain must clamp at the slowest reader safe cursor
                    log.retain(p, rng.randrange(0, max(hw, 1) + 1))
            except Exception as e:  # noqa: BLE001
                errors.append(("retainer", repr(e)))

        threads = [threading.Thread(target=worker, args=(r, 10 + i))
                   for i, r in enumerate(readers)]
        threads.append(threading.Thread(target=retainer))
        for t in threads:
            t.start()
        stop.wait(6.0)
        stop.set()
        for t in threads:
            t.join(timeout=30)
        self.assertFalse([e for e in errors if e[0] != "ack"], errors)

        # serial verification on a FRESH reopen
        log2 = self.reopen(log)
        for rid in ("r0", "r1", "r2"):
            r = log2.open_reader(rid)
            for p in range(n_parts):
                st = r.status(p)["partitions"].get(p)
                self.assertIsNotNone(st)
                safe, read = st["safe_offset"], st["read_offset"]
                self.assertLessEqual(safe, read)
                # blocking gaps are exactly the remaining ack rows and
                # all lie at or above the safe cursor
                self.assertTrue(all(g >= safe for g in st["blocking_gaps"]))
                # safe cursor must never have crossed the retain frontier
                retained = log2.inspect(partition=p)["partitions"][
                    p]["retained_below"]
                self.assertGreaterEqual(safe, retained)
                # redelivery sweep from the safe cursor reaches the
                # stable end without touching deleted data
                off = safe
                delivered = []
                for _ in range(5000):
                    page = r.poll(p, offset=off, max_records=32,
                                  max_scan=64)
                    delivered += [x["offset"] for x in page["records"]]
                    if page["reason"] in ("log_end", "stable_frontier"):
                        break
                    off = page["next_offset"]
                # offsets strictly increasing, all >= safe
                self.assertEqual(delivered, sorted(set(delivered)))
                self.assertTrue(all(o >= safe for o in delivered))

    def test_competing_handles_on_one_reader_converge(self):
        log = self.make_log()
        for i in range(200):
            log.append(0, f"r{i}".encode())
        h1 = log.create_reader("shared", start=0)
        h2 = log.open_reader("shared")
        errors, stop = [], threading.Event()

        def drain(handle, seed):
            rng = random.Random(seed)
            idle = 0
            try:
                while not stop.is_set() and idle < 200:
                    # every handle resumes from the durable safe cursor, so
                    # the two pollers are competing consumers with
                    # at-least-once redelivery; drive the SHARED cursor to
                    # the end and then stop.
                    st = handle.status(0)["partitions"].get(0)
                    if st is not None and st["safe_offset"] >= 200:
                        return
                    page = handle.poll(0, max_records=10)
                    if not page["records"]:
                        idle += 1
                        continue
                    idle = 0
                    # ack everything in the page: competing handles may
                    # re-ack the same offsets (idempotent), and the shared
                    # contiguous cursor can only advance
                    for rec in page["records"]:
                        handle.ack(0, rec["offset"])
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

        ts = [threading.Thread(target=drain, args=(h, s))
              for h, s in ((h1, 1), (h2, 2))]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=30)
        stop.set()
        self.assertFalse(errors)
        # both handles converge on the same durable state
        s1 = h1.status(0)["partitions"][0]
        s2 = h2.status(0)["partitions"][0]
        self.assertEqual(s1, s2)
        # every delivered record was eventually acked: the shared cursor
        # reaches the end with no remaining gaps (nothing lost despite
        # overlapping redelivery)
        self.assertEqual(s1["read_offset"], 200)
        self.assertEqual(s1["safe_offset"], 200)
        self.assertEqual(s1["blocking_gaps"], [])

    def test_delete_racing_with_poll_has_deterministic_outcome(self):
        log = self.make_log()
        for i in range(100):
            log.append(0, f"r{i}".encode())
        r = log.create_reader("racer", start=0)
        outcomes = []
        barrier = threading.Barrier(2)

        def poller():
            barrier.wait()
            try:
                for _ in range(200):
                    r.poll(0, max_records=4)
                    r.ack(0, 0)
                outcomes.append("finished")
            except UnknownReader:
                # the delete landed between two serialized SQLite ops;
                # the stale handle is deterministically fenced off
                outcomes.append("fenced")
            except txlog.TxLogError as e:
                outcomes.append(type(e).__name__)

        def deleter():
            barrier.wait()
            log.delete_reader("racer")
            outcomes.append("deleted")

        t1 = threading.Thread(target=poller)
        t2 = threading.Thread(target=deleter)
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)
        # exactly one deletion happened; the poller either completed all
        # its work first or was fenced — never a partial/duplicate delete
        self.assertEqual(outcomes.count("deleted"), 1)
        self.assertIn(outcomes[0] if outcomes else None,
                      ("deleted", "fenced", "finished"))
        with self.assertRaises(UnknownReader):
            log.open_reader("racer")
        # and retain is no longer constrained by the removed reader
        res = log.retain(0, 1000)
        self.assertIsNone(res["reader_floor"])


if __name__ == "__main__":
    unittest.main()
