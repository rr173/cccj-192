"""Retention, fetch paging, fetch/retain races, snapshot isolation."""

import threading
import time
import unittest

import txlog
from tests.helpers import LogTestCase


class TestRetain(LogTestCase):
    SEGMENT_BYTES = 200

    def _fill(self, log, n, partition=0):
        for i in range(n):
            log.append(partition, f"rec-{i}".encode())

    def test_retain_deletes_sealed_segments_and_exposes_boundary(self):
        log = self.make_log()
        self._fill(log, 20)
        res = log.retain(0, 12)
        self.assertEqual(res["applied"], 12)
        self.assertEqual(res["retained_below"], 12)
        self.assertEqual(res["constrained_by"], "requested")
        self.assertTrue(res["deleted_segments"])
        # deleted files are really gone; catalog keeps the evidence
        for s in res["deleted_segments"]:
            self.assertFalse(__import__("os").path.exists(
                __import__("os").path.join(log.path, s["path"])))
        info = log.inspect(partition=0)["partitions"][0]
        self.assertEqual(info["retained_below"], 12)
        states = {s["state"] for s in info["segments"]}
        self.assertIn("deleted", states)
        # fetch below the frontier reports the boundary, not "end of log"
        page = log.fetch(0, 0)
        self.assertEqual(page["reason"], "retained_boundary")
        self.assertEqual(page["next_offset"], 12)
        # fetch at the frontier reads live data
        page = log.fetch(0, 12)
        self.assertEqual(page["records"][0]["payload"], b"rec-12")

    def test_retain_never_touches_pending_transaction_data(self):
        log = self.make_log()
        self._fill(log, 6)
        p = log.producer("p")
        p.begin()
        p.append(0, b"pending", seq=0)   # offset 6, open
        res = log.retain(0, 100)
        self.assertEqual(res["constrained_by"], "lso")
        self.assertEqual(res["applied"], 6)
        # pending record still readable
        page = log.fetch(0, 6, isolation="read_uncommitted")
        self.assertEqual(page["records"][0]["payload"], b"pending")

    def test_retain_respects_dedup_window_floor(self):
        log = self.make_log()
        p = log.producer("p")
        for i in range(6):
            p.begin()
            p.append(0, f"d-{i}".encode(), seq=i)
            p.commit()
        # dedup window is 8 > 6 records, so the floor is offset 0
        res = log.retain(0, 100)
        self.assertEqual(res["constrained_by"], "dedup_window")
        self.assertEqual(res["applied"], 0)
        # retry identity inside the window still dedups after retain
        p.begin()
        self.assertEqual(p.append(0, b"d-4", seq=4), 4)

    def test_txn_evidence_survives_until_all_partitions_retain(self):
        log = self.make_log()
        p = log.producer("p")
        t = p.begin()
        p.append(0, b"on-p0", seq=0)
        p.append(1, b"on-p1", seq=0)
        p.commit()
        # the txn's own dedup rows pin retention at offset 0 (window=8)
        res = log.retain(0, 100)
        self.assertEqual(res["constrained_by"], "dedup_window")
        # p0 retains past the txn (after the producer's epoch moves on and
        # the old retry identity is dropped): evidence must stay, because
        # p1 still needs it to classify its records
        log.producer("p")  # epoch 2: drops epoch-1 dedup identity
        res = log.retain(0, 100)
        self.assertEqual(res["applied"], 1)
        st = log.txn_state(t)
        self.assertEqual(st["state"], "committed")
        page = log.fetch(1, 0)
        self.assertEqual(page["records"][0]["txn_status"], "committed")
        # p1 retains too: now the evidence may be collected
        log.retain(1, 100)
        self.assertIsNone(log.txn_state(t))

    def test_retain_idempotent_and_monotonic(self):
        log = self.make_log()
        self._fill(log, 10)
        log.retain(0, 5)
        res = log.retain(0, 3)   # moving backwards is a no-op
        self.assertEqual(res["retained_below"], 5)
        res = log.retain(0, 5)
        self.assertEqual(res["retained_below"], 5)


class TestFetchPaging(LogTestCase):
    def test_bounded_pages_and_next_offset(self):
        log = self.make_log()
        for i in range(10):
            log.append(0, f"r{i}".encode())
        seen = []
        offset = 0
        while True:
            page = log.fetch(0, offset, max_records=3)
            seen += [r["offset"] for r in page["records"]]
            offset = page["next_offset"]
            if page["reason"] == "log_end":
                break
            self.assertEqual(page["reason"], "page_full")
        self.assertEqual(seen, list(range(10)))
        self.assertEqual(offset, 10)

    def test_long_abort_hole_scan_is_bounded(self):
        log = self.make_log()
        p = log.producer("p")
        # one big aborted transaction = a long hole
        p.begin()
        for i in range(300):
            p.append(0, f"hole-{i}".encode(), seq=i)
        p.abort()
        # one committed record after the hole
        p.begin()
        p.append(0, b"live", seq=300)
        p.commit()
        # read_committed with a small scan budget must make progress
        # without unbounded scanning and must not fabricate "log end"
        offset = 0
        seen = []
        reasons = []
        last_page = None
        for _ in range(1000):
            page = log.fetch(0, offset, max_records=4, max_scan=25)
            last_page = page
            self.assertLessEqual(len(page["records"]), 4)
            seen += [r["offset"] for r in page["records"]]
            reasons.append(page["reason"])
            self.assertGreaterEqual(page["next_offset"], offset)
            if page["reason"] == "log_end":
                break
            self.assertGreater(page["next_offset"], offset,
                               "scan must always advance")
            offset = page["next_offset"]
        else:
            self.fail("fetch did not terminate over a long abort hole")
        self.assertEqual(seen, [300])
        self.assertIn("scan_limit", reasons)
        self.assertEqual(last_page["next_offset"], 301)

    def test_read_uncommitted_marks_states(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"open", seq=0)
        q = log.producer("q")
        q.begin()
        q.append(0, b"committed", seq=0)
        q.commit()
        log.append(0, b"plain")
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["txn_status"] for r in page["records"]],
                         ["open", "committed", None])

    def test_fetch_snapshot_is_stable_under_concurrent_commit(self):
        # a commit landing mid-fetch must not change the page's view
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"pending", seq=0)
        writer = self.make_log()  # second writable handle, same process
        fired = {"n": 0}

        def hook(point):
            if point == "fetch.after_snapshot" and fired["n"] == 0:
                fired["n"] += 1
                # commit lands in the middle of the reader's fetch
                writer._commit("p", p.epoch)

        reader = self.make_log(fault_hook=hook)
        page = reader.fetch(0, 0, isolation="read_committed")
        # snapshot was taken before the commit: still blocked at frontier
        self.assertEqual(page["records"], [])
        self.assertEqual(page["reason"], "stable_frontier")
        self.assertEqual(fired["n"], 1)
        # a later fetch sees the commit
        page = reader.fetch(0, 0, isolation="read_committed")
        self.assertEqual(len(page["records"]), 1)


class TestFetchRetainRace(LogTestCase):
    SEGMENT_BYTES = 300

    def test_concurrent_fetch_and_retain(self):
        log = self.make_log()
        errors = []
        stop = threading.Event()

        def writer():
            try:
                for i in range(400):
                    log.append(0, f"rec-{i}".encode())
            except Exception as e:  # noqa: BLE001
                errors.append(("writer", e))
            finally:
                stop.set()

        def retainer():
            boundary = 0
            while not stop.is_set():
                try:
                    log.retain(0, boundary)
                    boundary += 2
                except Exception as e:  # noqa: BLE001
                    errors.append(("retain", e))
                    return
                time.sleep(0.001)

        def reader():
            while not stop.is_set():
                try:
                    page = log.fetch(0, 0, max_records=5)
                    if page["reason"] == "retained_boundary":
                        page = log.fetch(0, page["retained_below"],
                                         max_records=5)
                    for rec in page["records"]:
                        want = f"rec-{rec['offset']}".encode()
                        if rec["payload"] != want:
                            errors.append(("reader-data",
                                           (rec["offset"], rec["payload"])))
                            return
                except Exception as e:  # noqa: BLE001
                    errors.append(("reader", e))
                    return

        threads = [threading.Thread(target=writer),
                   threading.Thread(target=retainer)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        self.assertFalse(errors, errors)


if __name__ == "__main__":
    unittest.main()
