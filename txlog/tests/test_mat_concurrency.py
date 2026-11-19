"""Concurrency: two refreshes can never build on the same old release."""

import threading

from tests.mat_helpers import MatTestCase, assignment, event
import txlog


class TestRefreshConcurrency(MatTestCase):

    def test_concurrent_refreshers_serialize_without_duplicate_releases(self):
        log = self.make_log()
        store = self.open_store()
        for i in range(300):
            log.append(i % 4, event("e", i))

        errors, releases_seen = [], []
        barrier = threading.Barrier(5)

        def worker():
            barrier.wait()
            try:
                for _ in range(20):
                    rep = store.refresh()
                    if rep["published"]:
                        releases_seen.append(rep["release_no"])
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        self.assertFalse(errors)
        # every published release number is unique and gapless
        self.assertEqual(sorted(releases_seen),
                         list(range(1, len(releases_seen) + 1)))
        # final state is a single coherent prefix covering all events
        details = store.read_all_details()
        self.assertEqual(len(details), 300)

    def test_reads_remain_consistent_during_refresh(self):
        log = self.make_log()
        store = self.open_store()
        for i in range(200):
            log.append(i % 3, event("e", i))
        store.refresh()
        stop = threading.Event()
        errors = []

        def reader():
            cursor = None
            while not stop.is_set():
                try:
                    page = store.read_details(limit=7, cursor=cursor)
                    if not page["has_more"]:
                        cursor = None
                    else:
                        cursor = page["next_cursor"]
                except txlog.StaleCursorError:
                    cursor = None  # a new release landed: restart cleanly
                except Exception as exc:  # noqa: BLE001
                    errors.append(repr(exc))

        def refresher():
            rnd_event = 0
            while not stop.is_set():
                rnd_event += 1
                log.append(0, assignment("e", rnd_event % 5, 0, "g"))
                store.refresh()

        rt = threading.Thread(target=reader)
        wt = threading.Thread(target=refresher)
        rt.start(); wt.start()
        stop.wait(4.0)
        stop.set()
        rt.join(timeout=10); wt.join(timeout=10)
        self.assertFalse(errors)

    def test_second_store_process_loses_the_flock(self):
        import os as _os
        log = self.make_log()
        log.append(0, event("e", 1))
        store = self.open_store(lock_timeout=0.2)
        # hold the refresh lock in this thread, then a second handle must
        # fail fast rather than block and build on a stale release
        held = store._refresh_lock()
        held.__enter__()
        try:
            other = txlog.open_materialized(self.mat_path, create=False,
                                            lock_timeout=0.1)
            self.stores.append(other)
            with self.assertRaises(txlog.MaterializationBusy):
                other.refresh()
        finally:
            held.__exit__(None, None, None)
