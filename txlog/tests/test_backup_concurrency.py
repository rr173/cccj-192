"""Concurrency tests for the backup repository.

  * a delete racing a verify/restore never removes blocks that are in use:
    it waits for the repository lock and then honours live leases rather
    than returning a recovery point whose bytes have vanished;
  * two concurrent restores into the same target path: at most one
    succeeds, the other reports the target busy/exists;
  * capture takes no lock that blocks source appends, transaction
    decisions, reader acks or retention (the long-running pause case).
"""

import glob
import os
import threading

import txlog
from txlog.backup import open_repository
from txlog.errors import BackupBusy, BackupPointNotFound

from tests.helpers import LogTestCase


class TestBackupConcurrency(LogTestCase):
    SEGMENT_BYTES = 256

    def _populated(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"open", seq=0)
        for i in range(12):
            log.append(1, b"m" * 40)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "p1")
        repo.create_backup(log, "p2")
        return log, repo

    def test_delete_waits_for_active_verify_then_succeeds(self):
        log, repo = self._populated()
        entered = threading.Event()
        release = threading.Event()

        def hook(point):
            if point == "verify.entered":
                entered.set()
                self.assertTrue(release.wait(timeout=10))

        repo._fault.hook = hook
        t = threading.Thread(target=repo.verify_point, args=("p1",))
        t.start()
        self.assertTrue(entered.wait(timeout=10))
        # while the verify lease is live, delete reports busy rather than
        # removing referenced bytes
        with self.assertRaises(BackupBusy):
            repo.delete_point("p1")
        release.set()
        t.join(timeout=10)
        self.assertFalse(t.is_alive())
        report = repo.delete_point("p1")
        self.assertIsInstance(report["released_blocks"], list)
        # p2 is untouched and restores fine
        repo.restore_point("p2", os.path.join(self.tmp.name, "t2"))

    def test_concurrent_restores_same_target_one_wins(self):
        _log, repo = self._populated()
        target = os.path.join(self.tmp.name, "target")
        results = {}
        barrier = threading.Barrier(2)

        def restore(which):
            barrier.wait()
            try:
                results[which] = ("ok",
                                  repo.restore_point("p1", target))
            except Exception as e:
                results[which] = (type(e).__name__, e)

        t1 = threading.Thread(target=restore, args=(1,))
        t2 = threading.Thread(target=restore, args=(2,))
        t1.start(); t2.start()
        t1.join(timeout=30); t2.join(timeout=30)
        kinds = {k: v[0] for k, v in results.items()}
        self.assertEqual(sorted(kinds), [1, 2])
        self.assertEqual(sorted(kinds.values()).count("ok"), 1)
        loser = next(k for k, v in results.items() if v[0] != "ok")
        self.assertIn(results[loser][0],
                      ("BackupBusy", "BackupTargetExists"))
        # exactly one finished log exists and it is usable
        self.assertTrue(os.path.isdir(target))
        self.assertFalse(glob.glob(target + ".restoring-*"))
        rl = txlog.Log(target, writable=False)
        rl.close()

    def test_concurrent_delete_and_verify_never_returns_dangling_point(self):
        log, repo = self._populated()
        # delete while verifications repeatedly run; every verify that
        # returns must correspond to a still-present manifest's blocks.
        stop = threading.Event()
        verify_errors = []

        def verifier():
            while not stop.is_set():
                try:
                    repo.verify_point("p1")
                except BackupPointNotFound:
                    return
                except Exception as e:  # pragma: no cover
                    verify_errors.append(e)
                    return
                # yield so an exclusive delete lock can be granted
                stop.wait(0.01)

        t = threading.Thread(target=verifier)
        t.start()
        # delete while verifications keep running; retry through the short
        # windows between verify lock acquisitions
        deleted = False
        for _ in range(500):
            try:
                repo.delete_point("p1")
                deleted = True
                break
            except BackupBusy:
                threading.Event().wait(0.01)
        self.assertTrue(deleted, "delete never acquired the repository lock")
        stop.set()
        t.join(timeout=10)
        self.assertEqual(verify_errors, [])
        with self.assertRaises(BackupPointNotFound):
            repo.verify_point("p1")
        # p2 still fully works
        self.assertTrue(repo.verify_point("p2")["ok"])

    def test_capture_never_blocks_source_writes_or_retention(self):
        log = self.make_log()
        for i in range(8):
            log.append(0, b"m" * 40)
        rdr = log.create_reader("r", start=0)
        rdr.poll(0, max_records=100)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        source_moved = threading.Event()

        def pause(_view):
            # From inside the paused backup, the source must still accept
            # every kind of operation without deadlocking.
            def mutate():
                p = log.producer("w")
                p.begin()
                p.append(0, b"during-pause", seq=0)
                p.commit()
                log.append(0, b"plain-during-pause")
                rdr.ack(0, 0)
                log.retain(0, before_offset=10_000)
                source_moved.set()

            t = threading.Thread(target=mutate)
            t.start()
            self.assertTrue(source_moved.wait(timeout=10),
                            "source stalled while the backup was paused")
            t.join(timeout=10)
            self.assertFalse(t.is_alive())

        repo.create_backup(log, "p1", after_capture=pause)
        # the paused point excludes everything that happened during the
        # pause: its high watermark is the one captured before the thread
        # ran, while the source itself has advanced past it.
        [point] = repo.list_points()
        self.assertLess(
            point["partitions"]["0"]["next_offset"],
            log.inspect(0)["partitions"][0]["next_offset"])
        # a second point sees the source's new high watermark
        second = repo.create_backup(log, "p2")
        self.assertGreater(
            second["partitions"]["0"]["next_offset"],
            point["partitions"]["0"]["next_offset"])

    def test_gc_never_sweeps_blocks_of_an_in_progress_publish(self):
        log = self.make_log()
        for i in range(10):
            log.append(0, b"m" * 40)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        ran_gc = threading.Event()

        def pause(_view):
            # blocks are being archived now; a concurrent gc must not
            # remove the not-yet-published blocks.
            report = repo.gc()
            self.assertTrue(report["skipped_due_to_active_publish"])
            self.assertEqual(report["freed_bytes"], 0)
            ran_gc.set()

        repo.create_backup(log, "p1", after_capture=pause)
        self.assertTrue(ran_gc.wait(timeout=10))
        # the point completed with every block intact
        self.assertTrue(repo.verify_point("p1")["ok"])
        # once no publish is active, a normal gc finds nothing unreferenced
        idle = repo.gc()
        self.assertFalse(idle["skipped_due_to_active_publish"])
        self.assertEqual(idle["freed_bytes"], 0)
