"""Crash tests for online backup / restore.

Real ``os._exit(42)`` crashes (via TXLOG_FAULT) at the backup and restore
fault points prove the publication/restore contracts under process death:

  * a crash while copying or before publication leaves no visible recovery
    point, no half-published manifest, and the repository stays usable —
    the same name can be published afterwards and unreferenced blocks are
    reclaimed by gc;
  * a crash after the manifest is durable leaves a fully valid point;
  * a crash during restore leaves no finished target log, only a removable
    staging directory, and retrying the restore succeeds exactly once.
"""

import glob
import os

import txlog
from txlog.backup import open_repository
from txlog.errors import BackupTargetExists

from tests.helpers import LogTestCase, run_cli, run_cli_json

CRASH = 42


class TestBackupCrash(LogTestCase):
    SEGMENT_BYTES = 256

    def setUp(self):
        super().setUp()
        self.repo = os.path.join(self.tmp.name, "repo")
        run_cli(["init", self.dir, "--segment-bytes", "256",
                 "--dedup-window", "8"])
        run_cli_json(["open-producer", self.dir, "p"])
        run_cli_json(["begin", self.dir, "p", "--epoch", "1"])
        run_cli_json(["append", self.dir, "p", "--epoch", "1",
                      "--partition", "0", "--seq", "0", "--data", "tx"])
        for i in range(10):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", "m" * 40])
        run_cli_json(["backup-init", self.repo])

    def _points(self):
        out = run_cli_json(["backup-list", self.repo])
        return [p["name"] for p in out["points"]]

    def test_crash_after_capture_leaves_no_point_and_republishes(self):
        rc, _, err = run_cli(
            ["backup-create", self.repo, self.dir, "p1"],
            env_extra={"TXLOG_FAULT": "backup.after_capture"}, expect=None)
        self.assertEqual(rc, CRASH)
        self.assertEqual(self._points(), [])
        # blocks may have been staged, but nothing is referenced; gc reclaims
        gc = run_cli_json(["backup-gc", self.repo])
        self.assertEqual(len(gc["removed_stale_jobs"]), 1)
        # the same name is now free and publishes cleanly
        out = run_cli_json(["backup-create", self.repo, self.dir, "p1"])
        self.assertEqual(out["name"], "p1")
        self.assertEqual(self._points(), ["p1"])
        run_cli_json(["backup-verify", self.repo, "p1"])

    def test_crash_before_publish_leaves_no_point(self):
        rc, _, _ = run_cli(
            ["backup-create", self.repo, self.dir, "p1"],
            env_extra={"TXLOG_FAULT": "backup.before_publish"}, expect=None)
        self.assertEqual(rc, CRASH)
        self.assertEqual(self._points(), [])
        run_cli_json(["backup-gc", self.repo])
        run_cli_json(["backup-create", self.repo, self.dir, "p1"])
        self.assertEqual(self._points(), ["p1"])

    def test_crash_after_publish_point_is_durable(self):
        rc, _, _ = run_cli(
            ["backup-create", self.repo, self.dir, "p1"],
            env_extra={"TXLOG_FAULT": "backup.after_publish"}, expect=None)
        self.assertEqual(rc, CRASH)
        self.assertEqual(self._points(), ["p1"])
        run_cli_json(["backup-verify", self.repo, "p1"])
        target = os.path.join(self.tmp.name, "target")
        run_cli_json(["backup-restore", self.repo, "p1", target])
        # the restored log is a normal, reopenable txlog
        page = run_cli_json(["fetch", target, "--partition", "0",
                             "--isolation", "read_uncommitted"])
        self.assertGreaterEqual(page["high_watermark"], 1)

    def test_crash_during_restore_leaves_no_finished_log(self):
        run_cli_json(["backup-create", self.repo, self.dir, "p1"])
        target = os.path.join(self.tmp.name, "target")
        rc, _, _ = run_cli(
            ["backup-restore", self.repo, "p1", target],
            env_extra={"TXLOG_FAULT": "restore.before_rename"}, expect=None)
        self.assertEqual(rc, CRASH)
        self.assertFalse(os.path.exists(target))
        leftovers = glob.glob(target + ".restoring-*")
        self.assertTrue(leftovers)
        # retry: exactly one success, stale staging removed automatically
        run_cli_json(["backup-restore", self.repo, "p1", target])
        self.assertTrue(os.path.exists(target))
        self.assertFalse(glob.glob(target + ".restoring-*"))
        run_cli_json(["fetch", target, "--partition", "0"])

    def test_restore_never_overwrites_existing_directory(self):
        run_cli_json(["backup-create", self.repo, self.dir, "p1"])
        target = os.path.join(self.tmp.name, "target")
        os.makedirs(target)
        rc, out, err = run_cli(
            ["backup-restore", self.repo, "p1", target], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("BackupTargetExists", err)
        # the pre-existing directory was not touched
        self.assertEqual(os.listdir(target), [])


class TestBackupInProcessCrash(LogTestCase):
    SEGMENT_BYTES = 256

    def test_blocks_staged_by_crashed_publish_are_reclaimed(self):
        # A crash after capture may already have written blocks; gc must
        # reclaim them, and the later successful point is independent.
        log = self.make_log()
        for i in range(10):
            log.append(0, b"m" * 40)
        log.close()
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        root = os.path.dirname(os.path.dirname(
            os.path.abspath(txlog.__file__)))

        import subprocess
        import sys
        code = (
            "import sys; sys.path.insert(0, %r)\n"
            "import txlog\n"
            "from txlog.backup import open_repository\n"
            "log = txlog.Log(%r, writable=False, create=False)\n"
            "repo = open_repository(%r)\n"
            "repo.create_backup(log, 'p1')\n"
            % (root, self.dir, repo.path))
        env = dict(os.environ, TXLOG_FAULT="backup.after_capture",
                   PYTHONPATH=root)
        cp = subprocess.run([sys.executable, "-c", code],
                            capture_output=True, text=True, env=env)
        self.assertEqual(cp.returncode, CRASH, cp.stderr)
        self.assertEqual(repo.list_points(), [])
        gc = repo.gc()
        self.assertEqual(len(gc["removed_stale_jobs"]), 1)
        # second attempt in this process succeeds and is self-contained
        log2 = txlog.Log(self.dir, writable=False, create=False)
        self.logs.append(log2)
        repo.create_backup(log2, "p1")
        self.assertEqual([p["name"] for p in repo.list_points()], ["p1"])
        repo.verify_point("p1")
        target = os.path.join(self.tmp.name, "target")
        repo.restore_point("p1", target)
        reopened = txlog.Log(target, writable=False)
        self.assertEqual(
            reopened.fetch(0, 0, 1000,
                          isolation="read_uncommitted")["high_watermark"],
            log2.fetch(0, 0, 1,
                       isolation="read_uncommitted")["high_watermark"])
        reopened.close()
