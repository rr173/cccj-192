"""CLI tests for the backup repository verbs."""

import json
import os

from tests.helpers import LogTestCase, run_cli, run_cli_json


class TestBackupCli(LogTestCase):
    SEGMENT_BYTES = 256

    def _seed(self):
        run_cli(["init", self.dir, "--segment-bytes", "256",
                 "--dedup-window", "8"])
        run_cli_json(["open-producer", self.dir, "p"])
        run_cli_json(["begin", self.dir, "p", "--epoch", "1"])
        run_cli_json(["append", self.dir, "p", "--epoch", "1",
                      "--partition", "0", "--seq", "0", "--data", "t"])
        run_cli_json(["commit", self.dir, "p", "--epoch", "1"])
        for i in range(8):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", "m" * 40])

    def test_backup_cli_lifecycle(self):
        self._seed()
        repo = os.path.join(self.tmp.name, "repo")
        out = run_cli_json(["backup-init", repo])
        self.assertEqual(out["repository"], os.path.abspath(repo))

        p1 = run_cli_json(["backup-create", repo, self.dir, "p1"])
        self.assertEqual(p1["name"], "p1")
        self.assertIn("0", p1["partitions"])

        # duplicate name is refused explicitly on stderr
        rc, _stdout, err = run_cli(
            ["backup-create", repo, self.dir, "p1"], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("BackupPointExists", err)

        listing = run_cli_json(["backup-list", repo])
        self.assertEqual([p["name"] for p in listing["points"]], ["p1"])
        point = listing["points"][0]
        # the list exposes the source identity and the retention/write
        # frontiers of the captured view
        self.assertEqual(len(point["source"]["log_id"]), 32)
        self.assertIn("retained_below", point["partitions"]["0"])
        self.assertIn("next_offset", point["partitions"]["0"])

        verify = run_cli_json(["backup-verify", repo, "p1"])
        self.assertTrue(verify["ok"])
        self.assertGreater(verify["blocks_checked"], 0)

        target = os.path.join(self.tmp.name, "target")
        restore = run_cli_json(["backup-restore", repo, "p1", target])
        self.assertEqual(restore["point"], "p1")
        self.assertNotEqual(restore["new_log_id"],
                            restore["source_log_id"])
        page = run_cli_json(["fetch", target, "--partition", "0"])
        self.assertGreater(page["high_watermark"], 1)

        # restore into an existing path is refused
        rc, _stdout, err = run_cli(
            ["backup-restore", repo, "p1", target], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("BackupTargetExists", err)

        deletion = run_cli_json(["backup-delete", repo, "p1"])
        self.assertEqual(deletion["point"], "p1")
        rc, _stdout, err = run_cli(
            ["backup-verify", repo, "p1"], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("BackupPointNotFound", err)

    def test_backup_create_runs_while_writer_is_active(self):
        self._seed()
        repo = os.path.join(self.tmp.name, "repo")
        run_cli_json(["backup-init", repo])
        # --pause-after-capture keeps the backup past its coordination
        # phase; meanwhile another CLI process appends successfully.
        import subprocess
        import sys
        from tests.helpers import ROOT
        env = dict(os.environ,
                   PYTHONPATH=ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        backup = subprocess.Popen(
            [sys.executable, "-m", "txlog", "backup-create", repo,
             self.dir, "p1", "--pause-after-capture", "1.5"],
            env=env, cwd="/", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            appended = run_cli_json(
                ["append", self.dir, "--partition", "1", "--data", "live"])
            self.assertEqual(appended["offset"], 0)
            stdout, stderr = backup.communicate(timeout=30)
            self.assertEqual(backup.returncode, 0, stderr.decode())
        finally:
            if backup.poll() is None:
                backup.kill()
        point = json.loads(stdout.decode())
        # the paused point never saw the append that landed during pause
        self.assertNotIn("1", point["partitions"])
        second = run_cli_json(["backup-create", repo, self.dir, "p2"])
        self.assertIn("1", second["partitions"])

    def test_backup_on_unknown_repo_and_point(self):
        self._seed()
        missing = os.path.join(self.tmp.name, "no-repo")
        rc, _o, err = run_cli(
            ["backup-list", missing], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("BackupPointNotFound", err)
        run_cli_json(["backup-init", missing])
        rc, _o, err = run_cli(
            ["backup-verify", missing, "ghost"], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("BackupPointNotFound", err)
