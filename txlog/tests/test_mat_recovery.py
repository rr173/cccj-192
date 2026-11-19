"""Real process-exit crash recovery for materialized refreshes.

Fault points (see txlog/faults.py):

  materialize.before_commit  everything computed, SQLite publish not committed
  materialize.after_commit   release committed, receipt lost before return

A process killed before the commit leaves no release and the retry rebuilds
the same round; a process killed after the commit durably published, so the
retry must not republish the same batch as a new release or resend changes.
"""

import json
import os
import unittest

from tests.helpers import run_cli, run_cli_json
from tests.mat_helpers import MatTestCase, assignment, event

CRASH = 42


class TestMatCrashRecovery(MatTestCase):

    def _seed_cli(self, n):
        run_cli_json(["init", self.dir, "--segment-bytes",
                      str(self.SEGMENT_BYTES), "--dedup-window",
                      str(self.DEDUP_WINDOW)])
        run_cli_json(["mat-open", self.mat_path, self.dir])
        for i in range(n):
            run_cli_json(["append", self.dir, "--partition", str(i % 3),
                          "--data", json.dumps(
                              {"kind": "event", "entity": "e",
                               "event_time": i})])

    def test_exit_before_commit_leaves_no_release_and_retries_cleanly(self):
        self._seed_cli(8)
        rc, _, err = run_cli(
            ["mat-refresh", self.mat_path],
            env_extra={"TXLOG_FAULT": "materialize.before_commit"},
            expect=None)
        self.assertEqual(rc, CRASH)
        # nothing durable was published
        st = run_cli_json(["mat-status", self.mat_path])
        self.assertIsNone(st["latest_release"])
        self.assertEqual(st["event_count"], 0)
        # a fresh process retries the whole round and publishes exactly once
        rep = run_cli_json(["mat-refresh", self.mat_path])
        self.assertTrue(rep["published"])
        self.assertEqual(rep["release_no"], 1)
        self.assertEqual(rep["change_count"], 8)
        st = run_cli_json(["mat-status", self.mat_path])
        self.assertEqual(st["latest_release"], 1)
        self.assertEqual(st["event_count"], 8)

    def test_exit_after_commit_lost_receipt_does_not_republish(self):
        self._seed_cli(6)
        # first publish succeeds
        rep = run_cli_json(["mat-refresh", self.mat_path])
        self.assertEqual(rep["release_no"], 1)
        # add more data, then crash AFTER the commit but before the receipt
        for i in range(6, 10):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", json.dumps(
                              {"kind": "event", "entity": "e",
                               "event_time": i})])
        rc, _, _ = run_cli(
            ["mat-refresh", self.mat_path],
            env_extra={"TXLOG_FAULT": "materialize.after_commit"},
            expect=None)
        self.assertEqual(rc, CRASH)
        # release 2 durably exists despite the lost receipt
        st = run_cli_json(["mat-status", self.mat_path])
        self.assertEqual(st["latest_release"], 2)
        self.assertEqual(st["event_count"], 10)
        # retry: positions unchanged -> no new release, changes of release 2
        # are not re-sent under a new number
        rep = run_cli_json(["mat-refresh", self.mat_path])
        self.assertFalse(rep["published"])
        self.assertEqual(rep["release_no"], 2)
        ch2 = run_cli_json(["mat-changes", self.mat_path, "--release", "2"])
        self.assertEqual(len(ch2["rows"]), 4)
        # and there is no release 3
        rc, _, _ = run_cli(["mat-changes", self.mat_path, "--release", "3"],
                           expect=1)
        self.assertEqual(rc, 1)

    def test_reopen_in_new_process_continues_incrementally(self):
        self._seed_cli(5)
        run_cli_json(["mat-refresh", self.mat_path])
        run_cli_json(["append", self.dir, "--partition", "0",
                      "--data", json.dumps(
                          {"kind": "assignment", "entity": "e",
                           "effective_from": 0, "revision": 1,
                           "group": "blue"})])
        # a late rule re-assigns all five prior events in release 2
        rep = run_cli_json(["mat-refresh", self.mat_path])
        self.assertEqual(rep["release_no"], 2)
        self.assertEqual(rep["change_count"], 5)
        page = run_cli_json(["mat-details", self.mat_path])
        self.assertTrue(all(r["group"] == "blue" for r in page["rows"]))


if __name__ == "__main__":
    unittest.main()
