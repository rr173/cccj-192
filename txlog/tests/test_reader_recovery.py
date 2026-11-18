"""Crash-recovery tests for named readers using REAL process exits.

The durable rules under test:

* reader poll progress and acks are committed in SQLite (synchronous=FULL),
  so an os._exit after the metadata commit but before the receipt reaches
  the caller leaves a coherent state: retried acks are idempotent and
  retried polls resume without skipping or duplicating durable progress;
* an exit before the commit loses nothing durable and the operation simply
  retries;
* all progress survives a fresh process reopening the log;
* a retain that crashes between catalog commit and file unlink finishes
  cleanly on recovery while reader cursors keep their hold.
"""

import json
import os
import unittest

from tests.helpers import LogTestCase, run_cli, run_cli_json

CRASH = 42


class TestReaderCrashRecovery(LogTestCase):
    SEGMENT_BYTES = 256

    def _seed(self, n=12):
        run_cli_json(["init", self.dir, "--segment-bytes",
                      str(self.SEGMENT_BYTES)])
        run_cli_json(["reader-create", self.dir, "r",
                      "--start-offset", "0"])
        for i in range(n):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", f"rec-{i}"])

    def test_ack_lost_receipt_is_idempotent_after_crash(self):
        self._seed()
        run_cli_json(["reader-poll", self.dir, "r", "--partition", "0",
                      "--max-records", "12"])
        # the ack metadata commits, then the process is killed before the
        # CLI can print the receipt
        rc, _, _ = run_cli(
            ["reader-ack", self.dir, "r", "--partition", "0",
             "--offset", "0"],
            env_extra={"TXLOG_FAULT": "reader.ack.after_meta"},
            expect=None)
        self.assertEqual(rc, CRASH)
        # the safe cursor durably advanced despite the lost receipt
        st = run_cli_json(["reader-status", self.dir, "r",
                           "--partition", "0"])
        self.assertEqual(st["partitions"]["0"]["safe_offset"], 1)
        # retrying the same ack is the idempotent duplicate path
        again = run_cli_json(["reader-ack", self.dir, "r",
                              "--partition", "0", "--offset", "0"])
        self.assertTrue(again["duplicate"])
        self.assertEqual(again["safe_offset"], 1)
        # the next offsets still need explicit acks
        run_cli_json(["reader-ack", self.dir, "r",
                      "--partition", "0", "--offset", "1"])
        st = run_cli_json(["reader-status", self.dir, "r",
                           "--partition", "0"])
        self.assertEqual(st["partitions"]["0"]["safe_offset"], 2)

    def test_poll_crash_keeps_durable_high_water_and_redelivers(self):
        self._seed()
        # first poll commits read_offset=12 then dies before returning
        rc, _, _ = run_cli(
            ["reader-poll", self.dir, "r", "--partition", "0",
             "--max-records", "12"],
            env_extra={"TXLOG_FAULT": "reader.poll.after_meta"},
            expect=None)
        self.assertEqual(rc, CRASH)
        # a fresh process handle resumes at safe=0: every record is
        # redelivered (nothing was acked), nothing skipped
        page = run_cli_json(["reader-poll", self.dir, "r",
                             "--partition", "0", "--max-records", "12"])
        self.assertEqual([x["offset"] for x in page["records"]],
                         list(range(12)))
        self.assertEqual(page["read_offset"], 12)

    def test_full_progress_survives_repeated_process_restarts(self):
        self._seed(8)
        run_cli_json(["reader-poll", self.dir, "r", "--partition", "0",
                      "--max-records", "8"])
        # out-of-order acks in separate processes; gap stays open
        run_cli_json(["reader-ack", self.dir, "r",
                      "--partition", "0", "--offset", "2"])
        run_cli_json(["reader-ack", self.dir, "r",
                      "--partition", "0", "--offset", "0"])
        st = run_cli_json(["reader-status", self.dir, "r",
                           "--partition", "0"])
        pos = st["partitions"]["0"]
        self.assertEqual(pos["safe_offset"], 1)
        self.assertEqual(pos["blocking_gaps"], [2])
        # close/reopen the log directory itself (recovery path)
        rc, _, _ = run_cli(
            ["reader-ack", self.dir, "r", "--partition", "0",
             "--offset", "1"],
            env_extra={"TXLOG_FAULT": "reader.ack.after_meta"},
            expect=None)
        self.assertEqual(rc, CRASH)
        st = run_cli_json(["reader-status", self.dir, "r",
                           "--partition", "0"])
        pos = st["partitions"]["0"]
        self.assertEqual(pos["safe_offset"], 3)
        self.assertEqual(pos["blocking_gaps"], [])

    def test_retain_crash_between_catalog_and_unlink_recovers(self):
        self._seed(12)
        # ack the first 4 so the reader holds at safe_offset=4
        run_cli_json(["reader-poll", self.dir, "r", "--partition", "0",
                      "--max-records", "12"])
        for off in range(4):
            run_cli_json(["reader-ack", self.dir, "r",
                          "--partition", "0", "--offset", str(off)])
        # retain commits its catalog updates, then crashes before unlinking
        rc, _, err = run_cli(
            ["retain", self.dir, "--partition", "0",
             "--before-offset", "4"],
            env_extra={"TXLOG_FAULT": "retain.after_catalog"},
            expect=None)
        self.assertEqual(rc, CRASH)
        # recovery on the next open finishes the pending deletions; the
        # reader hold and cursor are intact
        st = run_cli_json(["reader-status", self.dir, "r",
                           "--partition", "0"])
        self.assertEqual(st["partitions"]["0"]["safe_offset"], 4)
        res = run_cli_json(["retain", self.dir, "--partition", "0",
                            "--before-offset", "4"])
        self.assertEqual(res["retained_below"], 4)
        # reading below the frontier now reports the explicit boundary
        rc, _, err = run_cli(
            ["reader-poll", self.dir, "r", "--partition", "0",
             "--offset", "0"], expect=None)
        self.assertEqual(rc, 1)
        detail = json.loads(err)
        self.assertEqual(detail["error"], "ReaderDataUnavailable")
        self.assertEqual(detail["retained_below"], 4)
        # and the reader can continue from its safe cursor
        page = run_cli_json(["reader-poll", self.dir, "r",
                             "--partition", "0"])
        self.assertEqual([x["offset"] for x in page["records"]],
                         list(range(4, 12)))


if __name__ == "__main__":
    unittest.main()
