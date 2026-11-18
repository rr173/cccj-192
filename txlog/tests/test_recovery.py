"""Crash-recovery tests: real process exits at injected fault points.

Every scenario kills the process (os._exit via TXLOG_FAULT) at a precise
point between segment append, fsync and metadata commit, then reopens the
log and checks the deterministic recovery outcome.  Calls that never
returned may be retried; acknowledged records must survive with their
original offsets.
"""

import json
import os
import unittest

import txlog
from tests.helpers import LogTestCase, run_cli, run_cli_json, run_py

CRASH = 42  # exit code used by the fault injector


def snippet(body):
    return "import txlog\n" + body


class TestCrashRecovery(LogTestCase):
    SEGMENT_BYTES = 512

    def test_crash_after_fsync_before_meta_truncates_unacked(self):
        # record fsynced, metadata not committed: the append was never
        # acknowledged, so recovery must truncate it and reuse the offset.
        run_cli(["init", self.dir, "--segment-bytes", "512"])
        rc, _, _ = run_cli(
            ["append", self.dir, "--partition", "0", "--data", "lost"],
            env_extra={"TXLOG_FAULT": "append.after_fsync"}, expect=None)
        self.assertEqual(rc, CRASH)
        # retry after reopen: gets offset 0 again, no duplicate
        out = run_cli_json(["append", self.dir, "--partition", "0",
                            "--data", "kept"])
        self.assertEqual(out["offset"], 0)
        page = run_cli_json(["fetch", self.dir, "--partition", "0",
                             "--isolation", "read_uncommitted"])
        self.assertEqual([r["offset"] for r in page["records"]], [0])
        self.assertEqual(page["records"][0]["payload_text"], "kept")
        self.assertEqual(page["high_watermark"], 1)

    def test_crash_after_meta_keeps_record(self):
        # metadata committed, receipt lost on the way out: the record is
        # durable and must survive recovery unchanged.
        run_cli(["init", self.dir, "--segment-bytes", "512"])
        rc, _, _ = run_cli(
            ["append", self.dir, "--partition", "0", "--data", "durable"],
            env_extra={"TXLOG_FAULT": "append.after_meta"}, expect=None)
        self.assertEqual(rc, CRASH)
        page = run_cli_json(["fetch", self.dir, "--partition", "0",
                             "--isolation", "read_uncommitted"])
        self.assertEqual([r["offset"] for r in page["records"]], [0])
        self.assertEqual(page["records"][0]["payload_text"], "durable")
        # next append continues at offset 1
        out = run_cli_json(["append", self.dir, "--partition", "0",
                            "--data", "next"])
        self.assertEqual(out["offset"], 1)

    def test_txn_append_crash_then_retry_same_sequence(self):
        # transactional append durable but receipt lost; producer reopens
        # (new epoch fences the old txn); the retried sequence of the OLD
        # epoch is fenced, and the old record stays an aborted hole with
        # its original offset.  No duplicate is ever written.
        run_cli(["init", self.dir, "--segment-bytes", "512",
                 "--dedup-window", "8"])
        run_cli_json(["open-producer", self.dir, "p"])
        run_cli_json(["begin", self.dir, "p", "--epoch", "1"])
        rc, _, _ = run_cli(
            ["append", self.dir, "p", "--epoch", "1", "--partition", "0",
             "--seq", "0", "--data", "txn-data"],
            env_extra={"TXLOG_FAULT": "append.after_meta"}, expect=None)
        self.assertEqual(rc, CRASH)
        # producer reopens: fencing aborts the old open transaction
        out = run_cli_json(["open-producer", self.dir, "p"])
        self.assertEqual(out["epoch"], 2)
        self.assertEqual(out["aborted_transactions"], 1)
        # old epoch retry is fenced, not re-appended
        rc, _, err = run_cli(
            ["append", self.dir, "p", "--epoch", "1", "--partition", "0",
             "--seq", "0", "--data", "txn-data"], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("ProducerFenced", err)
        # the old record is a durable aborted hole at offset 0
        page = run_cli_json(["fetch", self.dir, "--partition", "0",
                             "--isolation", "read_uncommitted"])
        self.assertEqual(len(page["records"]), 1)
        self.assertEqual(page["records"][0]["txn_status"], "aborted")
        self.assertEqual(page["records"][0]["offset"], 0)
        # read_committed sees nothing; the hole pins nothing (txn decided)
        page = run_cli_json(["fetch", self.dir, "--partition", "0"])
        self.assertEqual(page["records"], [])
        self.assertEqual(page["reason"], "log_end")

    def test_cross_partition_commit_crash_before_decision(self):
        code = snippet(f"""
log = txlog.open_log({self.dir!r}, segment_bytes=512, dedup_window=8)
p = log.producer("p")
p.begin()
p.append(0, b"for-p0", seq=0)
p.append(1, b"for-p1", seq=0)
p.commit()
""")
        cp = run_py(code, env_extra={"TXLOG_FAULT": "commit.before_decision"},
                    expect=None)
        self.assertEqual(cp.returncode, CRASH)
        # decision never happened: txn is still open on both partitions
        log = self.make_log()
        self.assertEqual(log.fetch(0, 0)["records"], [])
        self.assertEqual(log.fetch(1, 0)["records"], [])
        self.assertEqual(log.fetch(0, 0)["reason"], "stable_frontier")
        self.assertEqual(log.fetch(1, 0)["reason"], "stable_frontier")
        # abort it: both partitions get holes, neither leaks committed data
        p = log.producer("p")  # epoch 2 fences+aborts the open txn
        for part in (0, 1):
            page = log.fetch(part, 0, isolation="read_uncommitted")
            self.assertEqual([r["txn_status"] for r in page["records"]],
                             ["aborted"])
            self.assertEqual(log.fetch(part, 0)["records"], [])

    def test_cross_partition_commit_crash_after_decision(self):
        code = snippet(f"""
log = txlog.open_log({self.dir!r}, segment_bytes=512, dedup_window=8)
p = log.producer("p")
p.begin()
p.append(0, b"for-p0", seq=0)
p.append(1, b"for-p1", seq=0)
p.commit()
""")
        cp = run_py(code, env_extra={"TXLOG_FAULT": "commit.after_decision"},
                    expect=None)
        self.assertEqual(cp.returncode, CRASH)
        # the decision was durable: BOTH partitions see the txn committed —
        # never one committed and the other not.
        log = self.make_log()
        self.assertEqual([r["payload"] for r in log.fetch(0, 0)["records"]],
                         [b"for-p0"])
        self.assertEqual([r["payload"] for r in log.fetch(1, 0)["records"]],
                         [b"for-p1"])
        st = log.txn_state(1)
        self.assertEqual(st["state"], "committed")
        self.assertEqual(sorted(st["partitions"]), [0, 1])

    def test_crash_between_partition_appends(self):
        # process dies after the p0 append is durable but before the p1
        # append: the transaction can never become visible anywhere.
        code = snippet(f"""
log = txlog.open_log({self.dir!r}, segment_bytes=512, dedup_window=8)
p = log.producer("p")
p.begin()
p.append(0, b"only-p0", seq=0)
p.append(1, b"never-written", seq=0)
p.commit()
""")
        cp = run_py(code, env_extra={"TXLOG_FAULT": "append.after_meta:1"},
                    expect=None)
        self.assertEqual(cp.returncode, CRASH)
        log = self.make_log()
        p = log.producer("p")  # fences the half-written txn
        self.assertEqual(log.fetch(0, 0)["records"], [])
        self.assertEqual(log.fetch(1, 0)["records"], [])
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["txn_status"] for r in page["records"]],
                         ["aborted"])
        # new epoch txn writes both partitions and commits
        p.begin()
        p.append(0, b"new-p0", seq=0)
        p.append(1, b"new-p1", seq=0)
        p.commit()
        self.assertEqual([r["offset"] for r in log.fetch(0, 0)["records"]],
                         [1])
        self.assertEqual([r["offset"] for r in log.fetch(1, 0)["records"]],
                         [0])

    def test_crash_during_retain_is_finished_by_recovery(self):
        run_cli(["init", self.dir, "--segment-bytes", "200"])
        for i in range(12):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", f"rec-{i}"])
        # crash after catalog says "deleted" but before unlink
        rc, _, _ = run_cli(
            ["retain", self.dir, "--partition", "0", "--before-offset", "6"],
            env_extra={"TXLOG_FAULT": "retain.after_delete"}, expect=None)
        self.assertEqual(rc, CRASH)
        # recovery finishes the deletion; data below the frontier is gone
        page = run_cli_json(["fetch", self.dir, "--partition", "0",
                             "--offset", "0"])
        self.assertEqual(page["reason"], "retained_boundary")
        self.assertEqual(page["retained_below"], 6)
        page = run_cli_json(["fetch", self.dir, "--partition", "0",
                             "--offset", "6"])
        self.assertEqual(page["records"][0]["payload_text"], "rec-6")

    def test_producer_epoch_change_via_cli(self):
        run_cli(["init", self.dir])
        run_cli_json(["open-producer", self.dir, "p"])
        run_cli_json(["begin", self.dir, "p", "--epoch", "1"])
        run_cli_json(["append", self.dir, "p", "--epoch", "1",
                      "--partition", "0", "--seq", "0", "--data", "old"])
        out = run_cli_json(["open-producer", self.dir, "p"])
        self.assertEqual(out["epoch"], 2)
        self.assertEqual(out["aborted_transactions"], 1)
        rc, _, err = run_cli(["commit", self.dir, "p", "--epoch", "1"],
                             expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("ProducerFenced", err)
        page = run_cli_json(["fetch", self.dir, "--partition", "0",
                             "--isolation", "read_uncommitted"])
        self.assertEqual(page["records"][0]["txn_status"], "aborted")

    def test_unreturned_commit_can_be_confirmed_by_retry(self):
        # commit decision durable, receipt lost; the client confirms via
        # txn-state and an idempotent commit retry.
        code = snippet(f"""
log = txlog.open_log({self.dir!r})
p = log.producer("p")
t = p.begin()
p.append(0, b"x", seq=0)
p.commit()
""")
        cp = run_py(code, env_extra={"TXLOG_FAULT": "commit.after_decision"},
                    expect=None)
        self.assertEqual(cp.returncode, CRASH)
        st = run_cli_json(["txn-state", self.dir, "--txn-id", "1"])
        self.assertEqual(st["state"], "committed")
        # idempotent retry of the same commit succeeds
        out = run_cli_json(["commit", self.dir, "p", "--epoch", "1",
                            "--txn-id", "1"])
        self.assertEqual(out["state"], "committed")


if __name__ == "__main__":
    unittest.main()
