"""CLI tests for the named-reader verbs."""

import unittest

from tests.helpers import LogTestCase, run_cli, run_cli_json


class TestReaderCli(LogTestCase):
    def test_reader_lifecycle_via_cli(self):
        run_cli_json(["init", self.dir, "--segment-bytes", "300"])
        for i in range(6):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", f"rec-{i}"])
        # create two readers with different starts
        out = run_cli_json(["reader-create", self.dir, "r1"])
        self.assertEqual((out["reused"], out["start_mode"]),
                         (False, "boundary"))
        out = run_cli_json(["reader-create", self.dir, "r2",
                            "--start-offset", "2"])
        self.assertEqual((out["reused"], out["start_mode"]),
                         (False, "offset"))
        # idempotent reuse vs explicit conflict
        out = run_cli_json(["reader-create", self.dir, "r2",
                            "--start-offset", "2"])
        self.assertTrue(out["reused"])
        rc, _, err = run_cli(
            ["reader-create", self.dir, "r2", "--start-offset", "5"],
            expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("ReaderConflict", err)

        # poll + out-of-order ack
        page = run_cli_json(["reader-poll", self.dir, "r2",
                             "--partition", "0", "--max-records", "10"])
        self.assertEqual([r["offset"] for r in page["records"]],
                         [2, 3, 4, 5])
        self.assertEqual(page["reader_id"], "r2")
        self.assertTrue(all(r["partition"] == 0
                            for r in page["records"]))
        ack = run_cli_json(["reader-ack", self.dir, "r2",
                            "--partition", "0", "--offset", "3"])
        self.assertEqual(ack["safe_offset"], 2)  # offset 2 still unacked
        # duplicate and illegal acks
        dup = run_cli_json(["reader-ack", self.dir, "r2",
                            "--partition", "0", "--offset", "3"])
        self.assertTrue(dup["duplicate"])
        rc, _, err = run_cli(
            ["reader-ack", self.dir, "r2", "--partition", "0",
             "--offset", "99"], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("ReaderAckError", err)
        self.assertIn("beyond_stable", err)

        # close the CLI process (each command is its own process): the
        # next process sees durable progress and redelivers unacked data
        run_cli_json(["reader-ack", self.dir, "r2",
                      "--partition", "0", "--offset", "2"])
        st = run_cli_json(["reader-status", self.dir, "r2",
                           "--partition", "0"])
        self.assertEqual(st["partitions"]["0"]["safe_offset"], 4)
        page = run_cli_json(["reader-poll", self.dir, "r2",
                             "--partition", "0"])
        # fresh process handle resumes at safe cursor: 4,5 redelivered
        self.assertEqual([r["offset"] for r in page["records"]], [4, 5])

        # registry + diagnostics
        out = run_cli_json(["readers", self.dir])
        self.assertEqual(set(out), {"r1", "r2"})
        info = run_cli_json(["inspect", self.dir, "--partition", "0"])
        self.assertEqual(info["readers"]["r2"]["partitions"]["0"]
                         ["safe_offset"], 4)

        # retain is pinned by the slowest reader; delete releases it
        res = run_cli_json(["retain", self.dir, "--partition", "0",
                            "--before-offset", "100"])
        self.assertEqual(res["constrained_by"], "reader")
        rep = run_cli_json(["reader-delete", self.dir, "r2"])
        self.assertIn("retention_effect", rep)
        out = run_cli_json(["readers", self.dir])
        self.assertNotIn("r2", out)
        # stale identity is rejected
        rc, _, err = run_cli(
            ["reader-poll", self.dir, "r2", "--partition", "0"],
            expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("UnknownReader", err)

    def test_unavailable_boundary_is_structured_error(self):
        run_cli_json(["init", self.dir, "--segment-bytes", "200"])
        for i in range(20):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", f"x{i}"])
        run_cli_json(["retain", self.dir, "--partition", "0",
                      "--before-offset", "10"])
        run_cli_json(["reader-create", self.dir, "late",
                      "--start-offset", "0"])
        rc, out, err = run_cli(
            ["reader-poll", self.dir, "late", "--partition", "0"],
            expect=None)
        self.assertEqual(rc, 1)
        import json
        detail = json.loads(err)
        self.assertEqual(detail["error"], "ReaderDataUnavailable")
        self.assertEqual(detail["retained_below"], 10)
        self.assertEqual(detail["requested_offset"], 0)
        # explicit recovery at the boundary works
        run_cli_json(["reader-replay", self.dir, "late",
                      "--partition", "0", "--offset", "10"])
        page = run_cli_json(["reader-poll", self.dir, "late",
                             "--partition", "0", "--max-records", "2"])
        self.assertEqual([r["offset"] for r in page["records"]], [10, 11])


if __name__ == "__main__":
    unittest.main()
