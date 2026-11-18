"""CLI smoke tests: every verb works and emits JSON."""

import unittest

from tests.helpers import LogTestCase, run_cli, run_cli_json


class TestCli(LogTestCase):
    def test_full_lifecycle_via_cli(self):
        out = run_cli_json(["init", self.dir, "--segment-bytes", "300",
                            "--dedup-window", "4"])
        self.assertEqual(out["segment_bytes"], 300)

        out = run_cli_json(["open-producer", self.dir, "p1"])
        self.assertEqual(out["epoch"], 1)
        out = run_cli_json(["begin", self.dir, "p1", "--epoch", "1"])
        txn = out["txn_id"]

        out = run_cli_json(["append", self.dir, "p1", "--epoch", "1",
                            "--partition", "0", "--seq", "0",
                            "--data", "hello"])
        self.assertEqual(out["offset"], 0)
        # retry: same offset, flagged duplicate
        out = run_cli_json(["append", self.dir, "p1", "--epoch", "1",
                            "--partition", "0", "--seq", "0",
                            "--data", "hello"])
        self.assertEqual((out["offset"], out["duplicate"]), (0, True))

        # non-transactional append
        out = run_cli_json(["append", self.dir, "--partition", "0",
                            "--data", "plain"])
        self.assertEqual(out["offset"], 1)

        # pending txn blocks the committed view
        page = run_cli_json(["fetch", self.dir, "--partition", "0"])
        self.assertEqual(page["reason"], "stable_frontier")
        self.assertEqual(page["records"], [])

        run_cli_json(["commit", self.dir, "p1", "--epoch", "1"])
        page = run_cli_json(["fetch", self.dir, "--partition", "0"])
        self.assertEqual([r["offset"] for r in page["records"]], [0, 1])
        self.assertEqual(page["records"][0]["payload_text"], "hello")

        st = run_cli_json(["txn-state", self.dir, "--txn-id", str(txn)])
        self.assertEqual(st["state"], "committed")

        # fenced epoch is rejected with a JSON error on stderr
        rc, _, err = run_cli(["begin", self.dir, "p1", "--epoch", "99"],
                             expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("ProducerFenced", err)

        # retain + inspect
        out = run_cli_json(["retain", self.dir, "--partition", "0",
                            "--before-offset", "10"])
        self.assertIn("applied", out)
        info = run_cli_json(["inspect", self.dir, "--partition", "0"])
        self.assertIn("partitions", info)
        raw = run_cli_json(["inspect", self.dir, "--partition", "0",
                            "--raw"])
        segs = raw["partitions"]["0"]["segments"] \
            if "0" in raw["partitions"] else raw["partitions"][0]
        self.assertTrue(segs)

    def test_inspect_raw_shows_segment_records(self):
        run_cli_json(["init", self.dir])
        run_cli_json(["append", self.dir, "--partition", "2",
                      "--data", "abc"])
        raw = run_cli_json(["inspect", self.dir, "--partition", "2",
                            "--raw"])
        part = raw["partitions"][0] if isinstance(
            raw["partitions"], list) else raw["partitions"]["2"]
        seg = part["segments"][0]
        rec = seg["records"]["records"][0]
        self.assertEqual(rec["offset"], 0)
        self.assertTrue(rec["crc_ok"])
        self.assertEqual(rec["payload_len"], 3)


if __name__ == "__main__":
    unittest.main()
