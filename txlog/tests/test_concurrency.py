"""Process-level mutual exclusion and cross-process behavior."""

import unittest

from tests.helpers import LogTestCase, run_cli, run_cli_json


class TestProcessMutualExclusion(LogTestCase):
    def test_single_writer_domain_across_processes(self):
        log = self.make_log()          # this process holds the writer lock
        log.append(0, b"mine")
        # another process trying to write times out on the writer lock
        rc, _, err = run_cli(
            ["append", self.dir, "--partition", "0", "--data", "theirs"],
            env_extra={"TXLOG_LOCK_TIMEOUT": "1"}, expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("writer lock", err)
        # ... but a read-only fetch from another process works concurrently
        page = run_cli_json(["fetch", self.dir, "--partition", "0"])
        self.assertEqual(page["records"][0]["payload_text"], "mine")
        # after we close, the other process can write
        log.close()
        out = run_cli_json(["append", self.dir, "--partition", "0",
                            "--data", "theirs"],
                           env_extra={"TXLOG_LOCK_TIMEOUT": "5"})
        self.assertEqual(out["offset"], 1)

    def test_readonly_open_needs_existing_log(self):
        # missing directory entirely
        rc, _, err = run_cli(["fetch", self.dir + "-missing",
                              "--partition", "0"], expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("does not exist", err)
        # directory exists but was never initialized as a txlog
        import os
        os.makedirs(self.dir)
        rc, _, err = run_cli(["fetch", self.dir, "--partition", "0"],
                             expect=None)
        self.assertEqual(rc, 1)
        self.assertIn("not an initialized txlog", err)


if __name__ == "__main__":
    unittest.main()
