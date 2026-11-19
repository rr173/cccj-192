"""CLI surface for the materialization commands."""

import json

from tests.helpers import run_cli, run_cli_json
from tests.mat_helpers import MatTestCase, assignment, event


class TestMatCli(MatTestCase):

    def test_full_cli_lifecycle(self):
        run_cli_json(["init", self.dir, "--segment-bytes",
                      str(self.SEGMENT_BYTES)])
        run_cli_json(["append", self.dir, "--partition", "0",
                      "--data", json.dumps(
                          {"kind": "event", "entity": "e",
                           "event_time": 5})])

        st = run_cli_json(["mat-open", self.mat_path, self.dir])
        self.assertIsNone(st["latest_release"])
        self.assertEqual(st["positions"], {})

        rep = run_cli_json(["mat-refresh", self.mat_path])
        self.assertTrue(rep["published"])
        self.assertEqual(rep["release_no"], 1)

        run_cli_json(["append", self.dir, "--partition", "0",
                      "--data", json.dumps(
                          {"kind": "assignment", "entity": "e",
                           "effective_from": 0, "revision": 1,
                           "group": "g"})])
        rep2 = run_cli_json(["mat-refresh", self.mat_path])
        self.assertEqual(rep2["release_no"], 2)

        details = run_cli_json(["mat-details", self.mat_path,
                                "--limit", "1"])
        self.assertEqual(len(details["rows"]), 1)
        self.assertTrue(details["has_more"])
        self.assertIn("next_cursor", details)

        changes = run_cli_json(["mat-changes", self.mat_path,
                                "--release", "2"])
        self.assertEqual(changes["release_no"], 2)
        self.assertEqual(changes["rows"][0]["after"]["group"], "g")

    def test_cli_reports_conflict_as_structured_error(self):
        run_cli_json(["init", self.dir])
        run_cli_json(["mat-open", self.mat_path, self.dir])
        run_cli_json(["append", self.dir, "--partition", "0",
                      "--data", json.dumps(
                          {"kind": "assignment", "entity": "e",
                           "effective_from": 1, "revision": 1,
                           "group": "a"})])
        run_cli_json(["append", self.dir, "--partition", "1",
                      "--data", json.dumps(
                          {"kind": "assignment", "entity": "e",
                           "effective_from": 1, "revision": 1,
                           "group": "b"})])
        rc, out, err = run_cli(["mat-refresh", self.mat_path], expect=1)
        doc = json.loads(err)
        self.assertEqual(doc["error"], "RuleConflictError")
        self.assertEqual(doc["entity"], "e")
        self.assertEqual(doc["effective_from"], 1)
        self.assertEqual(doc["revision"], 1)
        self.assertEqual(len(doc["locations"]), 2)

    def test_cli_reports_history_gap_with_range(self):
        run_cli_json(["init", self.dir])
        run_cli_json(["mat-open", self.mat_path, self.dir])
        for i in range(5):
            run_cli_json(["append", self.dir, "--partition", "0",
                          "--data", json.dumps(
                              {"kind": "event", "entity": "e",
                               "event_time": i})])
        run_cli_json(["retain", self.dir, "--partition", "0",
                      "--before-offset", "5"])
        rc, out, err = run_cli(["mat-refresh", self.mat_path], expect=1)
        doc = json.loads(err)
        self.assertEqual(doc["error"], "HistoryGapError")
        self.assertEqual(doc["partition"], 0)
        self.assertEqual(doc["missing_to"], 5)
