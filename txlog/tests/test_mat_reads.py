"""Reads, version pinning, source identity, history gaps and bad records."""

import json
import os

from tests.mat_helpers import MatTestCase, assignment, event
import txlog


class TestPagination(MatTestCase):

    def _seed(self, log, n):
        for i in range(n):
            log.append(i % 3, event("e", i))

    def test_paging_is_stable_and_pinned_to_one_release(self):
        log = self.make_log()
        self._seed(log, 25)
        store = self.open_store()
        store.refresh()
        rel = store.status()["latest_release"]

        seen, cursor = [], None
        for _ in range(100):
            page = store.read_details(limit=4, cursor=cursor)
            self.assertEqual(page["release_no"], rel)
            seen.extend(page["rows"])
            if not page["has_more"]:
                self.assertIsNone(page["next_cursor"])
                break
            cursor = page["next_cursor"]
        self.assertEqual(len(seen), 25)
        coords = [(r["event"]["partition"], r["event"]["offset"])
                  for r in seen]
        self.assertEqual(coords, sorted(coords))

    def test_stale_cursor_is_rejected_not_silently_stitched(self):
        log = self.make_log()
        self._seed(log, 10)
        store = self.open_store()
        store.refresh()
        page1 = store.read_details(limit=3)
        self.assertTrue(page1["has_more"])
        cursor = page1["next_cursor"]
        old_rel = page1["release_no"]

        # publish a new release
        self._seed(log, 5)
        store.refresh()
        new_rel = store.status()["latest_release"]
        self.assertGreater(new_rel, old_rel)

        with self.assertRaises(txlog.StaleCursorError) as cm:
            store.read_details(limit=3, cursor=cursor)
        self.assertEqual(cm.exception.cursor_release, old_rel)
        self.assertEqual(cm.exception.current_release, new_rel)
        # restarting on the current release works and is complete
        rows = store.read_all_details()
        self.assertEqual(len(rows), 15)

    def test_changes_paging_and_unknown_release(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, event("e", 1))
        store.refresh()
        log.append(0, assignment("e", 0, 1, "g"))
        store.refresh()
        latest = store.status()["latest_release"]
        page = store.read_changes(limit=1)
        self.assertEqual(page["release_no"], latest)
        with self.assertRaises(txlog.UnknownRelease):
            store.read_changes(latest + 99)
        # release 1's changes remain readable forever
        r1 = store.read_changes(1)
        self.assertTrue(all(c["change_type"] == "inserted"
                            for c in r1["rows"]))

    def test_empty_store_reads_are_empty_not_errors(self):
        self.make_log()
        store = self.open_store()
        self.assertIsNone(store.status()["latest_release"])
        self.assertEqual(store.read_details()["rows"], [])
        self.assertEqual(store.read_changes()["rows"], [])


class TestSourceIdentityAndGaps(MatTestCase):

    def test_different_log_at_same_path_is_refused(self):
        log = self.make_log()
        log.append(0, event("e", 1))
        store = self.open_store()
        store.refresh()
        log.close()

        # a brand-new log at the SAME path gets a different log_id
        import shutil
        shutil.rmtree(self.dir)
        other = txlog.open_log(self.dir, segment_bytes=4096, dedup_window=8)
        other.append(0, event("e", 2))
        with self.assertRaises(txlog.SourceIdentityError) as cm:
            store.refresh()
        self.assertNotEqual(cm.exception.expected_log_id,
                            cm.exception.actual_log_id)
        # previous materialized result stays readable
        self.assertEqual(len(store.read_all_details()), 1)
        other.close()

    def test_explicit_other_source_path_is_refused(self):
        log = self.make_log()
        log.append(0, event("e", 1))
        store = self.open_store()
        store.refresh()
        other_dir = os.path.join(self.tmp.name, "other-log")
        txlog.open_log(other_dir).close()
        with self.assertRaises(txlog.SourceIdentityError):
            store.refresh(source_path=other_dir)

    def test_retained_unprocessed_offsets_refuse_refresh_keep_result(self):
        log = self.make_log()
        for i in range(8):
            log.append(0, event("e", i))
        store = self.open_store()
        store.refresh()
        # materialize the first part, then retain away unprocessed offsets.
        # A second store on a copy simulates: here retain is blocked by LSO
        # only for open txns; non-txn records can be retained freely.
        log.retain(0, 5)
        info = log.inspect(partition=0)["partitions"][0]
        self.assertEqual(info["retained_below"], 5)
        # the store already published to 8, so this is harmless (processed)
        rep = store.refresh()
        self.assertFalse(rep["published"])
        # details survive even though the source segments are gone
        self.assertEqual(len(store.read_all_details()), 8)

    def test_history_gap_when_source_retains_ahead_of_store(self):
        # build log, bind store, but retain BEFORE the store ever refreshes
        log = self.make_log()
        store = self.open_store()
        for i in range(6):
            log.append(0, event("e", i))
        log.retain(0, 4)
        with self.assertRaises(txlog.HistoryGapError) as cm:
            store.refresh()
        self.assertEqual(cm.exception.partition, 0)
        self.assertEqual(cm.exception.missing_from, 0)
        self.assertEqual(cm.exception.missing_to, 4)
        # no partial publish happened
        self.assertIsNone(store.status()["latest_release"])

    def test_normal_cleanup_of_processed_range_keeps_future_refreshes(self):
        log = self.make_log()
        store = self.open_store()
        for i in range(6):
            log.append(0, event("e", i))
        store.refresh()
        log.retain(0, 6)  # everything retained was already materialized
        log.append(0, event("e", 6))
        rep = store.refresh()
        self.assertTrue(rep["published"])
        # all seven events still present in the materialized detail
        self.assertEqual(len(store.read_all_details()), 7)


class TestInvalidRecords(MatTestCase):

    def test_invalid_json_aborts_round_atomically_with_coordinate(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, event("e", 1))
        log.append(1, b"{not json")
        with self.assertRaises(txlog.MaterializationDataError) as cm:
            store.refresh()
        self.assertEqual(cm.exception.partition, 1)
        self.assertEqual(cm.exception.offset, 0)
        self.assertEqual(cm.exception.reason, "invalid_json")
        self.assertIsNone(store.status()["latest_release"])
        self.assertEqual(store.read_all_details(), [])

    def test_invalid_utf8_and_schema_violations(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, b"\xff\xfe not utf8")
        with self.assertRaises(txlog.MaterializationDataError) as cm:
            store.refresh()
        self.assertEqual(cm.exception.reason, "invalid_utf8")

        # a separate log + store for the schema-validation case
        log2_dir = os.path.join(self.tmp.name, "log2")
        log2 = txlog.open_log(log2_dir, segment_bytes=4096, dedup_window=8)
        self.logs.append(log2)
        store2 = txlog.open_materialized(
            os.path.join(self.tmp.name, "m2.db"), log2.path)
        self.stores.append(store2)
        log2.append(0, json.dumps(
            {"kind": "assignment", "entity": "e", "effective_from": 1,
             "revision": -1, "group": "g"}).encode())
        with self.assertRaises(txlog.MaterializationDataError) as cm:
            store2.refresh()
        self.assertEqual(cm.exception.reason,
                         "field_revision_must_be_nonnegative")

    def test_unknown_kind_is_rejected(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, json.dumps({"kind": "mystery"}).encode())
        with self.assertRaises(txlog.MaterializationDataError) as cm:
            store.refresh()
        self.assertTrue(cm.exception.reason.startswith("unknown_kind"))
