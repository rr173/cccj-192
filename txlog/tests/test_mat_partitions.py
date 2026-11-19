"""Partition lifecycle and the no-payload-mirror guarantee."""

import os
import sqlite3

from tests.mat_helpers import MatTestCase, assignment, event
import txlog


class TestPartitionsAndStorage(MatTestCase):

    def test_partitions_appearing_later_are_adopted(self):
        log = self.make_log()
        store = self.open_store()
        # the source starts with only partition 0
        log.append(0, event("e", 1))
        r = store.refresh()
        self.assertEqual(r["positions"], {0: 1})

        # partitions 1 and 2 appear later; the next refresh adopts them from 0
        log.append(1, event("e", 2))
        log.append(2, assignment("e", 0, 1, "g"))
        r = store.refresh()
        self.assertEqual(r["positions"], {0: 1, 1: 1, 2: 1})
        # the new event is assigned using the rule that arrived alongside it
        rows = {(d["event"]["partition"], d["event"]["offset"]): d
                for d in store.read_all_details()}
        self.assertEqual(rows[(1, 0)]["group"], "g")

        # a still-later partition is picked up on the following refresh
        log.append(5, event("e", 3))
        r = store.refresh()
        self.assertEqual(r["positions"][5], 1)

    def test_record_bodies_are_never_mirrored_into_the_mat_db(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, event("entity-with-long-name", 7))
        log.append(0, assignment("entity-with-long-name", 0, 1,
                                 "group-with-long-name"))
        store.refresh()
        con = sqlite3.connect(self.mat_path)
        try:
            tables = {r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            # no table may hold raw source payloads; only parsed facts
            self.assertNotIn("payloads", tables)
            for table in tables:
                cols = [r[1] for r in con.execute(
                    f"PRAGMA table_info({table})")]
                self.assertNotIn("payload", cols)
                self.assertNotIn("body", cols)
            # the only stored strings are parsed business fields, never JSON
            n_json = con.execute(
                "SELECT COUNT(*) FROM assignments WHERE grp LIKE '{%' "
                "OR entity LIKE '{%'").fetchone()[0]
            self.assertEqual(n_json, 0)
        finally:
            con.close()

    def test_source_metadata_db_is_not_modified_by_refresh(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, event("e", 1))
        meta = os.path.join(self.dir, "meta.sqlite3")
        before = os.path.getmtime(meta)
        # a read-only refresh must not write the source metadata DB
        store.refresh()
        after = os.path.getmtime(meta)
        self.assertEqual(before, after)
        # and the source log still serves the raw record unchanged
        page = log.fetch(0, 0)
        self.assertEqual(len(page["records"]), 1)
