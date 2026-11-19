"""Handwritten-history acceptance test for local incremental materialization.

This is the exact scenario from the specification, asserting both position
semantics (transaction-atomic prefix vectors across three partitions) and
business-time semantics (late rules re-assign past events; ingest order is
never confused with business time):

  three partitions, two positions each
  P0: standalone event E20 (off 0), transaction A's event E9 (off 1)
  P1: transaction B's rule effective 10 -> blue (off 0),
      transaction A's rule effective 20 -> gold (off 1)
  P2: an open transaction's record (off 0), transaction B's event E12 (off 1)
  A, B committed.

  first refresh, stable upper (2, 2, 0): publish only (1, 0, 0), just E20
  and unassigned; after the open transaction aborts, advance to (2, 2, 2)
  with gold E20, unassigned E9, blue E12.
"""

import json

from tests.mat_helpers import (MatTestCase, assignment, build_hand_history,
                               event)
import txlog


def detail_by_coord(rows):
    return {(r["event"]["partition"], r["event"]["offset"]): r
            for r in rows}


class TestHandHistory(MatTestCase):

    def test_two_position_semantics_never_mix(self):
        log = self.make_log()
        open_producer = build_hand_history(log)
        store = self.open_store()

        # -- refresh 1: upper (2,2,0), only (1,0,0) may publish -------------
        r1 = store.refresh()
        self.assertTrue(r1["published"])
        self.assertEqual(r1["release_no"], 1)
        self.assertEqual(r1["positions"], {0: 1, 1: 0, 2: 0})
        self.assertEqual(r1["stable_upper"], {0: 2, 1: 2, 2: 0})
        # every safe prefix is published: P0 advanced despite P2 being stuck
        deferred = {d["partition"]: d for d in r1["deferred"]}
        self.assertEqual(set(deferred), {0, 1})
        for d in deferred.values():
            self.assertEqual(d["reason"], "pending_transaction")

        rows = store.read_all_details()
        self.assertEqual(len(rows), 1)
        e20 = rows[0]
        self.assertEqual(e20["event"], {"partition": 0, "offset": 0})
        self.assertEqual(e20["event_time"], 20)
        self.assertFalse(e20["assigned"])
        self.assertIsNone(e20["group"])
        self.assertIsNone(e20["rule"])
        # exactly one inserted change, no corrections, in release 1
        ch1 = store.read_changes(1)["rows"]
        self.assertEqual(len(ch1), 1)
        self.assertEqual(ch1[0]["change_type"], "inserted")

        # -- abort the open transaction; refresh 2 reaches (2,2,2) ----------
        open_producer.abort()
        r2 = store.refresh()
        self.assertTrue(r2["published"])
        self.assertEqual(r2["release_no"], 2)
        self.assertEqual(r2["positions"], {0: 2, 1: 2, 2: 2})
        self.assertEqual(r2["deferred"], [])
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(set(details), {(0, 0), (0, 1), (2, 1)})

        gold = details[(0, 0)]
        self.assertTrue(gold["assigned"])
        self.assertEqual(gold["group"], "gold")
        self.assertEqual(gold["rule"]["effective_from"], 20)
        self.assertEqual(gold["rule"]["revision"], 1)
        self.assertEqual(gold["rule"]["source"],
                         {"partition": 1, "offset": 1})

        e9 = details[(0, 1)]
        self.assertFalse(e9["assigned"])
        self.assertIsNone(e9["rule"])

        blue = details[(2, 1)]
        self.assertEqual(blue["group"], "blue")
        self.assertEqual(blue["rule"]["effective_from"], 10)
        self.assertEqual(blue["rule"]["source"],
                         {"partition": 1, "offset": 0})

        # release-2 changes: two inserts + one correction of E20
        changes = store.read_changes(2)["rows"]
        by_event = {(c["event"]["partition"], c["event"]["offset"]): c
                    for c in changes}
        self.assertEqual(set(by_event), {(0, 1), (2, 1), (0, 0)})
        self.assertEqual(by_event[(0, 1)]["change_type"], "inserted")
        self.assertEqual(by_event[(2, 1)]["change_type"], "inserted")
        self.assertEqual(by_event[(0, 0)]["change_type"], "corrected")
        self.assertFalse(by_event[(0, 0)]["before"]["assigned"])
        self.assertEqual(by_event[(0, 0)]["after"]["group"], "gold")

    def test_late_rule_only_corrects_matching_past_events(self):
        log = self.make_log()
        pc = build_hand_history(log)
        store = self.open_store()
        store.refresh()
        pc.abort()
        store.refresh()

        # a rule effective at business time 8 arrives long after the events
        log.append(0, assignment("x", 8, 1, "red"))
        r3 = store.refresh()
        self.assertEqual(r3["release_no"], 3)
        # ONLY E9 (event_time 9) changes; E20 stays gold, E12 stays blue
        self.assertEqual(r3["change_count"], 1)
        change = r3["changes"][0]
        self.assertEqual(change["event"], {"partition": 0, "offset": 1})
        self.assertEqual(change["change_type"], "corrected")
        self.assertFalse(change["before"]["assigned"])
        self.assertEqual(change["after"]["group"], "red")
        self.assertEqual(change["after"]["rule"]["effective_from"], 8)
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(details[(0, 0)]["group"], "gold")
        self.assertEqual(details[(2, 1)]["group"], "blue")
        self.assertEqual(details[(0, 1)]["group"], "red")

        # a no-op refresh publishes nothing and keeps release 3's changes
        r4 = store.refresh()
        self.assertFalse(r4["published"])
        self.assertEqual(r4["release_no"], 3)
        self.assertEqual(len(store.read_changes(3)["rows"]), 1)

    def test_high_and_low_revision_equality_and_coverage_boundaries(self):
        log = self.make_log()
        pc = build_hand_history(log)
        store = self.open_store()
        store.refresh()
        pc.abort()
        store.refresh()

        # 1. a HIGHER revision at the same effective time replaces the rule;
        #    E12 (time 12) moves off blue.
        log.append(1, assignment("x", 10, 2, "teal"))
        store.refresh()
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(details[(2, 1)]["group"], "teal")
        self.assertEqual(details[(2, 1)]["rule"]["revision"], 2)

        # 2. a LOWER revision at the SAME effective time is ignored in favour
        #    of the higher revision already in force.  The position advances
        #    (the record was ingested) but it produces no net changes.
        log.append(1, assignment("x", 10, 0, "low-rev-ignored"))
        r = store.refresh()
        self.assertTrue(r["published"])
        self.assertEqual(r["change_count"], 0)
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(details[(2, 1)]["group"], "teal")
        self.assertEqual(details[(2, 1)]["rule"]["revision"], 2)

        # 2b. same (entity, eff, revision) with a DIFFERENT group is an
        #     unresolvable conflict.  It is exercised LAST: a rejected record
        #     stays unprocessed in the source and would block later refreshes
        #     until resolved, exactly as a poisoned message should.
        # 3. boundary equality: a rule whose effective_from EQUALS the
        #    event_time is eligible ("not later than").
        log.append(0, assignment("x", 12, 5, "equal"))
        store.refresh()
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(details[(2, 1)]["group"], "equal")
        self.assertEqual(details[(2, 1)]["rule"]["effective_from"], 12)

        # 4. a higher-revision rule at E20's exact effective time overrides
        #    gold for E20 but never reaches E12 (time 12).
        log.append(0, assignment("x", 20, 9, "mid"))
        store.refresh()
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(details[(0, 0)]["group"], "mid")
        self.assertEqual(details[(0, 0)]["rule"]["effective_from"], 20)
        self.assertEqual(details[(0, 0)]["rule"]["revision"], 9)
        self.assertEqual(details[(2, 1)]["group"], "equal")

        # 5. a rule later than every event time assigns nothing new.
        before = detail_by_coord(store.read_all_details())
        log.append(0, assignment("x", 1000, 1, "future"))
        r = store.refresh()
        self.assertTrue(r["published"])
        self.assertEqual(r["change_count"], 0)
        self.assertEqual(detail_by_coord(store.read_all_details()), before)

        # 6. an unresolvable version conflict rejects the whole round and
        #    leaves the previous release fully readable.
        log.append(1, assignment("x", 10, 2, "conflicting-teal"))
        with self.assertRaises(txlog.RuleConflictError):
            store.refresh()
        details = detail_by_coord(store.read_all_details())
        self.assertEqual(details[(2, 1)]["group"], "equal")

    def test_duplicate_rule_version_keeps_smallest_location(self):
        log = self.make_log()
        store = self.open_store()
        # same (entity, eff, rev, group) from two coordinates: smallest wins
        log.append(0, assignment("e", 5, 1, "g"))
        log.append(2, assignment("e", 5, 1, "g"))
        log.append(1, event("e", 9))
        store.refresh()
        row = detail_by_coord(store.read_all_details())[(1, 0)]
        self.assertEqual(row["group"], "g")
        self.assertEqual(row["rule"]["source"],
                         {"partition": 0, "offset": 0})

    def test_rule_conflict_rejects_the_whole_round_atomically(self):
        log = self.make_log()
        store = self.open_store()
        log.append(0, assignment("e", 5, 1, "g1"))
        log.append(1, assignment("e", 5, 1, "g2"))
        log.append(2, event("e", 9))
        with self.assertRaises(txlog.RuleConflictError) as cm:
            store.refresh()
        err = cm.exception
        self.assertEqual(err.entity, "e")
        self.assertEqual(err.effective_from, 5)
        self.assertEqual(err.revision, 1)
        groups = {loc["group"] for loc in err.locations}
        self.assertEqual(groups, {"g1", "g2"})
        # nothing was published: no release, no details, no progress
        self.assertIsNone(store.status()["latest_release"])
        self.assertEqual(store.read_all_details(), [])
        # a later refresh still starts from zero and fails the same way
        with self.assertRaises(txlog.RuleConflictError):
            store.refresh()

    def test_identical_bodies_at_distinct_coordinates_are_two_events(self):
        log = self.make_log()
        store = self.open_store()
        body = event("dup", 7)
        log.append(0, body)
        log.append(1, body)
        store.refresh()
        coords = sorted((r["event"]["partition"], r["event"]["offset"])
                        for r in store.read_all_details())
        self.assertEqual(coords, [(0, 0), (1, 0)])
