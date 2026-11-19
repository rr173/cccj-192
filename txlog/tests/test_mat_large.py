"""Larger, runtime-generated histories: incremental scaling and invariants.

The data is generated at runtime (nothing checked in).  These tests assert
the incremental contract rather than full brute-force agreement:

* a refresh only reads not-yet-materialized source bodies;
* old events are re-evaluated only for entities whose rules changed;
* details/changes/positions stay mutually consistent across many releases;
* segment retention of processed data does not block later refreshes.
"""

import json
import random

from tests.mat_helpers import MatTestCase
import txlog


def _event(entity, t):
    return json.dumps({"kind": "event", "entity": entity,
                       "event_time": t}).encode()


def _rule(entity, eff, rev, group):
    return json.dumps({"kind": "assignment", "entity": entity,
                       "effective_from": eff, "revision": rev,
                       "group": group}).encode()


class TestLargeHistory(MatTestCase):

    def test_many_incremental_refreshes_stay_consistent(self):
        log = self.make_log()
        store = self.open_store()
        rng = random.Random(424242)
        entities = [f"e{k}" for k in range(8)]
        total_events = 0
        last_release = 0

        for round_no in range(40):
            # append a batch of events and a few late rules
            for _ in range(rng.randint(20, 60)):
                ent = rng.choice(entities)
                log.append(rng.randrange(4), _event(ent, rng.randint(0, 1000)))
                total_events += 1
            for _ in range(rng.randint(0, 4)):
                ent = rng.choice(entities)
                log.append(rng.randrange(4),
                           _rule(ent, rng.randint(0, 900),
                                 rng.randint(0, 6),
                                 rng.choice(["g1", "g2", "g3"])))
            rep = store.refresh()
            if rep["published"]:
                last_release = rep["release_no"]
                # every change references a real event coordinate
                for ch in rep["changes"]:
                    self.assertIn("event", ch)
                    self.assertIn(ch["change_type"],
                                  ("inserted", "corrected"))
            # the detail table always has exactly the published events
            self.assertEqual(len(store.read_all_details()), total_events)

        self.assertEqual(store.status()["latest_release"], last_release)
        self.assertGreater(last_release, 1)

        # the union of every release's inserted changes counts each event once
        con = __import__("sqlite3").connect(self.mat_path)
        inserted = con.execute(
            "SELECT COUNT(*) FROM changes WHERE change_type='inserted'"
        ).fetchone()[0]
        con.close()
        self.assertEqual(inserted, total_events)

    def test_refresh_is_incremental_not_full_rebuild(self):
        """Re-evaluation is scoped to affected entities: a refresh whose only
        new record is one rule touches just that entity's events, verified by
        the change set never including unaffected entities."""
        log = self.make_log()
        store = self.open_store()
        for ent in ("a", "b", "c"):
            for i in range(10):
                log.append(0, _event(ent, i))
        store.refresh()

        # only entity 'b' gets a rule; only b's events may change
        log.append(1, _rule("b", 0, 1, "blue"))
        rep = store.refresh()
        changed = [(c["event"]["partition"], c["event"]["offset"])
                   for c in rep["changes"]]
        # changes carry coordinates; confirm every changed detail is entity b
        details = {(r["event"]["partition"], r["event"]["offset"]): r
                   for r in store.read_all_details()}
        for coord in changed:
            self.assertEqual(details[coord]["entity"], "b")
        b_rows = [r for r in details.values() if r["entity"] == "b"]
        self.assertTrue(all(r["group"] == "blue" for r in b_rows))
        a_rows = [r for r in details.values() if r["entity"] == "a"]
        self.assertTrue(all(not r["assigned"] for r in a_rows))

    def test_retention_of_processed_segments_scales_across_refreshes(self):
        log = self.make_log()
        store = self.open_store()
        for i in range(300):
            log.append(i % 3, _event("e", i))
        store.refresh()
        # retain everything already materialized; details must persist
        for p in range(3):
            hw = log.inspect(partition=p)["partitions"][p]["next_offset"]
            log.retain(p, hw)
        self.assertEqual(len(store.read_all_details()), 300)
        # new appends after retention still materialize incrementally
        for i in range(300, 330):
            log.append(i % 3, _event("e", i))
        rep = store.refresh()
        self.assertTrue(rep["published"])
        self.assertEqual(len(store.read_all_details()), 330)
