"""Shared helpers for the local incremental materialization tests."""

import json

from tests.helpers import LogTestCase


def jbody(obj):
    return json.dumps(obj, sort_keys=True).encode("utf-8")


def event(entity, event_time):
    return jbody({"kind": "event", "entity": entity,
                  "event_time": event_time})


def assignment(entity, effective_from, revision, group):
    return jbody({"kind": "assignment", "entity": entity,
                  "effective_from": effective_from, "revision": revision,
                  "group": group})


def build_hand_history(log, entity="x"):
    """The handwritten history from the materialization specification.

    Three partitions, two positions each (positions are offsets):

      P0: offset 0 standalone event E20; offset 1 transaction A's event E9.
      P1: offset 0 transaction B's rule (effective 10, blue);
          offset 1 transaction A's rule (effective 20, gold).
      P2: offset 0 a still-OPEN transaction's record;
          offset 1 transaction B's event E12.

    A and B are committed; the open transaction is returned so the test can
    abort it.  All three events belong to one entity and use their numeric
    suffix as event_time.
    """
    # The standalone event must be appended before any producer is opened
    # (opening a producer does not change offsets, but this keeps E20 at
    # partition 0 offset 0 exactly as the hand history specifies).
    log.append(0, event(entity, 20))                       # P0 off 0: E20

    pa = log.producer("A")
    pb = log.producer("B")
    pc = log.producer("C")
    # All producer handles must exist before any begin(): opening a producer
    # fences (aborts the open transactions of) any previously opened one.

    pa.begin()
    pa.append(0, event(entity, 9), seq=0)                  # P0 off 1: A E9

    pb.begin()
    pb.append(1, assignment(entity, 10, 1, "blue"), seq=0)  # P1 off 0: B rule

    # The open transaction's record is P2 offset 0, ahead of B's P2 record;
    # it pins partition 2's stable frontier at 0 until the abort.
    pc.begin()
    pc.append(2, event(entity, 100), seq=0)               # P2 off 0: open

    # A's P1 record lands after B's, at P1 offset 1, exactly as specified.
    pa.append(1, assignment(entity, 20, 1, "gold"), seq=0)  # P1 off 1: A rule
    pb.append(2, event(entity, 12), seq=0)                # P2 off 1: B E12

    pa.commit()
    pb.commit()
    return pc  # C remains open


# ---------------------------------------------------------------------------
# Independent brute-force reference model
#
# This model deliberately re-derives everything from scratch on every step:
# it enumerates legal prefix vectors directly and re-evaluates every selected
# event against the full selected rule set.  It shares no code with the
# incremental implementation, so agreement is a real cross-check.
# ---------------------------------------------------------------------------

class RefModel:
    def __init__(self):
        # coordinates -> parsed records
        self.events = {}        # (p, off) -> (entity, event_time)
        self.assignments = {}   # (p, off) -> (entity, eff, rev, group)
        # coordinate -> txn id (or None for non-transactional)
        self.txn_of = {}
        self.txn_state = {}     # txn id -> 'open'|'committed'|'aborted'
        self.txn_parts = {}     # txn id -> {p: (first, last)}
        self.hw = {}            # p -> next offset
        self.retained = {}      # p -> retained_below
        # expected published releases
        self.positions = {}     # p -> published exclusive offset
        self.details = {}       # (p, off) -> detail tuple or None
        self.releases = []      # list of (positions, changes dict)

    def observe_snapshot(self, partitions, hw, retained, txn_state,
                         txn_parts, records):
        """Replace the model's view of the source with a fresh snapshot."""
        self.hw = dict(hw)
        self.retained = dict(retained)
        self.txn_state = dict(txn_state)
        self.txn_parts = {t: dict(parts) for t, parts in txn_parts.items()}
        self.txn_of = {}
        self.events = {}
        self.assignments = {}
        for (p, off), rec in records.items():
            self.txn_of[(p, off)] = rec["txn_id"]
            doc = rec["doc"]
            if doc["kind"] == "event":
                self.events[(p, off)] = (doc["entity"], doc["event_time"])
            else:
                self.assignments[(p, off)] = (
                    doc["entity"], doc["effective_from"], doc["revision"],
                    doc["group"])
        self.partitions = sorted(partitions)

    def stable_upper(self):
        upper = {}
        for p in self.partitions:
            starts = [rng[0] for t, parts in self.txn_parts.items()
                      if self.txn_state.get(t) == "open"
                      for pp, rng in parts.items() if pp == p]
            upper[p] = min(starts) if starts else self.hw[p]
        return upper

    def _vector_is_legal(self, vec):
        """A vector is legal iff every committed transaction is either fully
        visible (every part's first offset below its partition prefix) or
        fully hidden (every part at/above the prefix)."""
        for t, parts in self.txn_parts.items():
            if self.txn_state.get(t) != "committed":
                continue
            visibility = set()
            for p, (first, last) in parts.items():
                visibility.add("in" if first < vec.get(p, 0) else "out")
            if len(visibility) > 1:
                return False
        return True

    def legal_prefixes(self, upper):
        """Enumerate every complete vector positions<=v<=upper that is
        txn-atomic.  Validation deliberately runs only on COMPLETE vectors:
        checking a transaction against a half-assigned vector would create
        spurious splits between visited and not-yet-visited partitions."""
        results = []
        starts = {p: self.positions.get(p, 0) for p in self.partitions}

        def recurse(i, vec):
            if i == len(self.partitions):
                if self._vector_is_legal(vec):
                    results.append(dict(vec))
                return
            p = self.partitions[i]
            for v in range(starts[p], upper[p] + 1):
                vec[p] = v
                recurse(i + 1, vec)

        recurse(0, {})
        return results

    def expected_vector(self, upper):
        """The component-wise maximum legal prefix vector."""
        legal = self.legal_prefixes(upper)
        best = {p: self.positions.get(p, 0) for p in self.partitions}
        for vec in legal:
            for p in self.partitions:
                best[p] = max(best[p], vec[p])
        return best

    def active_rules(self, selected_assign):
        """entity -> eff -> (revision, group, part, off); raises on conflict."""
        per_entity = {}
        versions = {}
        for (p, off), (entity, eff, rev, grp) in selected_assign.items():
            key = (entity, eff, rev)
            groups = versions.setdefault(key, {})
            cur = groups.get(grp)
            if cur is None or (p, off) < cur:
                groups[grp] = (p, off)
        conflicts = {k: v for k, v in versions.items() if len(v) > 1}
        if conflicts:
            raise AssertionError(f"reference model saw a rule conflict: "
                                 f"{conflicts}")
        for (p, off), (entity, eff, rev, grp) in selected_assign.items():
            per_eff = per_entity.setdefault(entity, {})
            cur = per_eff.get(eff)
            cand = (rev, grp, p, off)
            if cur is None or cand[0] > cur[0] or (
                    cand[0] == cur[0] and (cand[2], cand[3]) <
                    (cur[2], cur[3])):
                per_eff[eff] = cand
        return per_entity

    @staticmethod
    def pick(per_eff, event_time):
        best = None
        for eff in per_eff:
            if eff <= event_time and (best is None or eff > best):
                best = eff
        return None if best is None else (best,) + per_eff[best]

    def expected_refresh(self):
        """Compute the expected release; returns (vec, changes) or None."""
        upper = self.stable_upper()
        vec = self.expected_vector(upper)
        if all(vec.get(p, 0) == self.positions.get(p, 0)
               for p in set(vec) | set(self.positions)):
            return None
        selected_events, selected_assign = {}, {}

        def visible(coord):
            # read_committed: non-transactional records and committed
            # transactions only; aborted records are holes with no detail.
            tid = self.txn_of.get(coord)
            return tid is None or self.txn_state.get(tid) == "committed"

        for (p, off), val in self.events.items():
            if off < vec.get(p, 0) and visible((p, off)):
                selected_events[(p, off)] = val
        for (p, off), val in self.assignments.items():
            if off < vec.get(p, 0) and visible((p, off)):
                selected_assign[(p, off)] = val
        rules = self.active_rules(selected_assign)
        new_details = {}
        for coord in sorted(selected_events):
            entity, etime = selected_events[coord]
            rule = self.pick(rules.get(entity, {}), etime)
            new_details[coord] = None if rule is None else (
                rule[0], rule[1], rule[2], rule[3], rule[4])
        changes = {}
        for coord in sorted(new_details):
            old = self.details.get(coord)
            new = new_details[coord]
            if coord not in self.details:
                changes[coord] = ("inserted", old, new)
            elif old != new:
                changes[coord] = ("corrected", old, new)
        # commit the expected state
        self.positions = {p: vec[p] for p in self.partitions}
        self.details = new_details
        self.releases.append((dict(self.positions), changes))
        return vec, changes


class MatTestCase(LogTestCase):
    """Base class with a source log + separate materialized store path."""

    SEGMENT_BYTES = 2048
    DEDUP_WINDOW = 16

    def setUp(self):
        super().setUp()
        self.mat_path = self.tmp.name and (
            self.tmp.name + "/materialized.sqlite3")

    def open_store(self, **kw):
        import txlog
        store = txlog.open_materialized(self.mat_path, self.dir, **kw)
        if not hasattr(self, "stores"):
            self.stores = []
        self.stores.append(store)
        return store

    def tearDown(self):
        for store in getattr(self, "stores", []):
            try:
                store.close()
            except Exception:
                pass
        super().tearDown()
