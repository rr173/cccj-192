"""Independent brute-force model vs. the incremental materialization.

A small, deliberately independent reference implementation (``RefModel`` in
``tests/mat_helpers.py``) re-derives every result from scratch on each step:
it enumerates *all* legal transaction-atomic prefix vectors, takes the
component-wise maximum, and re-evaluates every selected event against the
full selected rule set.  It shares no code with the real implementation.

Random interleaved histories (non-transactional records, cross-partition
committed/aborted/open transactions, late rules, duplicate and conflicting
versions, repeated no-op refreshes and reopens) are replayed against both;
after every step the published vector, every detail row and every net change
must match the reference exactly.
"""

import json
import os
import random

from tests.mat_helpers import MatTestCase, RefModel
import txlog

_BASE_SEED = int(os.environ.get("TXLOG_MAT_SEED", "20261119"))


class TestModelDiff(MatTestCase):

    def _snapshot_source(self, log, model):
        """Feed the model a fresh, independently read source snapshot."""
        info = log.inspect(raw=False)
        partitions = sorted(int(p) for p in info["partitions"])
        hw, retained = {}, {}
        records = {}
        txn_state, txn_parts = {}, {}
        for p in partitions:
            pi = info["partitions"][str(p)] if isinstance(
                next(iter(info["partitions"])), str) else info["partitions"][p]
            hw[p] = pi["next_offset"]
            retained[p] = pi["retained_below"]
        for t in info["transactions"]:
            txn_state[t["txn_id"]] = t["state"]
            txn_parts[t["txn_id"]] = {
                int(q): tuple(rng) for q, rng in t["partitions"].items()}
        # read every currently retained record independently via fetch
        for p in partitions:
            off = retained[p]
            while True:
                page = log.fetch(p, off, max_records=500,
                                 isolation="read_uncommitted", max_scan=4096)
                for rec in page["records"]:
                    doc = json.loads(rec["payload"].decode("utf-8"))
                    records[(p, rec["offset"])] = {
                        "txn_id": rec["txn_id"], "doc": doc}
                off = page["next_offset"]
                if page["reason"] in ("log_end", "stable_frontier"):
                    break
        model.observe_snapshot(partitions, hw, retained, txn_state,
                               txn_parts, records)
        return partitions

    def _model_has_conflict(self, model):
        """True if THIS round's ingest plus already-stored rules conflict.

        Mirrors the store: the batch is the records in (published positions,
        maximal vector], checked against every already-stored version.
        """
        upper = model.stable_upper()
        vec = model.expected_vector(upper)
        # versions already stored below the previous published vector
        stored_versions = {}
        batch_versions = {}
        for (p, off), (entity, eff, rev, grp) in model.assignments.items():
            tid = model.txn_of.get((p, off))
            if tid is not None and model.txn_state.get(tid) != "committed":
                continue
            key = (entity, eff, rev)
            if off < model.positions.get(p, 0):
                stored_versions.setdefault(key, set()).add(grp)
            elif off < vec.get(p, 0):
                batch_versions.setdefault(key, set()).add(grp)
        for key, groups in batch_versions.items():
            if len(groups) > 1 or key in stored_versions and (
                    groups - stored_versions[key]):
                return True
        return False

    def _detail_tuple(self, row):
        # match RefModel detail shape: (eff, rev, group, part, off) or None
        if not row["assigned"]:
            return None
        return (row["rule"]["effective_from"], row["rule"]["revision"],
                row["group"], row["rule"]["source"]["partition"],
                row["rule"]["source"]["offset"])

    def _assert_matches_model(self, store, model, partitions, report=None):
        expected = model.expected_refresh()
        status = store.status()
        if expected is None:
            if report is not None:
                self.assertFalse(report["published"])
            # positions unchanged
            for p in partitions:
                self.assertEqual(status["positions"].get(p, 0),
                                 model.positions.get(p, 0))
            return
        exp_vec, exp_changes = expected
        if report is not None:
            self.assertTrue(report["published"])
            self.assertEqual(report["positions"], exp_vec)
        self.assertEqual(status["positions"],
                         {p: model.positions[p] for p in partitions})
        # every detail row matches the from-scratch evaluation
        actual = {}
        for row in store.read_all_details():
            coord = (row["event"]["partition"], row["event"]["offset"])
            actual[coord] = self._detail_tuple(row)
        self.assertEqual(actual, model.details)
        # net changes match (only real changes; no intermediate values)
        rel = status["latest_release"]
        got_changes = {}
        page = store.read_changes(rel, limit=100)
        while True:
            for c in page["rows"]:
                coord = (c["event"]["partition"], c["event"]["offset"])
                got_changes[coord] = c["change_type"]
            if not page["has_more"]:
                break
            page = store.read_changes(rel, limit=100,
                                      cursor=page["next_cursor"])
        self.assertEqual(got_changes,
                         {coord: kind for coord, (kind, _o, _n)
                          in exp_changes.items()})

    def test_random_histories_match_bruteforce(self):
        n_trials = int(os.environ.get("TXLOG_MAT_TRIALS", "8"))
        for trial in range(n_trials):
            with self.subTest(trial=trial):
                self._run_one(random.Random(_BASE_SEED + trial * 7919))

    def _run_one(self, rng):
        log = self.make_log()
        store = self.open_store()
        model = RefModel()
        n_parts = rng.randint(2, 4)
        entities = [f"e{i}" for i in range(rng.randint(1, 4))]
        producers = {}
        open_pids = set()

        def producer(pid):
            if pid not in producers:
                producers[pid] = log.producer(pid)
            return producers[pid]

        for step in range(rng.randint(40, 70)):
            choice = rng.random()
            ent = rng.choice(entities)
            if choice < 0.45:
                # standalone event/rule
                p = rng.randrange(n_parts)
                if rng.random() < 0.7:
                    log.append(p, json.dumps(
                        {"kind": "event", "entity": ent,
                         "event_time": rng.randint(0, 50)}).encode())
                else:
                    log.append(p, json.dumps({
                        "kind": "assignment", "entity": ent,
                        "effective_from": rng.randint(0, 40),
                        "revision": rng.randint(0, 4),
                        "group": rng.choice(["g1", "g2", "g3"])}).encode())
            elif choice < 0.85:
                # cross-partition transaction; a fresh pid guarantees no
                # stale-open-transaction or sequence bookkeeping
                pid = f"t{step}"
                pr = producer(pid)
                pr.begin()
                open_pids.add(pid)
                seq = {}
                parts = rng.sample(range(n_parts), rng.randint(1, n_parts))
                abort = rng.random() < 0.2
                for p in parts:
                    seq.setdefault(p, 0)
                    if rng.random() < 0.6:
                        pr.append(p, json.dumps({
                            "kind": "event", "entity": ent,
                            "event_time": rng.randint(0, 50)}).encode(),
                            seq=seq[p])
                    else:
                        pr.append(p, json.dumps({
                            "kind": "assignment", "entity": ent,
                            "effective_from": rng.randint(0, 40),
                            "revision": rng.randint(0, 4),
                            "group": rng.choice(["g1", "g2", "g3"])}).encode(),
                            seq=seq[p])
                    seq[p] += 1
                if abort:
                    pr.abort()
                else:
                    pr.commit()
                open_pids.discard(pid)
            else:
                # leave a transaction open to block a stable frontier
                pid = f"open{step}"
                pr = producer(pid)
                pr.begin()
                open_pids.add(pid)
                pr.append(rng.randrange(n_parts), json.dumps({
                    "kind": "event", "entity": ent,
                    "event_time": rng.randint(0, 50)}).encode(), seq=0)
                if rng.random() < 0.6:
                    pr.commit()
                    open_pids.discard(pid)
                # else stays open until the periodic decision pass

            # decide any leftover open transactions sometimes
            if rng.random() < 0.25 and open_pids:
                for pid in list(open_pids):
                    if rng.random() < 0.5:
                        pr = producers[pid]
                        try:
                            if rng.random() < 0.5:
                                pr.abort()
                            else:
                                pr.commit()
                            open_pids.discard(pid)
                        except txlog.TxLogError:
                            open_pids.discard(pid)

            # refresh both, sometimes repeatedly (no-op coverage).  Once the
            # selectable prefix contains an unresolvable conflict, no later
            # refresh can publish past it (the record stays in the source),
            # so this trial ends there — the atomic-rejection contract is
            # covered separately and in the hand-history tests.
            self._snapshot_source(log, model)
            if self._model_has_conflict(model):
                with self.assertRaises(txlog.RuleConflictError):
                    store.refresh()
                self.assertIsNotNone(store.status()["latest_release"])
                return
            report = store.refresh()
            partitions = sorted(int(p) for p in
                                log.inspect()["partitions"])
            self._assert_matches_model(store, model, partitions, report)
            if rng.random() < 0.3:
                # an immediate second refresh must publish nothing new
                rep2 = store.refresh()
                self.assertFalse(rep2["published"])

        # final reopen: durable state must equal the model
        store.close()
        store = self.open_store(create=False)
        self._snapshot_source(log, model)
        partitions = sorted(int(p) for p in log.inspect()["partitions"])
        self._assert_matches_model(store, model, partitions)
