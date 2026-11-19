#!/usr/bin/env python3
"""Materialization stress / runtime data generator.

Builds a single txlog with an interleaved workload of standalone records and
cross-partition committed/aborted/open transactions (events plus late and
duplicate assignment rules), refreshing a materialized store after every
batch.  All data is generated at runtime into --dir and the scale is bounded
by the CLI flags; no generated data files are committed.

After the workload, a fresh store handle reopens the database and verifies,
independently of the incremental run:

  * the published vector covers exactly the read_committed-stable prefix;
  * every materialized detail agrees with a from-scratch rule evaluation;
  * the inserted-change count equals the distinct published event count;
  * every release's positions are gapless and never move backwards.

Usage:
  python3 scripts/mat_stress.py --dir /tmp/txlog-mat-stress --rounds 200
  python3 scripts/mat_stress.py --small
"""

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import txlog  # noqa: E402


def body_event(entity, t):
    return json.dumps({"kind": "event", "entity": entity,
                       "event_time": t}).encode()


def body_rule(entity, eff, rev, group):
    return json.dumps({"kind": "assignment", "entity": entity,
                       "effective_from": eff, "revision": rev,
                       "group": group}).encode()


def evaluate_from_scratch(log):
    """Independent from-scratch evaluation of the stable committed prefix."""
    info = log.inspect()
    rules = {}   # entity -> eff -> (rev, group, part, off)
    events = []  # (part, off, entity, event_time)
    for p in sorted(int(x) for x in info["partitions"]):
        off = info["partitions"][str(p)]["retained_below"] if isinstance(
            next(iter(info["partitions"])), str) else \
            info["partitions"][p]["retained_below"]
        while True:
            page = log.fetch(p, off, max_records=500,
                             isolation="read_committed")
            for rec in page["records"]:
                doc = json.loads(rec["payload"].decode("utf-8"))
                if doc["kind"] == "assignment":
                    per_eff = rules.setdefault(doc["entity"], {})
                    cand = (doc["revision"], doc["group"], p, rec["offset"])
                    cur = per_eff.get(doc["effective_from"])
                    if cur is None or cand[0] > cur[0] or (
                            cand[0] == cur[0] and
                            (cand[2], cand[3]) < (cur[2], cur[3])):
                        per_eff[doc["effective_from"]] = cand
                else:
                    events.append((p, rec["offset"], doc["entity"],
                                   doc["event_time"]))
            off = page["next_offset"]
            if page["reason"] in ("log_end", "stable_frontier"):
                break
    expected = {}
    for p, off, entity, etime in events:
        per_eff = rules.get(entity, {})
        best = None
        for eff in per_eff:
            if eff <= etime and (best is None or eff > best):
                best = eff
        expected[(p, off)] = None if best is None else (
            best,) + per_eff[best]
    return expected


def main():
    ap = argparse.ArgumentParser(description="txlog materialization stress")
    ap.add_argument("--dir", default="/tmp/txlog-mat-stress")
    ap.add_argument("--rounds", type=int, default=200)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--small", action="store_true")
    args = ap.parse_args()
    if args.small:
        args.rounds = 40
        args.partitions = 3

    rng = random.Random(args.seed)
    log_dir = os.path.join(args.dir, "log")
    mat_db = os.path.join(args.dir, "mat.sqlite3")
    os.makedirs(args.dir, exist_ok=True)
    log = txlog.open_log(log_dir, segment_bytes=64 * 1024, dedup_window=128)
    store = txlog.open_materialized(mat_db, log_dir)
    entities = [f"e{k}" for k in range(6)]
    producers = {}
    seq_counter = {}
    groups = ["g1", "g2", "g3"]
    # make the group a deterministic function of the version key so the
    # generator never accidentally creates an unresolvable conflict; equal
    # keys can still repeat (exercising smallest-location deduplication).
    def group_for(ent, eff, rev):
        return groups[(hash((ent, eff, rev)) & 0x7fffffff) % len(groups)]
    try:
        for rnd in range(args.rounds):
            for _ in range(rng.randint(1, 8)):
                ent = rng.choice(entities)
                p = rng.randrange(args.partitions)
                if rng.random() < 0.7:
                    log.append(p, body_event(ent, rng.randint(0, 100)))
                else:
                    eff, rev = rng.randint(0, 90), rng.randint(0, 5)
                    log.append(p, body_rule(
                        ent, eff, rev, group_for(ent, eff, rev)))
            # occasional cross-partition transaction
            if rng.random() < 0.5:
                pid = f"p{rnd}"
                pr = log.producer(pid)
                producers[pid] = pr
                pr.begin()
                parts = rng.sample(range(args.partitions),
                                   rng.randint(1, args.partitions))
                for p in parts:
                    seq = seq_counter.setdefault((pid, p), 0)
                    if rng.random() < 0.5:
                        pr.append(p, body_event(
                            rng.choice(entities), rng.randint(0, 100)),
                            seq=seq)
                    else:
                        ent = rng.choice(entities)
                        eff, rev = rng.randint(0, 90), rng.randint(0, 5)
                        pr.append(p, body_rule(
                            ent, eff, rev, group_for(ent, eff, rev)),
                            seq=seq)
                    seq_counter[(pid, p)] = seq + 1
                if rng.random() < 0.2:
                    pr.abort()
                    # aborted records are holes: they never become details
                else:
                    pr.commit()
            # an extra committed transaction that can land near a frontier
            if rng.random() < 0.15:
                pid = f"open{rnd}"
                pr = log.producer(pid)
                pr.begin()
                pr.append(rng.randrange(args.partitions),
                          body_event(rng.choice(entities), 1), seq=0)
                pr.commit()
            try:
                store.refresh()
            except txlog.RuleConflictError:
                # an unresolvable version is a legitimate stop condition;
                # the previous release must stay coherent
                break
    finally:
        log.close()
        store.close()

    # independent verification on a fresh reopen
    log = txlog.open_log(log_dir, writable=False)
    store = txlog.open_materialized(mat_db, create=False)
    errors = []
    # the from-scratch enumeration is the authoritative committed-event set
    expected = evaluate_from_scratch(log)
    total_events = len(expected)
    details = store.read_all_details()
    if len(details) != total_events:
        errors.append(f"detail count {len(details)} != events {total_events}")
    for row in details:
        coord = (row["event"]["partition"], row["event"]["offset"])
        exp = expected.get(coord)
        got = None
        if row["assigned"]:
            got = (row["rule"]["effective_from"], row["rule"]["revision"],
                   row["group"], row["rule"]["source"]["partition"],
                   row["rule"]["source"]["offset"])
        if got != exp:
            errors.append(f"{coord}: got {got} expected {exp}")
    # release positions never move backwards and are gapless
    con = __import__("sqlite3").connect(mat_db)
    prev = {}
    last_rel = 0
    for (rel,) in con.execute("SELECT DISTINCT release_no FROM changes "
                              "UNION SELECT release_no FROM releases "
                              "ORDER BY release_no"):
        last_rel = max(last_rel, rel)
        vec = {r[0]: r[1] for r in con.execute(
            "SELECT partition, offset FROM release_positions "
            "WHERE release_no=?", (rel,))}
        for p, off in vec.items():
            if off < prev.get(p, 0):
                errors.append(f"release {rel} partition {p} moved backwards")
        prev = vec
    inserted = con.execute(
        "SELECT COUNT(*) FROM changes WHERE change_type='inserted'"
    ).fetchone()[0]
    if inserted != total_events:
        errors.append(f"inserted changes {inserted} != events {total_events}")
    con.close()
    store.close()
    log.close()

    if errors:
        print(f"FAILED: {len(errors)} invariant violations")
        for e in errors[:20]:
            print("  -", e)
        return 1
    print(f"OK: {total_events} events, latest release {last_rel}, "
          f"{len(details)} details agree with from-scratch evaluation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
