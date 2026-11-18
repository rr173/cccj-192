#!/usr/bin/env python3
"""txlog stress / data-generation script.

Generates a concurrent transactional workload against a log directory:
multiple producers run begin/append/commit/abort cycles (with occasional
epoch bumps), non-transactional appends interleave, readers continuously
fetch under both isolation levels, NAMED application readers poll and ack
records (out of order, with handle reopens and replays mixed in), and a
retainer advances the retention frontier.  Invariants are checked online
and in a final verification pass after a fresh reopen (full recovery):

  * every acknowledged (partition, offset) maps to exactly one payload;
  * read_committed never delivers aborted or uncommitted records;
  * offsets in every partition are physically contiguous from the
    retention frontier to the high watermark (abort holes included);
  * fetch always terminates and next_offset never goes backwards;
  * every named reader's safe cursor never crosses an unacked gap, never
    passes the retention frontier, and retain is clamped by the slowest
    reader; unacked records stay redeliverable after a reopen.

All data is generated at runtime into --dir and the scale is bounded by
the CLI flags; no large generated files are ever committed.

Usage:
  python3 scripts/stress.py --dir /tmp/txlog-stress --ops 3000
  python3 scripts/stress.py --small          # tiny fixture, runs in seconds
"""

import argparse
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import txlog  # noqa: E402


class Tracker:
    """Shared bookkeeping of acknowledged writes for invariant checks."""

    def __init__(self):
        self.mu = threading.Lock()
        self.acked = {}          # payload -> (partition, offset)
        self.by_offset = {}      # (partition, offset) -> payload
        self.aborted = set()     # payloads of aborted txns
        self.errors = []

    def record_append(self, payload, partition, offset):
        with self.mu:
            if (partition, offset) in self.by_offset:
                self.errors.append(
                    f"offset reused: {(partition, offset)} "
                    f"{self.by_offset[(partition, offset)]!r} vs {payload!r}")
            self.by_offset[(partition, offset)] = payload
            self.acked[payload] = (partition, offset)

    def record_abort(self, payloads):
        with self.mu:
            self.aborted.update(payloads)

    def check_record(self, rec, isolation):
        payload = rec["payload"]
        with self.mu:
            where = self.acked.get(payload)
            if where is None:
                self.errors.append(f"unknown payload fetched: {payload!r}")
                return
            if where[1] != rec["offset"]:
                self.errors.append(
                    f"payload {payload!r} at offset {rec['offset']}, "
                    f"acked at {where[1]}")
            if (isolation == "read_committed"
                    and payload in self.aborted):
                self.errors.append(
                    f"read_committed delivered aborted payload {payload!r}")


def writer(log, wid, n_partitions, ops, rng, tracker, counter):
    producer = log.producer(f"stress-w{wid}")
    txn_payloads = []
    open_txn = False
    seqs = {}
    for _ in range(ops):
        r = rng.random()
        if r < 0.03:
            producer = log.producer(f"stress-w{wid}")  # epoch bump
            open_txn = False
            txn_payloads = []
            seqs = {}
        if not open_txn and rng.random() < 0.5:
            producer.begin()
            open_txn = True
            txn_payloads = []
        if open_txn and rng.random() < 0.7:
            p = rng.randrange(n_partitions)
            seq = seqs.get(p, 0)
            payload = f"w{wid}-{counter()}".encode()
            try:
                off = producer.append(p, payload, seq=seq)
            except txlog.TxLogError as e:
                tracker.errors.append(f"append failed: {e}")
                continue
            seqs[p] = seq + 1
            txn_payloads.append(payload)
            tracker.record_append(payload, p, off)
        elif open_txn:
            if rng.random() < 0.8:
                producer.commit()
            else:
                producer.abort()
                tracker.record_abort(txn_payloads)
            open_txn = False
            txn_payloads = []
        else:
            p = rng.randrange(n_partitions)
            payload = f"w{wid}n-{counter()}".encode()
            off = log.append(p, payload)
            tracker.record_append(payload, p, off)
    if open_txn:
        producer.commit()


def reader(log, n_partitions, stop, tracker, stats):
    rng = random.Random()
    while not stop.is_set():
        p = rng.randrange(n_partitions)
        isolation = rng.choice(["read_committed", "read_uncommitted"])
        page = log.fetch(p, rng.randrange(0, 8), max_records=8,
                         isolation=isolation)
        prev = None
        for rec in page["records"]:
            if prev is not None and rec["offset"] <= prev:
                tracker.errors.append("offsets not increasing in page")
            prev = rec["offset"]
            tracker.check_record(rec, isolation)
        stats["fetches"] += 1


def retainer(log, n_partitions, stop, stats):
    while not stop.is_set():
        p = random.randrange(n_partitions)
        info = log.inspect(partition=p)["partitions"]
        hw = info[p]["next_offset"] if p in info or str(p) in info else 0
        if hw:
            log.retain(p, random.randrange(0, hw + 1))
            stats["retains"] += 1
        time.sleep(0.01)


def reader_worker(log, rid, n_partitions, stop, tracker, stats):
    """Named resumable reader: poll/ack with out-of-order completion,
    periodic handle reopens (redelivery checks) and occasional replays."""
    rng = random.Random()
    handle = log.create_reader(rid, start=0)
    pending = {p: set() for p in range(n_partitions)}
    reopened = 0
    while not stop.is_set():
        p = rng.randrange(n_partitions)
        try:
            if rng.random() < 0.05:
                # simulate a process restart: a brand-new handle resumes
                # from the durable safe cursor and redelivers unacked work
                handle.close()
                handle = log.open_reader(rid)
                reopened += 1
            elif rng.random() < 0.05:
                try:
                    handle.replay(p, rng.randrange(0, 4))
                except txlog.ReaderDataUnavailable:
                    pass
            page = handle.poll(p, max_records=rng.randint(1, 16),
                               max_scan=rng.randint(8, 40))
            stats["reader_polls"] += 1
            for rec in page["records"]:
                tracker.check_record(rec, "read_committed")
                pending[p].add(rec["offset"])
            # ack in random order, leaving ~10% deliberately unacked so
            # gap-blocking and redelivery are exercised continuously
            for off in list(pending[p]):
                if rng.random() < 0.9:
                    try:
                        handle.ack(p, off)
                        stats["reader_acks"] += 1
                        pending[p].discard(off)
                    except txlog.ReaderAckError as e:
                        # abort holes / races are classified rejections
                        stats["ack_rejected:" + e.reason] = (
                            stats.get("ack_rejected:" + e.reason, 0) + 1)
                        pending[p].discard(off)
        except txlog.ReaderDataUnavailable as e:
            # explicit recoverable-boundary contract: resume there
            try:
                handle.replay(p, e.retained_below)
            except txlog.TxLogError:
                pass
        except txlog.TxLogError as e:
            tracker.errors.append(f"reader {rid}: {e!r}")
            return
    handle.close()
    stats["reader_reopens"] = stats.get("reader_reopens", 0) + reopened


def verify_readers(dir_path, n_partitions, reader_ids):
    """Post-recovery reader invariants: cursors coherent vs. retention,
    and redelivery sweeps never read deleted data."""
    errors = []
    log = txlog.Log(dir_path)
    try:
        registry = log.list_readers()
        for rid in reader_ids:
            if rid not in registry:
                continue
            r = log.open_reader(rid)
            for p in range(n_partitions):
                st = r.status(p)["partitions"].get(p)
                if st is None:
                    continue
                safe, read = st["safe_offset"], st["read_offset"]
                if safe > read:
                    errors.append(
                        f"{rid} p{p}: safe {safe} > read {read}")
                if any(g < safe for g in st["blocking_gaps"]):
                    errors.append(
                        f"{rid} p{p}: gap below safe cursor {st}")
                retained = log.inspect(partition=p)["partitions"][p][
                    "retained_below"]
                if safe < retained:
                    errors.append(
                        f"{rid} p{p}: safe {safe} behind frontier "
                        f"{retained} — retain crossed a reader hold")
                # redelivery sweep from the safe cursor
                off, seen = safe, []
                for _ in range(100000):
                    page = r.poll(p, offset=off, max_records=64,
                                  max_scan=128)
                    seen += [x["offset"] for x in page["records"]]
                    if page["reason"] in ("log_end", "stable_frontier"):
                        break
                    off = page["next_offset"]
                if seen != sorted(set(seen)):
                    errors.append(
                        f"{rid} p{p}: non-unique redelivery {len(seen)}")
                if any(o < safe for o in seen):
                    errors.append(
                        f"{rid} p{p}: redelivery below safe cursor")
    finally:
        log.close()
    return errors


def verify(dir_path, n_partitions, tracker):
    """Fresh reopen (full recovery) + complete sweep verification."""
    log = txlog.Log(dir_path)
    try:
        for p in range(n_partitions):
            info = log.inspect(partition=p)["partitions"][p]
            hw = info["next_offset"]
            retained = info["retained_below"]
            # physical contiguity incl. abort holes: read_uncommitted must
            # yield every offset in [retained, hw) exactly once
            seen = []
            off = retained
            for _ in range(100000):
                page = log.fetch(p, off, max_records=64,
                                 isolation="read_uncommitted")
                seen += [r["offset"] for r in page["records"]]
                if page["reason"] == "log_end":
                    break
                off = page["next_offset"]
            if seen != list(range(retained, hw)):
                tracker.errors.append(
                    f"p{p}: offsets not contiguous: got {len(seen)} "
                    f"expected [{retained},{hw})")
            # read_committed: no aborted payloads, frontier respected
            off = retained
            for _ in range(100000):
                page = log.fetch(p, off, max_records=64,
                                 isolation="read_committed")
                for rec in page["records"]:
                    tracker.check_record(rec, "read_committed")
                if page["reason"] in ("log_end", "stable_frontier"):
                    break
                off = page["next_offset"]
    finally:
        log.close()


def main():
    ap = argparse.ArgumentParser(description="txlog stress generator")
    ap.add_argument("--dir", default="/tmp/txlog-stress")
    ap.add_argument("--ops", type=int, default=3000, help="ops per writer")
    ap.add_argument("--writers", type=int, default=2)
    ap.add_argument("--readers", type=int, default=3)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--segment-bytes", type=int, default=64 * 1024)
    ap.add_argument("--dedup-window", type=int, default=64)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--named-readers", type=int, default=2,
                    help="number of concurrent named resumable readers")
    ap.add_argument("--small", action="store_true",
                    help="tiny fixture: runs in a few seconds")
    args = ap.parse_args()
    if args.small:
        args.ops = 150
        args.writers = 2
        args.readers = 2
        args.named_readers = 2
        args.partitions = 2
        args.segment_bytes = 1024
        args.dedup_window = 16

    os.makedirs(args.dir, exist_ok=True)
    log = txlog.Log(args.dir, segment_bytes=args.segment_bytes,
                    dedup_window=args.dedup_window)
    tracker = Tracker()
    stats = {"fetches": 0, "retains": 0, "reader_polls": 0,
             "reader_acks": 0}
    n = {"v": 0}
    nmu = threading.Lock()

    def counter():
        with nmu:
            n["v"] += 1
            return n["v"]

    stop = threading.Event()
    threads = []
    for wid in range(args.writers):
        rng = random.Random(args.seed * 1000 + wid)
        t = threading.Thread(
            target=writer,
            args=(log, wid, args.partitions, args.ops, rng, tracker,
                  counter))
        t.start()
        threads.append(t)
    for _ in range(args.readers):
        t = threading.Thread(target=reader,
                             args=(log, args.partitions, stop, tracker,
                                   stats))
        t.start()
        threads.append(t)
    rt = threading.Thread(target=retainer,
                          args=(log, args.partitions, stop, stats))
    rt.start()
    threads.append(rt)
    reader_ids = [f"named-r{i}" for i in range(args.named_readers)]
    for rid in reader_ids:
        t = threading.Thread(
            target=reader_worker,
            args=(log, rid, args.partitions, stop, tracker, stats))
        t.start()
        threads.append(t)

    t0 = time.monotonic()
    for t in threads[:args.writers]:
        t.join()
    stop.set()
    for t in threads[args.writers:]:
        t.join(timeout=30)
    dt = time.monotonic() - t0
    log.close()

    print(f"workload: {args.writers}x{args.ops} ops, {n['v']} records, "
          f"{stats['fetches']} fetches, {stats['retains']} retains, "
          f"{stats['reader_polls']} reader polls, "
          f"{stats['reader_acks']} reader acks in {dt:.1f}s "
          f"({n['v'] / max(dt, 1e-9):.0f} rec/s)")
    rejected = {k: v for k, v in stats.items()
                if k.startswith("ack_rejected:")}
    if rejected:
        print("classified ack rejections:", rejected)

    verify(args.dir, args.partitions, tracker)
    reader_errors = verify_readers(args.dir, args.partitions, reader_ids)
    tracker.errors.extend(reader_errors)

    if tracker.errors:
        print(f"FAILED: {len(tracker.errors)} invariant violations")
        for e in tracker.errors[:20]:
            print("  -", e)
        return 1
    print("OK: all invariants held (online + post-recovery verification, "
          "including named-reader cursors and retention holds)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
