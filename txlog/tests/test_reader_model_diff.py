"""Differential test for named readers: an independent serial model
(tests/model_log.py ModelReader) explains every reader step of a random
operation history.  The model computes each reader's contiguous safe
position, blocking gaps, delivered records, redelivery after handle
reopen, replay behavior and cleanable retention boundary from the
operation history alone; every result is compared against the real
SQLite/segment implementation.
"""

import os
import random
import unittest

import txlog
from tests.helpers import LogTestCase
from tests.model_log import ModelLog
from txlog.errors import TxLogError

SEEDS = tuple(int(s) for s in os.environ.get(
    "TXLOG_READER_DIFF_SEEDS", "101,202,303").split(","))
OPS_PER_SEED = int(os.environ.get("TXLOG_READER_DIFF_OPS", "500"))
N_PARTITIONS = 2
SEGMENT_BYTES = 260
DEDUP_WINDOW = 6
N_READERS = 3


def _err(fn):
    try:
        fn()
        return None
    except TxLogError as e:
        return type(e).__name__ + ":" + getattr(e, "reason", "")
    except ValueError as e:
        return "ValueError"


class ReaderDriver:
    def __init__(self, seed, log_dir):
        self.rng = random.Random(seed)
        self.real = txlog.Log(log_dir, segment_bytes=SEGMENT_BYTES,
                              dedup_window=DEDUP_WINDOW)
        self.model = ModelLog(SEGMENT_BYTES, DEDUP_WINDOW)
        self.history = []
        # reader_id -> list of open handles (real handle, model handle)
        self.handles = {f"r{i}": [] for i in range(N_READERS)}
        self.created = set()
        self.producer = self.real.producer("w")
        self.model.open_producer("w")
        self.txn_open = False
        self.seq = {p: 0 for p in range(N_PARTITIONS)}
        self.counter = 0

    def close(self):
        for handles in self.handles.values():
            for h, _ in handles:
                try:
                    h.close()
                except Exception:
                    pass
        self.real.close()

    def _check(self, real_fn, model_fn, what):
        self.history.append(what)
        r, re = None, None
        try:
            r = real_fn()
        except Exception as e:  # noqa: BLE001
            re = type(e).__name__ + ":" + getattr(e, "reason", "")
        m, me = None, None
        try:
            m = model_fn()
        except Exception as e:  # noqa: BLE001
            me = type(e).__name__ + ":" + getattr(e, "reason", "")
        if re != me:
            raise AssertionError(
                f"error divergence (seed history tail):\n"
                f"  what={what}\n  real err={re}\n  model err={me}\n"
                f"history tail:\n" + "\n".join(self.history[-30:]))
        if re is None and r != m:
            raise AssertionError(
                f"result divergence in {what}:\n  real ={r!r}\n"
                f"  model={m!r}\nhistory tail:\n"
                + "\n".join(self.history[-30:]))
        return r

    # -- background log traffic -------------------------------------------

    def op_write(self):
        r = self.rng.random()
        if r < 0.45:
            if not self.txn_open:
                self.producer.begin()
                self.model.begin("w", 1)
                self.txn_open = True
            p = self.rng.randrange(N_PARTITIONS)
            self.counter += 1
            payload = f"x{self.counter}".encode()
            seq = self.seq[p]
            self.producer.append(p, payload, seq=seq)
            self.model.append_txn("w", 1, p, seq, payload)
            self.seq[p] += 1
        elif r < 0.6 and self.txn_open:
            if self.rng.random() < 0.7:
                self.producer.commit()
                self.model.commit("w", 1)
            else:
                self.producer.abort()
                self.model.abort("w", 1)
            self.txn_open = False
        else:
            p = self.rng.randrange(N_PARTITIONS)
            self.counter += 1
            payload = f"n{self.counter}".encode()
            self.real.append(p, payload)
            self.model.append_non_txn(p, payload)

    @staticmethod
    def _handle_key(h):
        return h.reader_id, h.reused

    def op_create(self):
        rid = self.rng.choice(list(self.handles))
        if self.rng.random() < 0.3:
            start = None
        else:
            start = self.rng.randrange(0, 6)
        rr = self._check(
            lambda: self._handle_key(self.real.create_reader(
                rid, start=start)),
            lambda: self._handle_key(self.model.create_reader(
                rid, start=start)),
            f"create {rid} start={start}")
        if rr is not None and rid not in self.created:
            self.created.add(rid)

    def _pick_handle(self):
        rid = self.rng.choice(list(self.handles))
        if rid not in self.created or not self.handles[rid]:
            return None
        return rid, self.rng.choice(self.handles[rid])

    def op_open(self):
        rid = self.rng.choice(list(self.handles))
        if rid not in self.created:
            return
        self._check(
            lambda: self._handle_key(self.real.open_reader(rid)),
            lambda: self._handle_key(self.model.open_reader(rid)),
            f"open {rid}")
        self.handles[rid].append(
            (self.real.open_reader(rid), self.model.open_reader(rid)))

    def op_close_handle(self):
        pick = self._pick_handle()
        if pick is None:
            return
        rid, pair = pick
        pair[0].close()
        pair[1].close()
        self.handles[rid].remove(pair)
        self.history.append(f"close handle {rid}")

    def op_poll(self):
        pick = self._pick_handle()
        if pick is None:
            return
        rid, (rh, mh) = pick
        p = self.rng.randrange(N_PARTITIONS)
        max_records = self.rng.randint(1, 5)
        max_scan = self.rng.choice([None, self.rng.randint(2, 10)])
        explicit = self.rng.choice([None, self.rng.randrange(0, 8)])
        self._check(
            lambda: rh.poll(p, max_records=max_records, max_scan=max_scan,
                            offset=explicit),
            lambda: mh.poll(p, max_records=max_records, max_scan=max_scan,
                            offset=explicit),
            f"poll {rid} p{p} max={max_records} scan={max_scan} "
            f"off={explicit}")

    def op_ack(self):
        pick = self._pick_handle()
        if pick is None:
            return
        rid, (rh, mh) = pick
        p = self.rng.randrange(N_PARTITIONS)
        # mix of plausible and deliberately wrong offsets
        offset = self.rng.choice(
            [self.rng.randrange(0, 12), self.rng.randrange(0, 4)])
        self._check(lambda: rh.ack(p, offset),
                    lambda: mh.ack(p, offset),
                    f"ack {rid} p{p} off={offset}")

    def op_status(self):
        pick = self._pick_handle()
        if pick is None:
            return
        rid, (rh, mh) = pick
        p = self.rng.choice([None, self.rng.randrange(N_PARTITIONS + 1)])
        self._check(lambda: rh.status(p), lambda: mh.status(p),
                    f"status {rid} p={p}")

    def op_replay(self):
        pick = self._pick_handle()
        if pick is None:
            return
        rid, (rh, mh) = pick
        p = self.rng.randrange(N_PARTITIONS)
        off = self.rng.choice([None, self.rng.randrange(0, 8)])
        self._check(lambda: rh.replay(p, off),
                    lambda: mh.replay(p, off),
                    f"replay {rid} p{p} off={off}")

    def op_retain(self):
        p = self.rng.randrange(N_PARTITIONS)
        hw = self.model.next_offset[p]
        boundary = self.rng.choice([0, hw, hw + 3,
                                    self.rng.randrange(0, hw + 4)])
        self._check(lambda: self.real.retain(p, boundary),
                    lambda: self.model.retain(p, boundary),
                    f"retain p{p} before={boundary}")

    def op_delete(self):
        if not self.created:
            return
        rid = self.rng.choice(list(self.created))
        # close its handles first (both sides)
        for rh, mh in self.handles[rid]:
            rh.close()
            mh.close()
        self.handles[rid] = []
        self._check(lambda: self.real.delete_reader(rid),
                    lambda: self.model.delete_reader(rid),
                    f"delete {rid}")
        self.created.discard(rid)

    def op_reopen_log(self):
        for handles in self.handles.values():
            for h, _ in handles:
                h.close()
            for _, mh in handles:
                mh.close()
        self.real.close()
        self.real = txlog.Log(self.real.path, segment_bytes=SEGMENT_BYTES,
                              dedup_window=DEDUP_WINDOW)
        new_handles = {rid: [] for rid in self.handles}
        for rid in self.created:
            pair = (self.real.open_reader(rid),
                    self.model.open_reader(rid))
            new_handles[rid].append(pair)
        self.handles = new_handles
        self.producer = self.real.producer("w")
        self.history.append("reopen log")

    def step(self):
        r = self.rng.random()
        if r < 0.30:
            self.op_write()
        elif r < 0.38:
            self.op_create()
        elif r < 0.46:
            self.op_open()
        elif r < 0.50:
            self.op_close_handle()
        elif r < 0.66:
            self.op_poll()
        elif r < 0.80:
            self.op_ack()
        elif r < 0.86:
            self.op_status()
        elif r < 0.91:
            self.op_replay()
        elif r < 0.97:
            self.op_retain()
        else:
            self.op_delete()

    def final_sweep(self):
        # durable reader registry and every position must match
        self._check(lambda: self.real.list_readers(),
                    lambda: self.model.list_readers(),
                    "final list_readers")
        for rid in sorted(self.created):
            rh = self.real.open_reader(rid)
            mh = self.model.open_reader(rid)
            self._check(lambda: rh.status(), lambda: mh.status(),
                        f"final status {rid}")
            for p in range(N_PARTITIONS):
                # a reader whose configured start was overtaken by a legal
                # retain (it never materialized this partition) must get
                # the explicit boundary error on both implementations
                real_err = _err(lambda: rh.poll(p, max_records=4,
                                                max_scan=8))
                model_err = _err(lambda: mh.poll(p, max_records=4,
                                                 max_scan=8))
                if real_err is not None or model_err is not None:
                    self.history.append(
                        f"boundary check {rid} p{p}")
                    assert real_err == model_err, (
                        f"boundary error mismatch {rid} p{p}: "
                        f"{real_err} vs {model_err}")
                    if real_err and real_err.startswith(
                            "ReaderDataUnavailable"):
                        continue
                # complete redelivery sweep from each safe cursor
                for _ in range(1000):
                    rp = rh.poll(p, max_records=4, max_scan=8)
                    mp = mh.poll(p, max_records=4, max_scan=8)
                    if rp != mp:
                        raise AssertionError(
                            f"sweep divergence {rid} p{p}:\n"
                            f"real={rp}\nmodel={mp}")
                    if rp["reason"] in ("log_end", "stable_frontier",
                                        "retained_boundary"):
                        break
                self._check(lambda: rh.status(p), lambda: mh.status(p),
                            f"post-sweep status {rid} p{p}")


class TestReaderModelDifferential(LogTestCase):
    def test_reader_model_matches_real_over_random_histories(self):
        for seed in SEEDS:
            with self.subTest(seed=seed):
                d = ReaderDriver(seed, self.dir + f"-{seed}")
                try:
                    for _ in range(OPS_PER_SEED):
                        d.step()
                    if d.txn_open:
                        d.producer.commit()
                        d.model.commit("w", 1)
                    d.final_sweep()
                finally:
                    d.close()


if __name__ == "__main__":
    unittest.main()
