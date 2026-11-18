"""Differential test: an independent serial state model explains the full
operation history.  Random histories are replayed against the real
segmented log and the model; every fetch (contents, offsets, stable
frontier), every append outcome, every retain boundary and every error is
compared at each step.
"""

import os
import random
import unittest

import txlog
from tests.helpers import LogTestCase
from tests.model_log import ModelLog

SEEDS = tuple(int(s) for s in os.environ.get(
    "TXLOG_DIFF_SEEDS", "11,22,33").split(","))
OPS_PER_SEED = int(os.environ.get("TXLOG_DIFF_OPS", "400"))
N_PARTITIONS = 3
N_PRODUCERS = 3
SEGMENT_BYTES = 300
DEDUP_WINDOW = 5


class Driver:
    def __init__(self, seed, log_dir):
        self.rng = random.Random(seed)
        self.seed = seed
        self.dir = log_dir
        self.real = txlog.Log(log_dir, segment_bytes=SEGMENT_BYTES,
                              dedup_window=DEDUP_WINDOW)
        self.model = ModelLog(SEGMENT_BYTES, DEDUP_WINDOW)
        self.pids = [f"prod{i}" for i in range(N_PRODUCERS)]
        self.current = {}          # pid -> epoch
        self.stale = {}            # pid -> [old epochs]
        self.counter = 0
        self.history = []
        self.txn_appends = []      # (pid, epoch, partition, seq, payload)

    def close(self):
        self.real.close()

    def _payload(self):
        self.counter += 1
        return f"payload-{self.counter}".encode()

    def _check(self, real_fn, model_fn, what):
        self.history.append(what)
        try:
            r, re = real_fn(), None
        except Exception as e:  # noqa: BLE001
            r, re = None, type(e).__name__
        try:
            m, me = model_fn(), None
        except Exception as e:  # noqa: BLE001
            m, me = None, type(e).__name__
        if re != me or (re is None and r != m):
            tail = "\n".join(self.history[-40:])
            raise AssertionError(
                f"divergence (seed={self.seed}) in {what}:\n"
                f"  real =({r!r}, {re})\n  model=({m!r}, {me})\n"
                f"history tail:\n{tail}")
        return r

    # -- operations --------------------------------------------------------

    def _pid_epoch(self, allow_stale=False):
        pid = self.rng.choice(self.pids)
        if (allow_stale and self.stale.get(pid)
                and self.rng.random() < 0.5):
            return pid, self.rng.choice(self.stale[pid])
        if pid not in self.current:
            self.op_reopen(pid)
        return pid, self.current[pid]

    def op_reopen(self, pid=None):
        pid = pid or self.rng.choice(self.pids)
        real_epoch, _ = self.real._open_producer(pid)
        model_epoch = self.model.open_producer(pid)
        assert real_epoch == model_epoch, (real_epoch, model_epoch)
        if pid in self.current:
            self.stale.setdefault(pid, []).append(self.current[pid])
        self.current[pid] = model_epoch
        self.history.append(f"reopen {pid} -> epoch {model_epoch}")

    def op_begin(self):
        pid, epoch = self._pid_epoch(allow_stale=True)
        self._check(lambda: self.real._begin(pid, epoch),
                    lambda: self.model.begin(pid, epoch),
                    f"begin {pid}@{epoch}")

    def op_append_txn(self):
        pid, epoch = self._pid_epoch(allow_stale=True)
        p = self.rng.randrange(N_PARTITIONS)
        next_seq = self.model.seq_state.get((pid, epoch, p), [0, 0])[0]
        r = self.rng.random()
        if r < 0.55 or next_seq == 0 and r < 0.8:
            seq, payload = next_seq, self._payload()
        elif r < 0.65:
            seq, payload = next_seq + self.rng.randint(1, 3), self._payload()
        elif r < 0.85 and self.txn_appends:
            _pid, _e, p, seq, payload = self.rng.choice(self.txn_appends)
        else:
            seq = max(0, next_seq - self.rng.randint(1, 12))
            payload = self._payload()
        res = self._check(
            lambda: self.real._append_txn(pid, epoch, p, payload, seq),
            lambda: self.model.append_txn(pid, epoch, p, seq, payload),
            f"append_txn {pid}@{epoch} p{p} seq{seq}")
        if res is not None:
            off, dup = res
            if not dup:
                self.txn_appends.append((pid, epoch, p, seq, payload))

    def op_append_non_txn(self):
        p = self.rng.randrange(N_PARTITIONS)
        payload = self._payload()
        self._check(lambda: self.real.append(p, payload),
                    lambda: self.model.append_non_txn(p, payload),
                    f"append_non_txn p{p}")

    def op_decide(self, verb):
        pid, epoch = self._pid_epoch(allow_stale=True)
        txn_id = None
        if self.rng.random() < 0.25:
            txn_id = self.rng.randrange(1, self.model.next_txn_id + 2)
        real_fn = (lambda: self.real._commit(pid, epoch, txn_id)) if verb == "commit" \
            else (lambda: self.real._abort(pid, epoch, txn_id))
        model_fn = (lambda: self.model.commit(pid, epoch, txn_id)) if verb == "commit" \
            else (lambda: self.model.abort(pid, epoch, txn_id))
        self._check(real_fn, model_fn, f"{verb} {pid}@{epoch} txn={txn_id}")

    def op_fetch(self):
        p = self.rng.randrange(N_PARTITIONS + 1)
        hw = self.model.next_offset[p]
        retained = self.model.retained_below[p]
        off = self.rng.choice([0, retained, hw,
                               self.rng.randrange(0, hw + 3)])
        max_records = self.rng.randint(1, 6)
        max_scan = self.rng.choice([None, self.rng.randint(2, 12)])
        isolation = self.rng.choice(["read_committed", "read_uncommitted"])
        self._check(
            lambda: self.real.fetch(p, off, max_records=max_records,
                                    isolation=isolation, max_scan=max_scan),
            lambda: self.model.fetch(p, off, max_records=max_records,
                                     isolation=isolation, max_scan=max_scan),
            f"fetch p{p} off={off} max={max_records} scan={max_scan} "
            f"iso={isolation}")

    def op_retain(self):
        p = self.rng.randrange(N_PARTITIONS)
        hw = self.model.next_offset[p]
        boundary = self.rng.choice([0, hw, hw + 5,
                                    self.rng.randrange(0, hw + 6)])
        self._check(lambda: self.real.retain(p, boundary),
                    lambda: self.model.retain(p, boundary),
                    f"retain p{p} before={boundary}")

    def op_reopen_log(self):
        self.real.close()
        self.real = txlog.Log(self.dir, segment_bytes=SEGMENT_BYTES,
                              dedup_window=DEDUP_WINDOW)
        self.history.append("reopen log (recovery)")
        self.op_fetch()

    def step(self):
        r = self.rng.random()
        if r < 0.05:
            self.op_reopen()
        elif r < 0.13:
            self.op_begin()
        elif r < 0.42:
            self.op_append_txn()
        elif r < 0.50:
            self.op_append_non_txn()
        elif r < 0.59:
            self.op_decide("commit")
        elif r < 0.64:
            self.op_decide("abort")
        elif r < 0.86:
            self.op_fetch()
        elif r < 0.94:
            self.op_retain()
        else:
            self.op_reopen_log()

    def final_sweep(self):
        for p in range(N_PARTITIONS + 1):
            for isolation in ("read_committed", "read_uncommitted"):
                off = 0
                for _ in range(5000):
                    rp = self.real.fetch(p, off, max_records=3,
                                         isolation=isolation, max_scan=7)
                    mp = self.model.fetch(p, off, max_records=3,
                                          isolation=isolation, max_scan=7)
                    if rp != mp:
                        raise AssertionError(
                            f"sweep divergence p{p} off={off} {isolation}:\n"
                            f"real={rp}\nmodel={mp}")
                    if rp["reason"] in ("log_end", "stable_frontier"):
                        break
                    off = rp["next_offset"]
                else:
                    raise AssertionError(f"sweep did not terminate p{p}")
        for txn_id in range(1, self.model.next_txn_id + 2):
            r = self.real.txn_state(txn_id)
            m = self.model.txn_state(txn_id)
            if r != m:
                raise AssertionError(
                    f"txn_state divergence txn {txn_id}: real={r} model={m}")


class TestModelDifferential(LogTestCase):
    def test_model_matches_real_over_random_histories(self):
        for seed in SEEDS:
            with self.subTest(seed=seed):
                d = Driver(seed, self.dir + f"-{seed}")
                try:
                    for _ in range(OPS_PER_SEED):
                        d.step()
                    d.final_sweep()
                finally:
                    d.close()


if __name__ == "__main__":
    unittest.main()
