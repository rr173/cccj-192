"""Independent serial state model of txlog, used for differential testing.

This is a second, deliberately simple in-memory implementation of the same
externally visible semantics (epochs, sequences, dedup window, transaction
visibility, stable frontier, retention constraints, segment rolls).  The
differential test replays identical operation histories against the model
and the real segmented-log implementation and compares every result.

Only the record *length* computation is shared with the real system (via
txlog.storage.encode_record) so that simulated segment rolls happen at the
same offsets; all state logic is independent.
"""

import zlib
from collections import defaultdict

from txlog import storage
from txlog.errors import (ContentConflict, ProducerFenced, SequenceGap,
                          SequenceTooOld, TxnStateError, UnknownTransaction)


class ModelLog:
    def __init__(self, segment_bytes, dedup_window):
        self.segment_bytes = segment_bytes
        self.dedup_window = dedup_window
        self.producers = {}                     # pid -> epoch
        self.txns = {}                          # txn_id -> dict
        self.next_txn_id = 1
        self.records = defaultdict(list)        # partition -> [rec,...]
        self.next_offset = defaultdict(int)
        self.retained_below = defaultdict(int)
        self.seq_state = {}                     # (pid,epoch,p)->[next,min_retained]
        self.dedup = {}                         # (pid,epoch,p,seq)->(offset,crc)
        self.segments = defaultdict(list)       # partition -> [seg dicts]
        self.partitions_seen = set()

    # -- helpers -----------------------------------------------------------

    def _seg_roll_if_needed(self, p, rec_len, offset):
        segs = self.segments[p]
        if not segs:
            segs.append({"base_offset": 0, "max_offset": -1, "bytes": 0,
                         "state": "active",
                         "path": f"p{p}/" + storage.segment_name(0)})
        active = segs[-1]
        if active["bytes"] > 0 and active["bytes"] + rec_len > self.segment_bytes:
            active["state"] = "sealed"
            active["max_offset"] = offset - 1
            segs.append({"base_offset": offset, "max_offset": -1, "bytes": 0,
                         "state": "active",
                         "path": f"p{p}/" + storage.segment_name(offset)})
        return segs[-1]

    def _append_record(self, p, payload, txn_id, epoch, seq, producer_id):
        offset = self.next_offset[p]
        rec_len = len(storage.encode_record(offset, txn_id, epoch, seq,
                                            producer_id or "", payload))
        active = self._seg_roll_if_needed(p, rec_len, offset)
        active["bytes"] += rec_len
        active["max_offset"] = offset
        self.records[p].append({
            "offset": offset, "payload": payload, "txn_id": txn_id,
            "producer_id": producer_id, "epoch": epoch, "seq": seq})
        self.next_offset[p] = offset + 1
        self.partitions_seen.add(p)
        return offset

    def _check_epoch(self, pid, epoch):
        cur = self.producers.get(pid)
        if cur is None or cur != epoch:
            raise ProducerFenced(
                f"producer {pid!r} fenced: current epoch is {cur}, "
                f"requested {epoch}")

    def _open_txn(self, pid):
        for t in self.txns.values():
            if t["producer_id"] == pid and t["state"] == "open":
                return t
        return None

    def _lso(self, p):
        firsts = []
        for t in self.txns.values():
            if t["state"] == "open" and p in t["parts"]:
                firsts.append(t["parts"][p][0])
        return min(firsts) if firsts else self.next_offset[p]

    def _dedup_floor(self, p):
        offs = [off for (pid, e, pp, s), (off, crc) in self.dedup.items()
                if pp == p]
        return min(offs) if offs else self.next_offset[p]

    # -- producer / txn ops --------------------------------------------------

    def open_producer(self, pid):
        epoch = self.producers.get(pid, 0) + 1
        self.producers[pid] = epoch
        for t in self.txns.values():
            if t["producer_id"] == pid and t["state"] == "open":
                t["state"] = "aborted"
        self.dedup = {k: v for k, v in self.dedup.items()
                      if not (k[0] == pid and k[1] < epoch)}
        self.seq_state = {k: v for k, v in self.seq_state.items()
                          if not (k[0] == pid and k[1] < epoch)}
        return epoch

    def begin(self, pid, epoch):
        self._check_epoch(pid, epoch)
        t = self._open_txn(pid)
        if t is not None:
            return t["txn_id"]
        txn_id = self.next_txn_id
        self.next_txn_id += 1
        self.txns[txn_id] = {"txn_id": txn_id, "producer_id": pid,
                             "epoch": epoch, "state": "open", "parts": {}}
        return txn_id

    def append_txn(self, pid, epoch, p, seq, payload):
        self._check_epoch(pid, epoch)
        txn = self._open_txn(pid)
        if txn is None:
            raise TxnStateError(f"producer {pid!r} has no open transaction")
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        key3 = (pid, epoch, p)
        if key3 not in self.seq_state:
            if seq != 0:
                raise SequenceGap(f"expected sequence 0, got {seq}")
            self.seq_state[key3] = [0, 0]
        ss = self.seq_state[key3]
        if seq < ss[1]:
            raise SequenceTooOld(
                f"sequence {seq} below dedup window lower bound {ss[1]}")
        key = (pid, epoch, p, seq)
        if key in self.dedup:
            off, ocrc = self.dedup[key]
            if ocrc != crc:
                raise ContentConflict(
                    f"seq {seq} already appended with different content")
            return off, True
        if seq != ss[0]:
            raise SequenceGap(f"expected sequence {ss[0]}, got {seq}")
        offset = self._append_record(p, payload, txn["txn_id"], epoch, seq,
                                     pid)
        self.dedup[key] = (offset, crc)
        ss[0] = seq + 1
        ss[1] = max(0, ss[0] - self.dedup_window)
        for k in [k for k in self.dedup
                  if k[:3] == (pid, epoch, p) and k[3] < ss[1]]:
            del self.dedup[k]
        parts = txn["parts"].setdefault(p, [offset, offset])
        parts[1] = offset
        return offset, False

    def append_non_txn(self, p, payload):
        return self._append_record(p, payload, 0, -1, -1, "")

    def _decide(self, pid, epoch, txn_id, state):
        self._check_epoch(pid, epoch)
        txn = self._open_txn(pid)
        if txn is None:
            if txn_id is not None:
                t = self.txns.get(txn_id)
                if t is None or t["producer_id"] != pid:
                    raise UnknownTransaction(f"transaction {txn_id} not found")
                if t["state"] == state:
                    return txn_id
                raise TxnStateError(
                    f"transaction {txn_id} is already {t['state']}")
            raise TxnStateError(f"producer {pid!r} has no open transaction")
        if txn_id is not None and txn["txn_id"] != txn_id:
            raise TxnStateError("different transaction is open")
        txn["state"] = state
        return txn["txn_id"]

    def commit(self, pid, epoch, txn_id=None):
        return self._decide(pid, epoch, txn_id, "committed")

    def abort(self, pid, epoch, txn_id=None):
        return self._decide(pid, epoch, txn_id, "aborted")

    def txn_state(self, txn_id):
        t = self.txns.get(txn_id)
        if t is None:
            return None
        return {"txn_id": t["txn_id"], "producer_id": t["producer_id"],
                "epoch": t["epoch"], "state": t["state"],
                "partitions": {p: list(r) for p, r in t["parts"].items()}}

    # -- fetch -----------------------------------------------------------------

    def fetch(self, partition, offset=0, max_records=100,
              isolation="read_committed", max_scan=None):
        if max_scan is None:
            max_scan = max(64, max_records * 8)
        hw = self.next_offset[partition]
        retained = self.retained_below[partition]
        lso = self._lso(partition)
        base = {"partition": partition, "isolation": isolation,
                "high_watermark": hw, "last_stable_offset": lso,
                "retained_below": retained}
        if offset < retained:
            return dict(base, records=[], next_offset=retained,
                        reason="retained_boundary")
        limit = hw if isolation == "read_uncommitted" else min(lso, hw)
        pos = min(offset, limit)
        records = []
        scanned = 0
        for rec in self.records[partition]:
            if rec["offset"] < pos:
                continue
            if rec["offset"] >= limit:
                break
            scanned += 1
            pos = rec["offset"] + 1
            status = None
            deliver = False
            if rec["txn_id"] == 0:
                deliver = True
            else:
                st = self.txns[rec["txn_id"]]["state"]
                if isolation == "read_uncommitted":
                    deliver = True
                    status = st
                elif st == "committed":
                    deliver = True
                    status = st
                # aborted: skip (hole)
            if deliver:
                records.append({
                    "offset": rec["offset"], "payload": rec["payload"],
                    "txn_id": rec["txn_id"] or None, "txn_status": status,
                    "producer_id": rec["producer_id"] or None,
                    "epoch": rec["epoch"] if rec["epoch"] >= 0 else None,
                    "seq": rec["seq"] if rec["seq"] >= 0 else None})
            if len(records) >= max_records or scanned >= max_scan:
                break
        if pos >= limit:
            reason = "log_end" if limit == hw else "stable_frontier"
        elif len(records) >= max_records:
            reason = "page_full"
        else:
            reason = "scan_limit"
        return dict(base, records=records, next_offset=pos, reason=reason)

    # -- retain -----------------------------------------------------------------

    def retain(self, partition, before_offset):
        if partition not in self.partitions_seen:
            return {"partition": partition, "requested": before_offset,
                    "applied": 0, "retained_below": 0, "deleted_segments": [],
                    "constrained_by": "empty", "lso": 0, "dedup_floor": 0,
                    "high_watermark": 0}
        hw = self.next_offset[partition]
        retained = self.retained_below[partition]
        lso = self._lso(partition)
        floor = self._dedup_floor(partition)
        applied = min(before_offset, lso, floor, hw)
        new_retained = max(retained, applied)
        deleted = []
        for s in self.segments[partition]:
            if s["state"] == "sealed" and s["max_offset"] < new_retained:
                s["state"] = "deleted"
                deleted.append({"base_offset": s["base_offset"],
                                "path": s["path"]})
        self.retained_below[partition] = new_retained
        # transaction evidence GC: only once every touched partition has
        # retained past the transaction's records
        for txn_id, t in list(self.txns.items()):
            if t["state"] == "open":
                continue
            if all(self.retained_below[pp] > lo
                   for pp, (f, lo) in t["parts"].items()):
                del self.txns[txn_id]
        if applied >= before_offset:
            constrained = "requested"
        elif applied == lso and lso < hw:
            constrained = "lso"
        elif applied == floor and floor < hw:
            constrained = "dedup_window"
        elif applied == hw:
            constrained = "high_watermark"
        else:
            constrained = "requested"
        return {"partition": partition, "requested": before_offset,
                "applied": applied, "retained_below": new_retained,
                "deleted_segments": deleted, "constrained_by": constrained,
                "lso": lso, "dedup_floor": floor, "high_watermark": hw}
