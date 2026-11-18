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
from txlog.errors import (ContentConflict, ProducerFenced, ReaderAckError,
                         ReaderConflict, ReaderDataUnavailable, SequenceGap,
                         SequenceTooOld, TxnStateError, UnknownReader,
                         UnknownTransaction)


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
        # reader_id -> {start_mode, start_offset, positions:
        #   {partition -> {"read", "safe", "acks": set()}}}
        self.readers = {}

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
                    "reader_floor": None, "reader_holds": [],
                    "high_watermark": 0}
        hw = self.next_offset[partition]
        retained = self.retained_below[partition]
        lso = self._lso(partition)
        floor = self._dedup_floor(partition)
        reader_floor, reader_holds = self._reader_constraint(
            partition, max(retained, 0))
        candidates = [before_offset, lso, floor, hw]
        if reader_floor is not None:
            candidates.append(reader_floor)
        applied = min(candidates)
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
        elif (reader_floor is not None and applied == reader_floor
              and reader_floor < hw):
            constrained = "reader"
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
                "lso": lso, "dedup_floor": floor,
                "reader_floor": reader_floor, "reader_holds": reader_holds,
                "high_watermark": hw}


class ModelReader:
    """Independent serial model of one named-reader HANDLE.

    Mirrors txlog.reader.Reader with deliberately different mechanics:
    plain sets and sorted walks instead of SQL; only the underlying record
    contents come from ModelLog.  The differential test compares every
    page, cursor, gap list and error class against the real implementation.
    """

    def __init__(self, model, reader_id, reused):
        self.model = model
        self.reader_id = reader_id
        self.reused = reused
        self.poll_pos = {}
        self.closed = False

    def _state(self):
        return self.model.readers[self.reader_id]

    def _check(self):
        if self.closed or self.reader_id not in self.model.readers:
            raise UnknownReader(f"unknown reader {self.reader_id!r}")

    def _aborted_offsets(self, p, low, high):
        out = set()
        for t in self.model.txns.values():
            if t["state"] != "aborted" or p not in t["parts"]:
                continue
            lo, hi = t["parts"][p]
            for o in range(max(lo, low), min(hi, high - 1) + 1):
                out.add(o)
        return out

    def _position(self, p):
        st = self._state()
        if p not in st["positions"]:
            retained = self.model.retained_below[p]
            if st["start_mode"] == "boundary":
                start = retained
            else:
                start = st["start_offset"]
                if start < retained:
                    raise ReaderDataUnavailable(self.reader_id, p, start,
                                                retained)
            st["positions"][p] = {"read": start, "safe": start,
                                  "acks": set()}
        return st["positions"][p]

    def poll(self, partition, max_records=100, max_scan=None, offset=None):
        self._check()
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        if max_scan is None:
            max_scan = max(64, max_records * 8)
        if max_scan < 1:
            raise ValueError("max_scan must be >= 1")
        if offset is not None:
            offset = int(offset)
        p = partition
        st = self._state()
        retained = self.model.retained_below[p]
        if offset is not None and int(offset) < retained:
            # the product rejects BEFORE materializing any position row
            raise ReaderDataUnavailable(self.reader_id, p, int(offset),
                                        retained)
        if p not in st["positions"] and st["start_mode"] == "offset" \
                and st["start_offset"] < retained:
            raise ReaderDataUnavailable(self.reader_id, p,
                                        st["start_offset"], retained)
        pos = self._position(p)
        cur = self.poll_pos.get(p, pos["safe"])
        if offset is not None:
            cur = int(offset)
        page = self.model.fetch(p, cur, max_records=max_records,
                                isolation="read_committed",
                                max_scan=max_scan)
        if offset is None:
            new_read = max(pos["read"], page["next_offset"])
            pos["read"] = new_read
            self.poll_pos[p] = new_read
        else:
            # explicit repositioning: handle-local only; the durable
            # delivery high-water never jumps over unseen records, and the
            # handle's continuation position is left untouched.
            new_read = pos["read"]
        page["reader_id"] = self.reader_id
        page["read_offset"] = new_read
        page["safe_offset"] = pos["safe"]
        for rec in page["records"]:
            rec["reader_id"] = self.reader_id
            rec["partition"] = p
        return page

    def ack(self, partition, offset):
        self._check()
        p, o = partition, int(offset)
        st = self._state()
        if p not in st["positions"]:
            raise ReaderAckError(
                "wrong_partition",
                f"reader {self.reader_id!r} has no position for "
                f"partition {p}; poll it before acking")
        pos = st["positions"][p]
        safe, read = pos["safe"], pos["read"]
        hw = self.model.next_offset[p]
        retained = self.model.retained_below[p]
        if o < safe:
            return {"reader_id": self.reader_id, "partition": p,
                    "offset": o, "duplicate": True, "safe_offset": safe,
                    "read_offset": read}
        if o >= read:
            lso = self.model._lso(p)
            if o >= min(lso, hw):
                raise ReaderAckError(
                    "beyond_stable",
                    f"offset {o} is beyond the stable readable frontier "
                    f"{min(lso, hw)}")
            raise ReaderAckError(
                "not_delivered",
                f"offset {o} was never delivered to reader "
                f"{self.reader_id!r} (read_offset={read})")
        if o in self._aborted_offsets(p, retained, read):
            raise ReaderAckError(
                "abort_hole",
                f"offset {o} on partition {p} is an aborted-transaction "
                f"hole: no record was ever delivered there")
        duplicate = o in pos["acks"]
        pos["acks"].add(o)
        holes = self._aborted_offsets(p, retained, read)
        walk = safe
        while walk < read:
            if walk in pos["acks"]:
                pos["acks"].discard(walk)
                walk += 1
            elif walk in holes:
                walk += 1
            else:
                break
        pos["safe"] = walk
        return {"reader_id": self.reader_id, "partition": p, "offset": o,
                "duplicate": duplicate, "safe_offset": walk,
                "read_offset": read}

    def replay(self, partition, offset=None):
        self._check()
        p = partition
        st = self._state()
        retained = self.model.retained_below[p]
        if offset is None:
            offset = (retained if st["start_mode"] == "boundary"
                      else st["start_offset"])
        offset = int(offset)
        if offset < retained:
            raise ReaderDataUnavailable(self.reader_id, p, offset, retained)
        if p not in st["positions"]:
            if st["start_mode"] == "boundary":
                row_start = retained
            elif st["start_offset"] >= retained:
                row_start = st["start_offset"]
            else:
                row_start = offset
            st["positions"][p] = {"read": row_start, "safe": row_start,
                                  "acks": set()}
        self.poll_pos[p] = offset
        out = self.status(p)
        out["replay_from_offset"] = offset
        return out

    def status(self, partition=None):
        self._check()
        st = self._state()
        out = {"reader_id": self.reader_id, "start_mode": st["start_mode"],
               "start_offset": st["start_offset"], "partitions": {}}
        parts = st["positions"]
        if partition is not None:
            parts = {partition: parts[partition]} if partition in parts \
                else {}
        for p, pos in sorted(parts.items()):
            out["partitions"][p] = {
                "read_offset": pos["read"], "safe_offset": pos["safe"],
                "blocking_gaps": sorted(o for o in pos["acks"]
                                        if o >= pos["safe"])}
        return out

    def close(self):
        self.closed = True
        self.poll_pos.clear()

    def delete(self):
        return self.model.delete_reader(self.reader_id, handle=self)


def _make_model_reader_methods(model_cls):
    """Attach reader lifecycle methods to ModelLog (kept separate for
    readability; the logic is the serial reference implementation)."""

    def create_reader(self, reader_id, start=None):
        mode = "boundary" if start is None else "offset"
        start_offset = None if start is None else int(start)
        existing = self.readers.get(reader_id)
        if existing is not None:
            if (existing["start_mode"] != mode
                    or existing["start_offset"] != start_offset):
                raise ReaderConflict(
                    f"reader {reader_id!r} already exists with "
                    f"start_mode={existing['start_mode']!r} "
                    f"start_offset={existing['start_offset']}; requested "
                    f"start_mode={mode!r} start_offset={start_offset}. "
                    f"Existing progress was not modified")
            return ModelReader(self, reader_id, reused=True)
        self.readers[reader_id] = {
            "start_mode": mode, "start_offset": start_offset,
            "positions": {}}
        return ModelReader(self, reader_id, reused=False)

    def open_reader(self, reader_id):
        if reader_id not in self.readers:
            raise UnknownReader(f"unknown reader {reader_id!r}")
        return ModelReader(self, reader_id, reused=True)

    def list_readers(self):
        out = {}
        for rid, st in self.readers.items():
            parts = {}
            for p, pos in sorted(st["positions"].items()):
                parts[p] = {
                    "read_offset": pos["read"], "safe_offset": pos["safe"],
                    "blocking_gaps": sorted(o for o in pos["acks"]
                                            if o >= pos["safe"])}
            out[rid] = {"start_mode": st["start_mode"],
                        "start_offset": st["start_offset"],
                        "partitions": parts}
        return out

    def delete_reader(self, reader_id, handle=None):
        if reader_id not in self.readers:
            raise UnknownReader(f"unknown reader {reader_id!r}")
        st = self.readers[reader_id]
        positions = {p: {"read_offset": pos["read"],
                         "safe_offset": pos["safe"]}
                     for p, pos in st["positions"].items()}
        # mirrors the product's SELECT over the partition catalog: every
        # known partition appears, including ones never retained (value 0)
        retained = {p: self.retained_below.get(p, 0)
                    for p in self.partitions_seen}
        del self.readers[reader_id]
        if handle is not None and getattr(handle, "reader_id",
                                          None) == reader_id:
            handle.closed = True
            handle.poll_pos.clear()
        return {"reader_id": reader_id, "start_mode": st["start_mode"],
                "start_offset": st["start_offset"],
                "former_positions": positions, "retained_below": retained,
                "retention_effect":
                    "reader hold released; run retain() on each partition "
                    "to recompute the cleanable boundary"}

    def _reader_constraint(self, partition, boundary):
        holds, floor = [], None
        for rid, st in self.readers.items():
            pos = st["positions"].get(partition)
            if pos is None:
                continue
            holds.append({"reader_id": rid, "safe_offset": pos["safe"]})
            floor = pos["safe"] if floor is None \
                else min(floor, pos["safe"])
        holds.sort(key=lambda h: (h["safe_offset"], h["reader_id"]))
        return floor, holds

    model_cls.create_reader = create_reader
    model_cls.open_reader = open_reader
    model_cls.list_readers = list_readers
    model_cls.delete_reader = delete_reader
    model_cls._reader_constraint = _reader_constraint


_make_model_reader_methods(ModelLog)
