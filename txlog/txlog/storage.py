"""Segment file record format (all integers big-endian).

    record := MAGIC(4) LENGTH(4) CRC32(4) BODY
    MAGIC  = b"TXL1"
    LENGTH = number of bytes after the LENGTH field (i.e. 4 + len(BODY))
    CRC32  = zlib.crc32(BODY)
    BODY   := OFFSET(u64) TXN_ID(i64) EPOCH(i64) SEQ(i64)
              PRODUCER_LEN(u16) PRODUCER(utf-8) PAYLOAD_LEN(u32) PAYLOAD

    Non-transactional records use TXN_ID=0, EPOCH=-1, SEQ=-1, PRODUCER="".

Segment files are append-only sequences of records.  File name is the base
offset: "<base:020d>.seg" inside the partition directory "p<partition>".

Parsing rules (used by recovery and readers):
  * A framing/checksum failure at the very end of the *active* segment, with
    no valid record after it, is a TORN TAIL (crash mid-append) and may be
    truncated by recovery.
  * Garbage followed by a valid record, or any parse failure inside a sealed
    segment, is definite CORRUPTION and must be reported, never silently
    treated as a clean end-of-log.
"""

import os
import struct
import zlib

MAGIC = b"TXL1"
HEADER = struct.Struct(">4sI")        # magic, length (crc + body)
BODY_FIXED = struct.Struct(">QqqqH")  # offset, txn_id, epoch, seq, producer_len
PLEN = struct.Struct(">I")            # payload_len
MAX_RECORD_BYTES = 64 * 1024 * 1024


class Record:
    __slots__ = ("offset", "txn_id", "epoch", "seq", "producer_id",
                 "payload", "pos", "end")

    def __init__(self, offset, txn_id, epoch, seq, producer_id, payload,
                 pos, end):
        self.offset = offset
        self.txn_id = txn_id
        self.epoch = epoch
        self.seq = seq
        self.producer_id = producer_id
        self.payload = payload
        self.pos = pos
        self.end = end

    def __repr__(self):
        return (f"Record(offset={self.offset}, txn_id={self.txn_id}, "
                f"epoch={self.epoch}, seq={self.seq}, "
                f"producer={self.producer_id!r}, "
                f"payload={len(self.payload)}B)")


def encode_record(offset, txn_id, epoch, seq, producer_id, payload):
    """Serialize one record to its on-disk byte form."""
    pid = producer_id.encode("utf-8") if producer_id else b""
    body = (BODY_FIXED.pack(offset, txn_id, epoch, seq, len(pid)) + pid
            + PLEN.pack(len(payload)) + payload)
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return HEADER.pack(MAGIC, 4 + len(body)) + struct.pack(">I", crc) + body


class TornRecord(Exception):
    """Framing/checksum failure at a position; a torn-tail candidate."""


class SegmentCorruption(Exception):
    """Garbage bytes followed by a valid record: definite corruption."""


def parse_one(data, pos):
    """Parse the record at byte ``pos``; returns (Record, end_pos).

    Raises TornRecord for any framing/checksum failure.
    """
    if pos + HEADER.size > len(data):
        raise TornRecord(
            f"short header at {pos} ({len(data) - pos} bytes left)")
    magic, length = HEADER.unpack_from(data, pos)
    if magic != MAGIC:
        raise TornRecord(f"bad magic at {pos}")
    if length < 4 or length > MAX_RECORD_BYTES:
        raise TornRecord(f"implausible record length {length} at {pos}")
    end = pos + HEADER.size + length
    if end > len(data):
        raise TornRecord(
            f"short record at {pos}: need {length}, have "
            f"{len(data) - pos - HEADER.size}")
    (crc_stored,) = struct.unpack_from(">I", data, pos + HEADER.size)
    body = data[pos + HEADER.size + 4:end]
    if (zlib.crc32(body) & 0xFFFFFFFF) != crc_stored:
        raise TornRecord(f"crc mismatch at {pos}")
    if len(body) < BODY_FIXED.size + PLEN.size:
        raise TornRecord(f"short body at {pos}")
    offset, txn_id, epoch, seq, pid_len = BODY_FIXED.unpack_from(body, 0)
    rest = BODY_FIXED.size + pid_len
    if rest + PLEN.size > len(body):
        raise TornRecord(f"producer field overruns body at {pos}")
    (payload_len,) = PLEN.unpack_from(body, rest)
    if rest + PLEN.size + payload_len != len(body):
        raise TornRecord(f"payload length mismatch at {pos}")
    pid = body[BODY_FIXED.size:rest].decode("utf-8", "replace")
    payload = body[rest + PLEN.size:]
    return Record(offset, txn_id, epoch, seq, pid, payload, pos, end), end


def _has_valid_record_after(data, pos):
    """True if any valid record can be parsed strictly after ``pos``."""
    i = data.find(MAGIC, pos + 1)
    while i != -1:
        try:
            parse_one(data, i)
            return True
        except TornRecord:
            i = data.find(MAGIC, i + 1)
    return False


def parse_segment(data):
    """Parse a whole segment image.

    Returns (records, good_end): ``records`` are the valid records and
    ``good_end`` is the byte position just past the last valid record.
    Trailing garbage with no valid record after it is reported as a torn
    tail (records stop, good_end < len(data)).  Garbage followed by a valid
    record raises SegmentCorruption.
    """
    records = []
    pos = 0
    while pos < len(data):
        try:
            rec, end = parse_one(data, pos)
        except TornRecord as e:
            if _has_valid_record_after(data, pos):
                raise SegmentCorruption(
                    f"corrupt record at byte {pos} followed by valid "
                    f"data: {e}")
            return records, pos
        records.append(rec)
        pos = end
    return records, pos


def segment_name(base_offset):
    return f"{base_offset:020d}.seg"


def partition_dirname(partition):
    return f"p{partition}"


def write_all(fd, data):
    mv = memoryview(data)
    while mv:
        n = os.write(fd, mv)
        mv = mv[n:]


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
