"""Acceptance test built on a HANDWRITTEN log: segment bytes and metadata
are constructed directly (not through the append API), proving the on-disk
format is real and documented.

Scenario (from the requirements):
  partition 0, offset 0 -> record of pending transaction T1
  partition 0, offset 1 -> record of committed transaction T2
  * read_committed must NOT deliver offset 1 while T1 is pending;
  * after T1 is aborted, fetch delivers offset 1 and next_offset advances
    by the original positions (the aborted record stays as a hole).
"""

import os
import sqlite3
import struct
import unittest
import zlib

from tests.helpers import LogTestCase
import txlog


def hand_record(offset, txn_id, epoch, seq, producer, payload):
    """Independently packed record (mirrors the documented format)."""
    pid = producer.encode("utf-8")
    body = (struct.pack(">QqqqH", offset, txn_id, epoch, seq, len(pid))
            + pid + struct.pack(">I", len(payload)) + payload)
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return (b"TXL1" + struct.pack(">I", 4 + len(body))
            + struct.pack(">I", crc) + body)


SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE producers (producer_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL);
CREATE TABLE transactions (
    txn_id INTEGER PRIMARY KEY AUTOINCREMENT, producer_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open','committed','aborted')),
    created_at REAL NOT NULL, decided_at REAL);
CREATE TABLE txn_parts (txn_id INTEGER NOT NULL, partition INTEGER NOT NULL,
    first_offset INTEGER NOT NULL, last_offset INTEGER NOT NULL,
    PRIMARY KEY (txn_id, partition));
CREATE TABLE dedup (producer_id TEXT NOT NULL, epoch INTEGER NOT NULL,
    partition INTEGER NOT NULL, seq INTEGER NOT NULL, offset INTEGER NOT NULL,
    crc INTEGER NOT NULL, txn_id INTEGER NOT NULL,
    PRIMARY KEY (producer_id, epoch, partition, seq));
CREATE TABLE seq_state (producer_id TEXT NOT NULL, epoch INTEGER NOT NULL,
    partition INTEGER NOT NULL, next_seq INTEGER NOT NULL,
    min_retained_seq INTEGER NOT NULL,
    PRIMARY KEY (producer_id, epoch, partition));
CREATE TABLE partitions (partition INTEGER PRIMARY KEY,
    next_offset INTEGER NOT NULL, active_segment INTEGER NOT NULL,
    retained_below INTEGER NOT NULL);
CREATE TABLE segments (partition INTEGER NOT NULL, base_offset INTEGER NOT NULL,
    path TEXT NOT NULL, max_offset INTEGER NOT NULL, bytes INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active','sealed','deleted')),
    PRIMARY KEY (partition, base_offset));
"""


def build_handwritten_log(root):
    os.makedirs(os.path.join(root, "p0"))
    rec0 = hand_record(0, txn_id=1, epoch=1, seq=0,
                       producer="hand1", payload=b"t1-pending")
    rec1 = hand_record(1, txn_id=2, epoch=1, seq=0,
                       producer="hand2", payload=b"t2-committed")
    data = rec0 + rec1
    seg_rel = os.path.join("p0", "00000000000000000000.seg")
    with open(os.path.join(root, seg_rel), "wb") as fh:
        fh.write(data)
    con = sqlite3.connect(os.path.join(root, "meta.sqlite3"))
    con.executescript(SCHEMA)
    con.execute("INSERT INTO meta VALUES ('format', '1')")
    con.execute("INSERT INTO meta VALUES ('segment_bytes', '4096')")
    con.execute("INSERT INTO meta VALUES ('dedup_window', '8')")
    con.execute("INSERT INTO producers VALUES ('hand1', 1)")
    con.execute("INSERT INTO producers VALUES ('hand2', 1)")
    con.execute("INSERT INTO transactions VALUES "
                "(1, 'hand1', 1, 'open', 1.0, NULL)")
    con.execute("INSERT INTO transactions VALUES "
                "(2, 'hand2', 1, 'committed', 1.0, 2.0)")
    con.execute("INSERT INTO txn_parts VALUES (1, 0, 0, 0)")
    con.execute("INSERT INTO txn_parts VALUES (2, 0, 1, 1)")
    con.execute("INSERT INTO partitions VALUES (0, 2, 0, 0)")
    con.execute("INSERT INTO segments VALUES (0, 0, ?, 1, ?, 'active')",
                (seg_rel, len(data)))
    con.commit()
    con.close()


class TestHandwrittenAcceptance(LogTestCase):
    def test_pending_blocks_committed_then_abort_releases(self):
        build_handwritten_log(self.dir)
        log = self.make_log()

        # 1. read_committed cannot deliver offset 1: the earliest pending
        #    transaction pins the stable frontier at offset 0.
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual(page["records"], [])
        self.assertEqual(page["reason"], "stable_frontier")
        self.assertEqual(page["last_stable_offset"], 0)
        self.assertEqual(page["next_offset"], 0)
        self.assertEqual(page["high_watermark"], 2)

        # 2. read_uncommitted sees both records with their txn status.
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["offset"] for r in page["records"]], [0, 1])
        self.assertEqual([r["txn_status"] for r in page["records"]],
                         ["open", "committed"])
        self.assertEqual(page["reason"], "log_end")

        # 3. Abort T1 via the persistent fencing flow: reopening producer
        #    "hand1" bumps its epoch and durably aborts the leftover txn.
        p = log.producer("hand1")
        self.assertEqual(p.epoch, 2)
        self.assertEqual(log.txn_state(1)["state"], "aborted")

        # 4. read_committed now delivers offset 1; next_offset advances by
        #    the original positions (offset 0 remains a physical hole).
        page = log.fetch(0, 0, isolation="read_committed")
        self.assertEqual([r["offset"] for r in page["records"]], [1])
        self.assertEqual(page["records"][0]["payload"], b"t2-committed")
        self.assertEqual(page["next_offset"], 2)
        self.assertEqual(page["reason"], "log_end")

        # 5. The aborted record is still on disk as evidence.
        raw = log.inspect(partition=0, raw=True)
        segs = raw["partitions"][0]["segments"]
        self.assertEqual(len(segs), 1)
        recs = segs[0]["records"]["records"]
        self.assertEqual([r["offset"] for r in recs], [0, 1])
        self.assertEqual(recs[0]["txn_state"], "aborted")
        self.assertEqual(recs[1]["txn_state"], "committed")

        # 6. read_uncommitted marks the hole as aborted but keeps position.
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["txn_status"] for r in page["records"]],
                         ["aborted", "committed"])

    def test_handwritten_bytes_survive_reopen(self):
        build_handwritten_log(self.dir)
        log = self.make_log()
        log.close()
        log2 = self.reopen(log)
        page = log2.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["offset"] for r in page["records"]], [0, 1])
        self.assertEqual(page["records"][1]["payload"], b"t2-committed")


if __name__ == "__main__":
    unittest.main()
