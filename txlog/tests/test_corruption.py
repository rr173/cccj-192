"""Corruption detection: torn tails, checksum failures, missing segments.

Recovery must repair the *expected* damage of a crash (a half-written
record at the tail of the active segment) but must never pass definite
corruption off as a clean truncation.
"""

import os
import unittest

import txlog
from tests.helpers import LogTestCase
from txlog import storage
from txlog.errors import CorruptionError


class TestCorruption(LogTestCase):
    SEGMENT_BYTES = 4096

    def _seg_path(self, partition, base):
        return os.path.join(
            self.dir, storage.partition_dirname(partition),
            storage.segment_name(base))

    def test_torn_tail_is_truncated_and_reported(self):
        log = self.make_log()
        for i in range(3):
            log.append(0, f"r{i}".encode())
        log.close()
        # simulate a crash mid-append: half a record at the tail
        with open(self._seg_path(0, 0), "ab") as fh:
            fh.write(b"TXL1\x00\x00\x00\x40partial")
        log = self.make_log()
        self.assertTrue(any("truncated" in e for e in log.recovery_report),
                        log.recovery_report)
        page = log.fetch(0, 0, isolation="read_uncommitted")
        self.assertEqual([r["offset"] for r in page["records"]], [0, 1, 2])
        # log keeps working; the next append reuses the frontier offset
        self.assertEqual(log.append(0, b"r3"), 3)

    def test_torn_tail_garbage_without_magic(self):
        log = self.make_log()
        log.append(0, b"r0")
        log.close()
        with open(self._seg_path(0, 0), "ab") as fh:
            fh.write(os.urandom(17))
        log = self.make_log()
        self.assertTrue(any("truncated" in e for e in log.recovery_report))
        self.assertEqual(log.fetch(0, 0, isolation="read_uncommitted")
                         ["high_watermark"], 1)

    def test_garbage_followed_by_valid_record_is_corruption(self):
        log = self.make_log()
        for i in range(3):
            log.append(0, f"r{i}".encode())
        log.close()
        # splice: garbage bytes, then a valid record (offset 99)
        valid = storage.encode_record(99, 0, -1, -1, "", b"alien")
        with open(self._seg_path(0, 0), "ab") as fh:
            fh.write(b"\x00\x01\x02\x03garbage")
            fh.write(valid)
        with self.assertRaises(CorruptionError):
            self.make_log()

    def test_mid_segment_checksum_failure_is_corruption(self):
        log = self.make_log()
        for i in range(6):
            log.append(0, f"record-{i}".encode())
        log.close()
        path = self._seg_path(0, 0)
        with open(path, "r+b") as fh:
            data = fh.read()
            # flip a payload byte of the third record
            recs, _ = storage.parse_segment(data)
            pos = recs[2].pos + 20
            fh.seek(pos)
            fh.write(bytes([data[pos] ^ 0xFF]))
        with self.assertRaises(CorruptionError):
            self.make_log()

    def test_acknowledged_tail_record_damaged_is_corruption(self):
        # crc failure at the very tail, but the record WAS acknowledged:
        # recovery must refuse to silently drop acknowledged data.
        log = self.make_log()
        log.append(0, b"only-record")
        log.close()
        path = self._seg_path(0, 0)
        with open(path, "r+b") as fh:
            fh.seek(-1, os.SEEK_END)
            (last,) = fh.read(1)
            fh.seek(-1, os.SEEK_END)
            fh.write(bytes([last ^ 0xFF]))
        with self.assertRaises(CorruptionError):
            self.make_log()

    def test_sealed_segment_torn_tail_is_corruption(self):
        # tiny segments so some get sealed
        log = self.make_log(segment_bytes=150)
        for i in range(10):
            log.append(0, f"record-{i}".encode())
        info = log.inspect(partition=0)["partitions"][0]
        sealed = [s for s in info["segments"] if s["state"] == "sealed"]
        self.assertTrue(sealed, "expected rolled segments")
        log.close()
        # damage the tail of a sealed segment
        path = os.path.join(self.dir, sealed[-1]["path"])
        with open(path, "r+b") as fh:
            fh.seek(-3, os.SEEK_END)
            (b,) = fh.read(1)
            fh.seek(-3, os.SEEK_END)
            fh.write(bytes([b ^ 0xFF]))
        with self.assertRaises(CorruptionError):
            self.make_log()

    def test_missing_segment_is_corruption(self):
        log = self.make_log(segment_bytes=150)
        for i in range(10):
            log.append(0, f"record-{i}".encode())
        info = log.inspect(partition=0)["partitions"][0]
        sealed = [s for s in info["segments"] if s["state"] == "sealed"]
        self.assertTrue(sealed)
        log.close()
        os.unlink(os.path.join(self.dir, sealed[0]["path"]))
        with self.assertRaisesRegex(CorruptionError, "missing segment"):
            self.make_log()

    def test_missing_active_segment_is_corruption(self):
        log = self.make_log()
        log.append(0, b"x")
        log.close()
        os.unlink(self._seg_path(0, 0))
        with self.assertRaises(CorruptionError):
            self.make_log()

    def test_corrupt_log_still_inspectable_readonly(self):
        log = self.make_log()
        for i in range(3):
            log.append(0, f"r{i}".encode())
        log.close()
        path = self._seg_path(0, 0)
        with open(path, "r+b") as fh:
            data = fh.read()
            recs, _ = storage.parse_segment(data)
            pos = recs[1].pos + 15
            fh.seek(pos)
            fh.write(bytes([data[pos] ^ 0xFF]))
        # writable open refuses; read-only diagnostics still work
        with self.assertRaises(CorruptionError):
            self.make_log()
        diag = txlog.Log(self.dir, writable=False)
        self.logs.append(diag)
        raw = diag.inspect(partition=0, raw=True)
        seg = raw["partitions"][0]["segments"][0]
        self.assertIn("error", seg["records"])


if __name__ == "__main__":
    unittest.main()
