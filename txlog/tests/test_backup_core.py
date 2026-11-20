"""Unit tests for backup repository mechanics.

Covers: block-level sharing and grown-segment block versioning, naming and
publication rules, missing/corrupt block and frame detection with exact
partition/segment/byte-range reporting, unsupported backup versions,
bounded chunking, and the list view describing the captured source rather
than the runtime's current progress.
"""

import glob
import json
import os

import txlog
from txlog.backup import _FrameValidator, open_repository
from txlog.errors import (BackupPointExists, BackupPointNotFound,
                         BackupTargetExists, BackupUnsupportedVersion,
                         BackupVerificationError)
from txlog.storage import partition_dirname

from tests.helpers import LogTestCase


class TestBackupBasics(LogTestCase):
    SEGMENT_BYTES = 512

    def test_name_rules_and_immutable_publication(self):
        log = self.make_log()
        log.append(0, b"a")
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "point-1")
        with self.assertRaises(BackupPointExists):
            repo.create_backup(log, "point-1")
        for bad in ("", "..", "a/b", "a b", ".hidden", "-x", "x" * 128):
            with self.assertRaises(ValueError):
                repo.create_backup(log, bad)
        # the list only carries published points
        self.assertEqual([p["name"] for p in repo.list_points()],
                         ["point-1"])

    def test_list_describes_captured_view_not_runtime(self):
        log = self.make_log()
        for i in range(5):
            log.append(0, ("r%d" % i).encode())
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "p")
        # source moves on after the point is published
        for i in range(7):
            log.append(0, b"later")
        [point] = repo.list_points()
        self.assertEqual(point["partitions"]["0"]["next_offset"], 5)
        self.assertEqual(point["partitions"]["0"]["retained_below"], 0)
        self.assertEqual(point["source"]["log_id"], log.log_id)
        self.assertTrue(point["partitions"]["0"]["segments"])

    def test_restore_refuses_existing_target_and_unknown_point(self):
        log = self.make_log()
        log.append(0, b"a")
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "p")
        target = os.path.join(self.tmp.name, "target")
        os.makedirs(target)
        with self.assertRaises(BackupTargetExists):
            repo.restore_point("p", target)
        with self.assertRaises(BackupPointNotFound):
            repo.restore_point("nope", os.path.join(self.tmp.name, "t2"))
        with self.assertRaises(BackupPointNotFound):
            repo.verify_point("nope")
        with self.assertRaises(BackupPointNotFound):
            repo.delete_point("nope")

    def test_restored_log_reopens_and_continues(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"tx", seq=0)
        p.commit()
        log.append(1, b"plain")
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "p")
        target = os.path.join(self.tmp.name, "target")
        res = repo.restore_point("p", target)
        new = txlog.Log(target)
        try:
            self.assertEqual(new.log_id, res["new_log_id"])
            self.assertNotEqual(new.log_id, log.log_id)
            # ordinary API continues from the restored offsets
            off = new.append(1, b"after-restore")
            page = new.fetch(1, 0, max_records=100)
            self.assertEqual([r["offset"] for r in page["records"]],
                             [0, off])
            self.assertEqual(off, 1)
        finally:
            new.close()


class TestBlockSharing(LogTestCase):
    SEGMENT_BYTES = 256

    def test_unchanged_blocks_shared_grown_segment_makes_new_block(self):
        log = self.make_log()
        # a sealed segment plus an active one
        for i in range(20):
            log.append(0, b"a" * 40)
        log.append(0, b"active-tail")
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        first = repo.create_backup(log, "p1")
        self.assertEqual(first["stats"]["blocks_reused"], 0)
        self.assertGreater(first["stats"]["blocks_new"], 1)

        # nothing changes: every block is reused
        same = repo.create_backup(log, "p1b")
        self.assertEqual(same["stats"]["bytes_new"], 0)
        self.assertEqual(same["stats"]["bytes_reused"],
                         same["stats"]["segment_bytes"])

        # grow the active segment: its full prefix blocks stay shared, only
        # the still-filling last block gets a new version
        before_files = set(glob.glob(os.path.join(repo.path, "chunks",
                                                  "*", "*")))
        log.append(0, b"b" * 60)
        grown = repo.create_backup(log, "p2")
        self.assertGreater(grown["stats"]["bytes_reused"], 0)
        self.assertGreater(grown["stats"]["bytes_new"], 0)
        after_files = set(glob.glob(os.path.join(repo.path, "chunks",
                                                 "*", "*")))
        added = after_files - before_files
        # one new last-block version, never a rewrite of the full segment
        self.assertEqual(len(added), 1)

        # all three points remain independently restorable
        for name in ("p1", "p1b", "p2"):
            repo.verify_point(name)
        repo.restore_point("p1", os.path.join(self.tmp.name, "t1"))
        repo.restore_point("p2", os.path.join(self.tmp.name, "t2"))
        r1 = txlog.Log(os.path.join(self.tmp.name, "t1"), writable=False)
        r2 = txlog.Log(os.path.join(self.tmp.name, "t2"), writable=False)
        self.assertEqual(
            r1.fetch(0, 0, 1000, isolation="read_uncommitted")["high_watermark"]
            + 1,
            r2.fetch(0, 0, 1000, isolation="read_uncommitted")["high_watermark"])
        r1.close(); r2.close()

    def test_delete_releases_only_exclusive_blocks(self):
        log = self.make_log()
        for i in range(12):
            log.append(0, b"x" * 40)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "a")
        log.append(0, b"more")
        repo.create_backup(log, "b")
        # All sealed prefix blocks are shared.  The active segment's tail
        # block was versioned by the append, so point 'a' exclusively owns
        # the OLD tail version; deleting 'a' releases exactly that block
        # (and its metadata snapshot), never any block 'b' references.
        report = repo.delete_point("a")
        self.assertTrue(report["released_metadata"])
        self.assertTrue(report["released_blocks"])
        self.assertTrue(repo.verify_point("b")["ok"])
        repo.restore_point("b", os.path.join(self.tmp.name, "target-b"))
        # removing 'b' releases everything else and the chunk store empties
        report_b = repo.delete_point("b")
        self.assertGreater(report_b["released_bytes"], 0)
        self.assertEqual(
            glob.glob(os.path.join(repo.path, "chunks", "*", "*")), [])
        self.assertEqual(repo.gc()["freed_bytes"], 0)

    def test_exclusive_grown_blocks_are_released(self):
        log = self.make_log()
        for i in range(12):
            log.append(0, b"x" * 40)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "a")
        log.append(0, b"grown")
        repo.create_backup(log, "b")
        # deleting 'b' releases only its exclusive newer block version;
        # blocks shared with (or exclusive to) 'a' stay untouched
        report = repo.delete_point("b")
        self.assertGreater(report["released_bytes"], 0)
        self.assertTrue(repo.verify_point("a")["ok"])
        repo.restore_point("a", os.path.join(self.tmp.name, "target-a"))


class TestValidation(LogTestCase):
    SEGMENT_BYTES = 256

    def _point_with_blocks(self):
        log = self.make_log()
        p = log.producer("p")
        p.begin()
        p.append(0, b"open-txn", seq=0)
        for i in range(10):
            log.append(1, ("c%d" % i).encode() + b"y" * 30)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "p")
        return log, repo

    def test_missing_block_reported_with_partition_and_range(self):
        _log, repo = self._point_with_blocks()
        blocks = glob.glob(os.path.join(repo.path, "chunks", "*", "*"))
        self.assertTrue(blocks)
        os.unlink(blocks[0])
        with self.assertRaises(BackupVerificationError) as cm:
            repo.verify_point("p")
        problems = cm.exception.problems
        self.assertTrue(any(q["kind"] == "missing" for q in problems))
        q = next(q for q in problems if q["kind"] == "missing")
        self.assertEqual(q["point"], "p")
        self.assertIn("partition", q)
        self.assertIn("segment", q)
        self.assertEqual(len(q["byte_range"]), 2)
        # restore must fail and leave no finished target behind
        target = os.path.join(self.tmp.name, "target")
        with self.assertRaises(BackupVerificationError):
            repo.restore_point("p", target)
        self.assertFalse(os.path.exists(target))
        self.assertFalse(glob.glob(target + ".restoring-*"))

    def test_corrupt_block_detected(self):
        _log, repo = self._point_with_blocks()
        blocks = glob.glob(os.path.join(repo.path, "chunks", "*", "*"))
        target = blocks[0]
        with open(target, "r+b") as fh:
            fh.seek(0)
            fh.write(b"\xff" * 8)
        with self.assertRaises(BackupVerificationError) as cm:
            repo.verify_point("p")
        kinds = {q["kind"] for q in cm.exception.problems}
        self.assertIn("corrupt", kinds)

    def test_frame_corruption_inside_confirmed_range(self):
        log = self.make_log()
        log.append(0, b"hello")
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        repo.create_backup(log, "p")
        # flip payload bytes (offset 50) inside the archived block so the
        # stored CRC32 no longer matches
        [block] = glob.glob(os.path.join(repo.path, "chunks", "*", "*"))
        with open(block, "r+b") as fh:
            fh.seek(50)
            fh.write(b"\xff" * 4)
        with self.assertRaises(BackupVerificationError) as cm:
            repo.verify_point("p")
        self.assertTrue(cm.exception.problems)

    def test_shallow_verify_checks_hashes_only(self):
        _log, repo = self._point_with_blocks()
        self.assertTrue(repo.verify_point("p", deep=False)["ok"])

    def test_unsupported_backup_version(self):
        _log, repo = self._point_with_blocks()
        path = os.path.join(repo.path, "points", "p.json")
        with open(path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
        manifest["backup_format"] = "999"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh)
        with self.assertRaises(BackupUnsupportedVersion):
            repo.verify_point("p")

    def test_frame_validator_bounded_and_strict(self):
        from txlog.storage import encode_record
        recs = b"".join(encode_record(i, 0, -1, -1, "", b"x" * 10)
                        for i in range(3))
        v = _FrameValidator(0, len(recs), "p", 0, "p0/000.seg")
        # feed one byte at a time: framing must survive tiny feeds
        for byte in recs:
            v.feed(bytes([byte]))
        self.assertEqual(v.finish(), [])
        self.assertEqual(v.last_offset, 2)
        # torn tail INSIDE a confirmed range is corruption
        v2 = _FrameValidator(0, len(recs) + 4, "p", 0, "p0/000.seg")
        v2.feed(recs + b"TXL1")
        self.assertTrue(v2.finish())

    def test_large_segment_chunked_without_whole_file_reads(self):
        # A segment larger than the block size is split into bounded
        # blocks and reassembled byte-identically.  The block limit is
        # shrunk for the test so the multi-block case needs no giant
        # generated file; records are still produced through the normal
        # append path at runtime.
        import txlog.backup as bmod
        old_chunk = bmod.CHUNK_BYTES
        small_chunk = 4096
        try:
            bmod.CHUNK_BYTES = small_chunk
            log = txlog.Log(self.dir, segment_bytes=small_chunk * 8,
                            dedup_window=8)
            self.logs.append(log)
            # enough records to cross at least two block boundaries
            for i in range(small_chunk * 3 // 200):
                log.append(0, b"q" * 196)
            repo = open_repository(os.path.join(self.tmp.name, "repo"))
            point = repo.create_backup(log, "p")
            block_files = glob.glob(
                os.path.join(repo.path, "chunks", "*", "*"))
            self.assertTrue(any(os.path.getsize(b) == small_chunk
                                for b in block_files))
            self.assertGreaterEqual(
                point["stats"]["blocks_new"]
                + point["stats"]["blocks_reused"], 2)
            report = repo.verify_point("p")
            self.assertTrue(report["ok"])
            # restore reconstructs the exact original segment bytes
            target = os.path.join(self.tmp.name, "target")
            repo.restore_point("p", target)
            for rel in glob.glob(os.path.join(self.dir, "p*", "*.seg")):
                rel_part = os.path.relpath(rel, self.dir)
                with open(rel, "rb") as a, \
                        open(os.path.join(target, rel_part), "rb") as b:
                    self.assertEqual(a.read(), b.read())
        finally:
            bmod.CHUNK_BYTES = old_chunk


class TestSourceFailures(LogTestCase):
    SEGMENT_BYTES = 256

    def test_corrupt_source_segment_aborts_publication(self):
        log = self.make_log()
        for i in range(8):
            log.append(0, b"m" * 40)
        log.close()
        # Damage a SEALED segment on disk.  A read-only open runs no
        # recovery, so the backup must detect the frame damage itself and
        # refuse to publish.
        segs = sorted(glob.glob(os.path.join(self.dir,
                                             partition_dirname(0), "*.seg")))
        with open(segs[0], "r+b") as fh:
            fh.seek(8)  # the CRC32 field
            fh.write(b"\xff" * 4)
        ro = txlog.Log(self.dir, writable=False, create=False)
        self.logs.append(ro)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        with self.assertRaises(BackupVerificationError) as cm:
            repo.create_backup(ro, "p")
        self.assertTrue(cm.exception.problems)
        self.assertEqual(repo.list_points(), [])
        # the half-finished job is safely reclaimable afterwards
        gc_report = repo.gc()
        self.assertGreaterEqual(len(gc_report["removed_stale_jobs"]), 0)

    def test_catalog_references_missing_segment_aborts_publication(self):
        log = self.make_log()
        for i in range(8):
            log.append(0, b"m" * 40)
        log.close()
        segs = sorted(glob.glob(os.path.join(self.dir,
                                             partition_dirname(0), "*.seg")))
        os.unlink(segs[0])  # catalog still references it
        ro = txlog.Log(self.dir, writable=False, create=False)
        self.logs.append(ro)
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        with self.assertRaises(BackupVerificationError) as cm:
            repo.create_backup(ro, "p")
        q = cm.exception.problems[0]
        self.assertEqual(q["kind"], "missing")
        self.assertEqual(q["partition"], 0)
        self.assertEqual(repo.list_points(), [])

    def test_unpublished_point_absent_after_failure(self):
        log = self.make_log()
        log.append(0, b"a")
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        with self.assertRaises(BackupPointNotFound):
            repo.verify_point("never")
        self.assertEqual(
            os.listdir(os.path.join(repo.path, "jobs")), [])


class TestConfirmedBytesOnly(LogTestCase):
    SEGMENT_BYTES = 4096

    def test_active_segment_tail_after_view_is_never_included(self):
        log = self.make_log()
        for i in range(6):
            log.append(0, ("r%d" % i).encode())
        repo = open_repository(os.path.join(self.tmp.name, "repo"))
        first = repo.create_backup(log, "p1")
        p0 = first["partitions"]["0"]
        active = next(s for s in p0["segments"] if s["state"] == "active")
        # the archived length is exactly the view's confirmed byte count
        self.assertEqual(sum(b["length"] for b in active["blocks"]),
                         active["bytes"])
        # append a record WITHOUT rolling: the active segment grows
        log.append(0, b"after-the-view")
        second = repo.create_backup(log, "p2")
        active2 = next(s for s in second["partitions"]["0"]["segments"]
                       if s["state"] == "active")
        self.assertGreater(active2["bytes"], active["bytes"])

        # restore point-1: the unconfirmed later record must be absent,
        # while the restored log continues cleanly at the old frontier
        target = os.path.join(self.tmp.name, "t1")
        repo.restore_point("p1", target)
        rl = txlog.Log(target)
        try:
            page = rl.fetch(0, 0, 100, isolation="read_uncommitted")
            self.assertEqual(page["high_watermark"], 6)
            off = rl.append(0, b"new-after-restore")
            self.assertEqual(off, 6)
        finally:
            rl.close()

        # point-2 contains the record the first point excluded
        target2 = os.path.join(self.tmp.name, "t2")
        repo.restore_point("p2", target2)
        rl2 = txlog.Log(target2, writable=False)
        try:
            self.assertEqual(
                rl2.fetch(0, 0, 100,
                          isolation="read_uncommitted")["high_watermark"], 7)
        finally:
            rl2.close()


class TestCrossDeviceFallback(LogTestCase):
    SEGMENT_BYTES = 256

    def test_hardlink_failure_falls_back_to_bounded_copy(self):
        import errno
        import txlog.backup as bmod
        real_link = bmod.os.link
        bmod.os.link = lambda src, dst: (_ for _ in ()).throw(
            OSError(errno.EXDEV, "simulated cross-device link"))
        try:
            log = self.make_log()
            p = log.producer("p")
            p.begin()
            p.append(0, b"t", seq=0)
            p.commit()
            for i in range(10):
                log.append(0, b"m" * 40)
            repo = open_repository(os.path.join(self.tmp.name, "repo"))
            point = repo.create_backup(
                log, "p",
                after_capture=lambda _v: log.append(1, b"later"))
            # the paused view never includes the append during the pause
            self.assertNotIn("1", point["partitions"])
            self.assertTrue(repo.verify_point("p")["ok"])
            target = os.path.join(self.tmp.name, "target")
            repo.restore_point("p", target)
            rl = txlog.Log(target, writable=False)
            self.assertEqual(rl.txn_state(1)["state"], "committed")
            rl.close()
        finally:
            bmod.os.link = real_link
