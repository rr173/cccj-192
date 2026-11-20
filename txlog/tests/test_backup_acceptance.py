"""End-to-end acceptance test for online backup / offline restore.

The scenario mirrors an operator replacing the local disk of a running
collection node:

  * a multi-partition log with genuinely deletable OLD segments (an old
    producer has been fenced away, so its dedup entries no longer pin
    retention);
  * a named reader on another partition that has acknowledged LATER
    entries while leaving an earlier gap;
  * one pending cross-partition transaction at a HIGH offset, so old
    sealed segments remain eligible for deletion.

Two predictions are written down from the captured views BEFORE any
mutation happens, then the source is changed while the first backup is
paused after capturing its view: the pending transaction is committed,
the reader gap is filled, active segments receive new appends (and roll),
and eligible old segments are really unlinked from the source.

Afterwards the source directory is moved away and both recovery points
are restored using ONLY the repository; the first must still contain the
deleted old segments and the pending transaction (which is then fenced
and aborted through the ordinary producer contract), while the second
sees only the retained range, the whole committed transaction and a
reader no longer blocked by the filled gap.
"""

import glob
import os
import shutil

import txlog
from txlog.backup import open_repository

from tests.helpers import LogTestCase

SEGMENT_BYTES = 256


class TestBackupAcceptance(LogTestCase):
    SEGMENT_BYTES = SEGMENT_BYTES
    DEDUP_WINDOW = 64

    def _build_source(self):
        log = self.make_log()
        # Old producer: a few transactional records on partition 0, then a
        # new producer of the same id fences it.  The fence deletes the old
        # epoch's dedup/seq rows, so those early segments are not pinned by
        # the dedup window any more.
        old = log.producer("ingest")
        old.begin()
        old.append(0, b"old-epoch-0", seq=0)
        old.append(0, b"old-epoch-1", seq=1)
        old.commit()
        cur = log.producer("ingest")           # epoch bump fences `old`
        self.ingest_epoch = cur.epoch

        # Plenty of non-transactional history on p0 to create sealed
        # segments below the later pending transaction.
        for i in range(24):
            log.append(0, ("p0-%02d-" % i).encode() + b"h" * 48)

        # Partition 1: a reader with an explicit ack GAP (acks 0,2,3,...
        # while offset 1 stays unacked).
        for i in range(8):
            log.append(1, ("p1-%d" % i).encode())
        rdr = log.create_reader("analyst", start=0)
        page = rdr.poll(1, max_records=100)
        delivered = [r["offset"] for r in page["records"]]
        self.assertEqual(delivered, list(range(8)))
        for off in delivered:
            if off != 1:
                rdr.ack(1, off)
        gap_status = rdr.status(1)["partitions"][1]
        self.assertEqual(gap_status["safe_offset"], 1)
        # offset 1 is the unacked gap; 2..7 are the out-of-order acks held
        # back behind it.
        self.assertEqual(gap_status["blocking_gaps"], [2, 3, 4, 5, 6, 7])

        # A little unrelated history on partition 3, then a pending
        # cross-partition transaction spanning 0 and 3 at a HIGH offset.
        for i in range(4):
            log.append(3, ("p3-%d" % i).encode())
        cur.begin()
        cur.append(0, b"pending-on-p0", seq=0)
        cur.append(3, b"pending-on-p3", seq=0)
        self.pending_txn = cur.current_txn
        # records appended AFTER the pending transaction stay in place
        log.append(3, b"after-pending-p3")
        return log, cur, rdr

    # ------------------------------------------------------------------

    def test_full_online_backup_restore_lifecycle(self):
        log, producer, rdr = self._build_source()
        repo_dir = os.path.join(self.tmp.name, "repo")
        repo = open_repository(repo_dir)

        # ---- phase A: first backup pauses after the view is captured ----
        captured = {}

        def pause(view):
            # Record the point-1 expectations BEFORE mutating the source.
            p0 = view["partitions"]["0"]
            p1 = view["partitions"]["1"]
            p3 = view["partitions"]["3"]
            captured["p1_view"] = {
                "partitions": sorted(view["partitions"], key=int),
                "p0_next": p0["next_offset"],
                "p0_retained": p0["retained_below"],
                "p0_segments": [(s["base_offset"], s["bytes"], s["state"])
                                for s in p0["segments"]],
                "p1_next": p1["next_offset"],
                "p3_next": p3["next_offset"],
            }
            self.assertEqual(p0["retained_below"], 0)
            self.assertEqual(
                log.txn_state(self.pending_txn)["state"], "open")

            # 1) commit the cross-partition transaction
            producer.commit()
            # 2) fill the reader's missing ack on partition 1
            rdr.ack(1, 1)
            self.assertEqual(
                rdr.status(1)["partitions"][1]["blocking_gaps"], [])
            # 3) appends to active segments, forcing at least one roll
            for i in range(16):
                log.append(0, b"z" * 60)
            log.append(1, b"p1-extra")
            log.append(3, b"p3-extra")
            # 4) real retention: old sealed p0 segments must be unlinked
            rep = log.retain(0, before_offset=10_000)
            captured["deleted_during_pause"] = [
                d["path"] for d in rep["deleted_segments"]]
            self.assertTrue(
                captured["deleted_during_pause"],
                "fixture must really delete eligible old segments")
            for rel in captured["deleted_during_pause"]:
                self.assertFalse(os.path.exists(os.path.join(log.path, rel)))
            # 5) a brand new partition may never enter the paused point
            log.append(9, b"new-partition")

        point1 = repo.create_backup(log, "point-1", after_capture=pause)
        # point-1 view stayed fixed while the source moved on
        listing = {m["name"]: m for m in repo.list_points()}
        m1 = listing["point-1"]
        self.assertEqual(
            sorted(m1["partitions"], key=int),
            captured["p1_view"]["partitions"])
        self.assertNotIn("9", m1["partitions"])
        self.assertEqual(
            m1["partitions"]["0"]["next_offset"],
            captured["p1_view"]["p0_next"])
        self.assertEqual(m1["partitions"]["0"]["retained_below"], 0)
        # the old segments deleted from the live source are still inside
        # point-1
        archived = {s["path"]
                    for e in m1["partitions"].values()
                    for s in e["segments"]}
        for rel in captured["deleted_during_pause"]:
            self.assertIn(rel, archived)

        # ---- phase B: a second point after all the mutations -------------
        point2 = repo.create_backup(log, "point-2")
        m2 = {m["name"]: m for m in repo.list_points()}["point-2"]
        self.assertIn("9", m2["partitions"])
        self.assertGreater(
            m2["partitions"]["0"]["retained_below"], 0)
        # unchanged sealed blocks were shared; only genuinely new/grown
        # bytes were written for point-2
        self.assertGreater(point2["stats"]["bytes_reused"], 0)
        self.assertGreaterEqual(point2["stats"]["bytes_new"], 0)
        self.assertEqual(
            point1["stats"]["bytes_new"]
            + point1["stats"]["bytes_reused"],
            point1["stats"]["segment_bytes"])

        # both points verify offline, using only the repository
        r1 = repo.verify_point("point-1")
        r2 = repo.verify_point("point-2")
        self.assertTrue(r1["ok"] and r2["ok"])
        self.assertGreater(r1["segments_checked"], 0)

        # ---- phase C: move the source away, restore both points ----------
        moved = os.path.join(self.tmp.name, "log-moved-away")
        shutil.move(log.path, moved)
        log.close()

        target1 = os.path.join(self.tmp.name, "restored-1")
        target2 = os.path.join(self.tmp.name, "restored-2")
        res1 = repo.restore_point("point-1", target1)
        res2 = repo.restore_point("point-2", target2)
        self.assertEqual(res1["source_log_id"], res2["source_log_id"])
        self.assertNotEqual(res1["new_log_id"], res1["source_log_id"])
        self.assertNotEqual(res1["new_log_id"], res2["new_log_id"])
        self.assertFalse(os.path.exists(target1 + ".restoring-x"))

        # ---- point-1 restore: old segments + still-pending txn -----------
        rl1 = txlog.Log(target1, writable=False)
        self.assertNotEqual(rl1.log_id, res1["source_log_id"])
        self.assertEqual(
            rl1.txn_state(self.pending_txn)["state"], "open")
        insp0 = rl1.inspect(0)["partitions"][0]
        self.assertEqual(insp0["retained_below"], 0)
        # the deleted-away old segments came back from the repository
        for rel in captured["deleted_during_pause"]:
            self.assertTrue(os.path.exists(os.path.join(target1, rel)))
        # read_committed is blocked by the still-open transaction on p0
        committed_p0 = rl1.fetch(0, 0, max_records=1000,
                                 isolation="read_committed")
        self.assertLess(
            committed_p0["last_stable_offset"],
            m1["partitions"]["0"]["next_offset"])
        # the raw stream still contains every record, including the ones
        # after the pending transaction and the pending records themselves
        raw3 = rl1.fetch(3, 0, max_records=1000,
                         isolation="read_uncommitted")
        self.assertEqual(
            raw3["high_watermark"],
            captured["p1_view"]["p3_next"])
        self.assertIn(
            self.pending_txn,
            {r["txn_id"] for r in raw3["records"]})
        # the reader gap is still open
        self.assertEqual(
            rl1.list_readers()["analyst"]["partitions"][1],
            {"read_offset": 8, "safe_offset": 1,
             "blocking_gaps": [2, 3, 4, 5, 6, 7]})
        rl1.close()

        # Reopen the producer through the ORDINARY contract: the epoch
        # bump fences the leftover open transaction and aborts it; the raw
        # positions remain as abort holes.
        fenced = txlog.open_producer(target1, "ingest")
        rl1w = txlog.Log(target1, writable=False)
        self.assertEqual(
            rl1w.txn_state(self.pending_txn)["state"], "aborted")
        raw0 = rl1w.fetch(0, 0, max_records=1000,
                          isolation="read_uncommitted")
        hole = next(r for r in raw0["records"]
                    if r["txn_id"] == self.pending_txn)
        self.assertEqual(hole["txn_status"], "aborted")
        # offsets after the hole were never moved
        self.assertEqual(
            raw0["high_watermark"],
            captured["p1_view"]["p0_next"])
        rl1w.close()
        fenced.log.close()

        # ---- point-2 restore: retained range, committed txn, no gap -----
        rl2 = txlog.Log(target2, writable=False)
        self.assertEqual(
            rl2.txn_state(self.pending_txn)["state"], "committed")
        retained2 = m2["partitions"]["0"]["retained_below"]
        boundary = rl2.fetch(0, offset=0, max_records=1)
        self.assertEqual(boundary["reason"], "retained_boundary")
        self.assertEqual(boundary["retained_below"], retained2)
        # the whole cross-partition transaction reads as committed
        c0 = rl2.fetch(0, offset=retained2, max_records=1000,
                       isolation="read_committed")
        c3 = rl2.fetch(3, offset=0, max_records=1000,
                       isolation="read_committed")
        txn0 = {r["offset"] for r in c0["records"]
                if r["txn_id"] == self.pending_txn}
        txn3 = {r["offset"] for r in c3["records"]
                if r["txn_id"] == self.pending_txn}
        self.assertEqual(len(txn0), 1)
        self.assertEqual(len(txn3), 1)
        self.assertTrue(all(r["txn_status"] == "committed"
                            for r in c0["records"] if r["txn_id"]))
        # reader no longer blocked by the gap that was filled
        st = rl2.list_readers()["analyst"]["partitions"][1]
        self.assertEqual(st["blocking_gaps"], [])
        self.assertEqual(st["safe_offset"], 8)
        rl2.close()

        # ---- phase D: delete the old point; the new one survives ---------
        delete_report = repo.delete_point("point-1")
        self.assertGreater(delete_report["released_bytes"], 0)
        self.assertTrue(delete_report["released_blocks"])
        # point-2 is still fully usable: every shared block survived
        ok2 = repo.verify_point("point-2")
        self.assertTrue(ok2["ok"])
        target2b = os.path.join(self.tmp.name, "restored-2-again")
        repo.restore_point("point-2", target2b)
        check = txlog.Log(target2b, writable=False)
        self.assertEqual(
            check.txn_state(self.pending_txn)["state"], "committed")
        check.close()
        # point-1 is gone and cannot be restored
        with self.assertRaises(txlog.BackupPointNotFound):
            repo.verify_point("point-1")
        with self.assertRaises(txlog.BackupPointNotFound):
            repo.restore_point("point-1",
                               os.path.join(self.tmp.name, "nope"))
        # blocks exclusive to point-1 were really released
        remaining = {os.path.basename(p)
                     for p in glob.glob(os.path.join(repo_dir, "chunks",
                                                      "*", "*"))}
        for released in delete_report["released_blocks"]:
            self.assertNotIn(released["sha256"], remaining)
        # gc reports nothing new once the deletion is complete
        self.assertEqual(repo.gc()["freed_bytes"], 0)
