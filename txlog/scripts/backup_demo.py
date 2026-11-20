"""Operator walk-through: online backup, disk loss and offline restore.

This script is fully self-contained and generates all data at runtime
(nothing large is written to disk).  It reproduces the operational story:

  1. build a multi-partition log with recyclable old segments, a reader
     that acked later entries but left an earlier gap, and a pending
     cross-partition transaction;
  2. take recovery point #1, paused after its source view is captured;
  3. while the backup is paused: commit the transaction, fill the reader
     gap, append to active segments (forcing rolls) and really delete
     eligible old segments from the live directory;
  4. finish point #1 and take point #2;
  5. move the source directory away (simulate the failed disk) and
     restore BOTH points from the repository alone, checking the raw and
     committed pages, next write offsets and reader safe positions
     against two predictions written down up front;
  6. delete point #1 and reclaim its exclusive blocks, proving point #2
     still verifies and restores from the shared blocks.

Run:  python3 scripts/backup_demo.py [--keep]
"""

import argparse
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import txlog  # noqa: E402
from txlog.backup import open_repository  # noqa: E402


def hr(title):
    print("\n=== %s" % title)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true",
                    help="keep the generated directories instead of "
                         "cleaning them up")
    args = ap.parse_args()

    work = tempfile.mkdtemp(prefix="txlog-backup-demo-")
    src = os.path.join(work, "live-log")
    repo = os.path.join(work, "backup-repo")
    print("work directory:", work)

    # ---- 1. fixture ------------------------------------------------------
    hr("1. build the source log")
    log = txlog.Log(src, segment_bytes=256, dedup_window=64)
    old = log.producer("ingest")
    old.begin()
    old.append(0, b"old-0", seq=0)
    old.append(0, b"old-1", seq=1)
    old.commit()
    producer = log.producer("ingest")          # fence the old epoch away
    for i in range(24):
        log.append(0, ("p0-%02d-" % i).encode() + b"h" * 48)
    for i in range(8):
        log.append(1, ("p1-%d" % i).encode())
    reader = log.create_reader("analyst", start=0)
    delivered = [r["offset"]
                 for r in reader.poll(1, max_records=100)["records"]]
    for off in delivered:
        if off != 1:
            reader.ack(1, off)
    print("reader 'analyst' partition 1 before:",
          reader.status(1)["partitions"][1])
    for i in range(4):
        log.append(3, ("p3-%d" % i).encode())
    producer.begin()
    producer.append(0, b"pending-on-p0", seq=0)
    producer.append(3, b"pending-on-p3", seq=0)
    pending = producer.current_txn
    log.append(3, b"after-pending")
    print("pending cross-partition transaction:", pending,
          "on partitions 0 and 3")

    repository = open_repository(repo)

    # ---- 2/3. paused first point with source mutations ------------------
    hr("2/3. capture point #1, then mutate the source while it is paused")
    predictions = {}

    def pause(view):
        p0 = view["partitions"]["0"]
        p1v = view["partitions"]["1"]
        p3 = view["partitions"]["3"]
        predictions["point1"] = {
            "p0_next": p0["next_offset"], "p0_retained": 0,
            "p3_next": p3["next_offset"],
            "reader_safe": 1, "reader_gap_present": True,
            "txn_state": "open",
            "partitions": sorted(view["partitions"], key=int)}
        print("prediction point #1:", json.dumps(predictions["point1"]))

        producer.commit()
        reader.ack(1, 1)
        for _ in range(16):
            log.append(0, b"z" * 60)
        log.append(1, b"p1-extra")
        log.append(3, b"p3-extra")
        deleted = log.retain(0, before_offset=10_000)["deleted_segments"]
        print("segments really unlinked from the live source during pause:",
              len(deleted))
        log.append(9, b"new-partition")
        predictions["deleted"] = [d["path"] for d in deleted]

    point1 = repository.create_backup(log, "point-1", after_capture=pause)
    print("point #1 published; segment bytes new/reused:",
          point1["stats"]["bytes_new"], "/", point1["stats"]["bytes_reused"])

    # ---- 4. second point -------------------------------------------------
    hr("4. capture point #2 after the source moved on")
    point2 = repository.create_backup(log, "point-2")
    print("point #2 stats:", point2["stats"])
    predictions["point2"] = {
        "p0_retained": point2["partitions"]["0"]["retained_below"],
        "p0_next": point2["partitions"]["0"]["next_offset"],
        "txn_state": "committed", "reader_gap_present": False,
        "partitions": sorted(point2["partitions"], key=int)}
    print("prediction point #2:", json.dumps(predictions["point2"]))

    repository.verify_point("point-1")
    repository.verify_point("point-2")

    # ---- 5. lose the disk and restore from the repository ---------------
    hr("5. move the source away and restore both points offline")
    log.close()
    shutil.move(src, os.path.join(work, "source-removed"))
    t1 = os.path.join(work, "restored-1")
    t2 = os.path.join(work, "restored-2")
    r1 = repository.restore_point("point-1", t1)
    r2 = repository.restore_point("point-2", t2)
    print("both restored logs got NEW identities:",
          r1["new_log_id"] != r1["source_log_id"],
          r1["new_log_id"] != r2["new_log_id"])

    rl1 = txlog.Log(t1, writable=False)
    assert rl1.txn_state(pending)["state"] == "open"
    assert rl1.inspect(0)["partitions"][0]["retained_below"] == 0
    for rel in predictions["deleted"]:
        assert os.path.exists(os.path.join(t1, rel)), rel
    gap1 = rl1.list_readers()["analyst"]["partitions"][1]
    assert gap1["safe_offset"] == 1 and gap1["blocking_gaps"]
    print("point #1 restore OK: pending txn still open, deleted old "
          "segments restored, reader gap still present:", gap1)
    # ordinary producer contract aborts the leftover transaction
    rl1.close()
    fenced = txlog.open_producer(t1, "ingest")
    rl1w = txlog.Log(t1, writable=False)
    assert rl1w.txn_state(pending)["state"] == "aborted"
    print("reopening producer 'ingest' fenced and aborted the leftover txn")
    fenced.log.close()
    rl1w.close()

    rl2 = txlog.Log(t2, writable=False)
    assert rl2.txn_state(pending)["state"] == "committed"
    boundary = rl2.fetch(0, 0, max_records=1)
    assert boundary["reason"] == "retained_boundary"
    assert boundary["retained_below"] == predictions["point2"]["p0_retained"]
    c0 = rl2.fetch(0, boundary["retained_below"], 1000)
    c3 = rl2.fetch(3, 0, 1000)
    assert any(r["txn_id"] == pending for r in c0["records"])
    assert any(r["txn_id"] == pending and r["txn_status"] == "committed"
               for r in c3["records"])
    gap2 = rl2.list_readers()["analyst"]["partitions"][1]
    assert not gap2["blocking_gaps"] and gap2["safe_offset"] == 8
    print("point #2 restore OK: only retained range readable, whole "
          "transaction committed, reader gap gone:", gap2)
    rl2.close()

    # ---- 6. delete the old point and prove the new one survives ---------
    hr("6. delete point #1 and reclaim its exclusive blocks")
    report = repository.delete_point("point-1")
    print("released %d exclusive blocks (%d bytes)"
          % (len(report["released_blocks"]), report["released_bytes"]))
    repository.verify_point("point-2")
    t2b = os.path.join(work, "restored-2-again")
    repository.restore_point("point-2", t2b)
    print("point #2 still verifies and restores from the shared blocks")
    print("gc finds nothing left:", repository.gc())

    if args.keep:
        print("\nkept:", work)
    else:
        shutil.rmtree(work, ignore_errors=True)
        print("\ncleaned up", work)
    print("DEMO OK")


if __name__ == "__main__":
    main()
