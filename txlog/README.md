# txlog — transactional partitioned local log

`txlog` is a local-disk **partitioned log** with **cross-partition
transactions**.  Producers hand off a group of records spanning several
partitions as one atomic transaction; analysts choose to read the raw
appends (`read_uncommitted`) or only committed data (`read_committed`).
It is *not* page-level transactions over relational tables and it does not
do consumer-group scheduling: the point of the design is to show how real
segment bytes and pending transactions constrain the readable frontier of
a log.

* Record bodies live only in **custom segment files** (length-framed,
  CRC32-checked).  SQLite stores **metadata only**: producer epochs,
  transaction state, dedup identity, sequence state, the segment catalog
  and per-partition watermarks.
* A single SQLite transaction flips a transaction's state, so a
  cross-partition commit becomes visible **atomically on all partitions**.
* Process-level mutual exclusion: one writer domain per log directory
  (flock), readers protected against segment deletion (shared/exclusive
  flock).
* Injectable **real exit points** (`os._exit`) between file append, fsync
  and metadata commit give deterministic crash-recovery outcomes.

## Layout

```
<dir>/
  meta.sqlite3            # metadata only (WAL, synchronous=FULL)
  writer.lock             # exclusive writer-domain flock
  retain.lock             # shared (fetch) / exclusive (retain) flock
  p0/00000000000000000000.seg   # segment: base offset in the name
  p0/00000000000000000128.seg
  p1/...
```

### Segment record format (big-endian)

```
record := MAGIC(4) LENGTH(4) CRC32(4) BODY
MAGIC  = b"TXL1"
LENGTH = bytes after LENGTH (4 + len(BODY))
CRC32  = zlib.crc32(BODY)
BODY   := OFFSET(u64) TXN_ID(i64) EPOCH(i64) SEQ(i64)
          PRODUCER_LEN(u16) PRODUCER(utf-8) PAYLOAD_LEN(u32) PAYLOAD
```

Non-transactional records use `TXN_ID=0, EPOCH=-1, SEQ=-1, PRODUCER=""`.
Offsets are contiguous per partition; aborted transactions leave **holes**
(their records stay in place, later records are never moved).

## API

```python
import txlog

log = txlog.open_log("./log", segment_bytes=1<<20, dedup_window=4096)
p   = log.producer("etl-1")          # or txlog.open_producer("./log", "etl-1")

p.begin()                            # -> txn_id (idempotent retry-safe)
p.append(0, b"rec-a", seq=0)         # transactional append -> offset
p.append(1, b"rec-b", seq=0)
p.commit()                           # atomic visibility on partitions 0+1
# p.abort()                          # or leave holes

log.append(0, b"plain")              # non-transactional append -> offset

page = log.fetch(0, offset=0, max_records=100,
                 isolation="read_committed")   # or "read_uncommitted"
log.retain(0, before_offset=10_000)
log.inspect(partition=0, raw=True)
log.txn_state(txn_id)                # retry-confirm for lost receipts
```

### Producer epochs and fencing

`open_producer`/`log.producer(id)` bumps the producer's **epoch** and, in
the *same* durable SQLite transaction, **aborts every leftover open
transaction** of that producer (the explicit persistent fencing flow).
All operations of older epochs fail immediately with `ProducerFenced`.
One producer has at most one open transaction.  Per-partition sequence
numbers run `0,1,2,...` continuously **within one epoch** (across its
transactions) and restart for a new epoch.

### Retry identity and the dedup window (API contract)

`(epoch, partition, seq)` identifies a transactional append:

* retry with **same content** → returns the **original offset**, no rewrite;
* **different content** → `ContentConflict`;
* **skipped sequence** → `SequenceGap`;
* sequence **below the window lower bound** `next_seq - dedup_window` →
  `SequenceTooOld` — rejected, never re-appended.  The bound is per
  (producer, epoch, partition) and is exposed via `inspect()`
  (`sequences[].min_retained_seq`) and in the error message.

Non-transactional appends have **no retry identity**: visible to
`read_committed` once durable; a retried call after a lost receipt may
duplicate.  Unreturned `begin`/`commit`/`abort` calls can be confirmed by
retry (idempotent) or via `txn_state(txn_id)`.

### Fetch semantics

`fetch` returns a bounded page plus `next_offset`:

* `read_uncommitted`: every record, each tagged `open`/`committed`/
  `aborted`/`None`.
* `read_committed`: committed + non-transactional records only, and never
  past the partition's **last stable offset** (first offset of the
  earliest pending transaction).  Aborted records are skipped but their
  positions are preserved (`next_offset` advances over holes).
* `reason`: `page_full` | `log_end` | `stable_frontier` (blocked by a
  pending transaction; the frontier is reported, never claimed to be the
  permanent end) | `scan_limit` | `retained_boundary`.
* Page length counts **delivered business records**; the physical scan is
  bounded by `max_scan` so long abort holes cannot accumulate work without
  bound (the page returns early with `scan_limit` and an advanced
  `next_offset`).
* One fetch evaluates the whole page against **one snapshot** of the
  transaction states; a commit landing mid-fetch cannot mix two states
  into the page.  Segment files cannot be deleted under an in-flight read
  (shared `retain.lock`).

### Retention

`retain(partition, before_offset)` requests cleanup and returns the
**actually applied boundary** plus evidence (`deleted_segments`,
`constrained_by`, `lso`, `dedup_floor`, `high_watermark`).  The applied
boundary never crosses:

* the partition's **stable frontier** (pending transactions keep data);
* the **dedup floor** (oldest offset still covered by a retry identity —
  retry identity is never lost while inside its window).

Only sealed segments fully below the boundary are unlinked; the frontier
itself is logical and exact.  Decided-transaction metadata is
garbage-collected only once **every** partition the transaction touched
has retained past its records — one partition cleaning ahead can never
destroy another partition's visibility evidence.

### Durability contract

* `append` returns after: record written → segment **fsync** → metadata
  committed (SQLite `synchronous=FULL`).  Acknowledged records are never
  lost and never change offset.
* `commit`/`abort` return after the state flip is durable; all data
  records were already fsynced by their appends, so the whole transaction
  becomes visible atomically.
* Crash between segment write/fsync and metadata commit leaves an
  unacknowledged tail: recovery **truncates** it and the retried append
  reuses the same offset.

### Recovery rules

On writable open, recovery reconciles catalog ↔ disk and **validates every
byte** (framing + CRC32 + offset contiguity):

* torn tail of the *active* segment (crash mid-append) → truncate, logged
  in `recovery_report`;
* checksum failure or garbage **followed by a valid record**, damage in a
  **sealed** segment, **missing** cataloged segment, acknowledged records
  missing from disk → `CorruptionError`, never passed off as a clean
  truncation;
  read-only opens (`writable=False`) skip recovery so corrupt logs stay
  inspectable via `inspect(raw=True)`.

## CLI

```
txlog init DIR [--segment-bytes N] [--dedup-window N]
txlog open-producer DIR PRODUCER
txlog begin DIR PRODUCER --epoch E
txlog append DIR PRODUCER --epoch E --partition P --seq S --data X
txlog append DIR --partition P --data X          # non-transactional
txlog commit|abort DIR PRODUCER --epoch E [--txn-id T]
txlog fetch DIR --partition P [--offset O] [--max-records N]
              [--isolation read_committed|read_uncommitted] [--max-scan N]
txlog retain DIR --partition P --before-offset N
txlog inspect DIR [--partition P] [--raw]        # raw segments, txns, retention evidence
txlog txn-state DIR --txn-id T
```

All commands print JSON.  `inspect --raw` dumps every segment record with
its transaction state and CRC status — the diagnostic view of raw
segments, transactions and retention evidence.

## Fault injection (crash testing)

`TXLOG_FAULT=<point>[:<n>]` makes the process `os._exit(42)` at the n-th
hit of a named point:

```
append.after_write  append.after_fsync  append.after_meta
roll.after_file     roll.after_catalog
begin.after_meta
commit.before_decision  commit.after_decision
abort.before_decision   abort.after_decision
open_producer.after_fence
retain.after_catalog    retain.after_delete
fetch.after_snapshot    (test-hook only)
```

## Tests

```
cd txlog
python3 -m unittest discover -s tests -t . -v
python3 scripts/stress.py --small            # tiny fixture, seconds
python3 scripts/stress.py --ops 20000 --writers 3 --readers 4
```

* `tests/test_acceptance.py` — starts from a **handwritten** log (segment
  bytes packed by hand): pending T1 at offset 0 blocks committed T2 at
  offset 1; aborting T1 releases offset 1 with `next_offset` advancing by
  original positions.
* `tests/test_model_diff.py` — an **independent serial state model**
  explains full random operation histories; fetch contents, offsets and
  stable frontiers are compared at every step.
* `tests/test_recovery.py` — cross-partition commit killed midway,
  lost-receipt retries, epoch changes, retain crashes (real `os._exit`).
* `tests/test_retain_fetch.py` — fetch/retain race, long abort holes,
  snapshot stability, retention constraints.
* `tests/test_corruption.py` — half-record tails, mid-segment checksum
  damage, missing segments.
* `scripts/stress.py` — concurrent workload generator with online and
  post-recovery invariant verification.
