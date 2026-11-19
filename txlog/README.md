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
`constrained_by`, `lso`, `dedup_floor`, `reader_floor`, `reader_holds`,
`high_watermark`).  The applied boundary never crosses:

* the partition's **stable frontier** (pending transactions keep data);
* the **dedup floor** (oldest offset still covered by a retry identity —
  retry identity is never lost while inside its window);
* every **live named reader's safe cursor** on that partition (see below).

Only sealed segments fully below the boundary are unlinked; the frontier
itself is logical and exact.  Decided-transaction metadata is
garbage-collected only once **every** partition the transaction touched
has retained past its records — one partition cleaning ahead can never
destroy another partition's visibility evidence.

## Named, resumable application readers

A bare `fetch` leaves position bookkeeping to the caller; a named
**reader** owns durable, restart-safe read progress with explicit
per-record acknowledgement.  Readers do **not** do consumer-group
assignment, load balancing or background scheduling — they only make
"what has this application safely processed" a durable fact.

```python
r = log.create_reader("etl-1")                 # tail: starts at the
                                               # current retention boundary
r = log.create_reader("etl-2", start=0)        # explicit readable start
r2 = log.create_reader("etl-2", start=0)       # identical -> reused=True
# log.create_reader("etl-2", start=9)          # ReaderConflict: a recreate
#                                              # never silently resets progress

page = r.poll(0, max_records=100)              # read_committed only
for rec in page["records"]:                    # rec carries reader_id,
    process(rec["payload"])                    # partition and offset
    r.ack(0, rec["offset"])                    # idempotent per record
r.status(0)     # safe_offset, read_offset, blocking_gaps
r.replay(0, 5)  # re-deliver from a retained position; acks are untouched
r.delete()      # explicit deletion; releases the reader's retain hold
```

### Cursors: delivery high-water vs. safe cursor

Two distinct numbers are persisted per (reader, partition):

* `read_offset` — every read_committed-visible record below it has been
  *delivered* by some `poll` (durable; used to restart a poller);
* `safe_offset` — the **contiguous acknowledgement cursor**.  Every
  delivered record in `[start, safe_offset)` is acked.  Abort holes are
  skipped but keep their offset semantics.

Acks are stored as a **set of offsets**, never as "highest acked offset".
If offsets 2 and 0 are acked while 1 is not, the safe cursor stays at 1;
acking 1 then advances it continuously to 3 (and over any abort holes).
Out-of-order completion of many records is therefore safe, and multiple
readers independently track the same partition.  Acks are idempotent; the
rejections are classified (`ReaderAckError.reason`):

* `wrong_partition` — the reader never opened that partition;
* `not_delivered` — stable/readable offset that this reader never got;
* `beyond_stable` — past the stable readable frontier;
* `abort_hole` — an aborted-record position, never delivered.

### Restart, redelivery and replay semantics

A new handle (`open_reader`, or a new process) resumes polling at the
safe cursor: a record that was delivered but not acked before a crash is
delivered again (at-least-once).  All progress lives in SQLite
(synchronous=FULL); ack receipts lost after the commit simply return
`duplicate=True` on retry.  `replay(partition, offset)` only moves the
handle's poll position — it never erases acks and old acks can never pull
new progress backwards.  An explicit `poll(offset=...)` is handle-local
peeking and never marks skipped positions as delivered.

If legal retention has already moved past a reader's starting position,
the next poll raises `ReaderDataUnavailable` carrying the recoverable
boundary (`retained_below`); it is never reported as an empty page, a
permanent log end or an automatic jump.  Recover explicitly with
`replay(partition, retained_below)`.

### Readers and retention

`retain` additionally takes the minimum of every live reader's
`safe_offset` (`reader_floor`); a slow reader therefore keeps the
segments it has not finished processing, and `reader_holds` /
`constrained_by="reader"` name the blocking reader in the retain report
and in `inspect()`.  Deleting a reader is an explicit operation that
returns the released holds; the next `retain` then recomputes the
cleanable range.  Segment deletion still requires the exclusive
`retain.lock`, so an in-flight poll can never read an unlinked file.

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

## Local incremental materialization: event assignment details

A *materialized store* turns the raw committed records into a directly
usable "event ownership" detail table.  It lives in its **own separate
SQLite database** (e.g. `./warehouse/mat.sqlite3`), bound once to one txlog
by the log's durable `log_id` (stored in the log's metadata DB; the
producer/fetch/reader/retain contracts are unchanged and record bodies are
never mirrored into the materialization DB).

### Record contract

Committed record bodies are UTF-8 JSON with a `kind`:

* `event` — `entity` (non-empty string), integer `event_time`;
* `assignment` — `entity`, integer `effective_from`, non-negative integer
  `revision`, non-empty `group`.

An event's business identity is its **source partition and offset**; two
identical bodies at different coordinates are two events.  For one entity,
the effective rule is the assignment with the greatest `effective_from`
not later than `event_time`.  At one `(entity, effective_from)` the highest
`revision` wins; an identical `(entity, effective_from, revision, group)`
seen from several positions is one version whose source is the smallest
`(partition, offset)` location.  The same `(entity, effective_from,
revision)` carrying different groups is an unresolvable
`RuleConflictError` — the whole refresh is rejected.  Events with no
matching rule stay published, explicitly unassigned.

### Prefix-vector publication

A refresh takes one source snapshot and, per partition, uses the stable
frontier as an exclusive upper bound.  It then chooses the maximal prefix
vector such that every **committed transaction is either fully inside or
fully outside** the prefix — a pending transaction on another partition can
never make half of a cross-partition transaction visible, and cutting one
partition may force a transaction to wait on another.  All still-safe
prefixes are published; one blocked partition never stalls the others.
Aborted records produce no details but keep their offset-advance semantics.
The report carries the actual positions plus, for every partition held
short of its frontier, the deferred partition, the waiting transaction and
the open transaction/offset blocking it.

### Releases, changes and paging

Each publish is a gapless **release**.  Details, net changes, the release
number and the source-position vector commit in one SQLite transaction; a
refresh with no new positions creates no release and never wipes the
previous release's changes.  Changes are keyed by event source position and
contain either an inserted detail or the before/after assignment and rule
source of a correction, computed from the round's **final** state (no
intermediate or false corrections).  Detail/change pages are pinned to one
release; continuing with a cursor from an older release raises
`StaleCursorError` instead of silently mixing versions.

Refreshes read only not-yet-materialized source bodies and re-evaluate only
the entities whose rules changed — no full log copy or table rebuild.
Normal retention of already-materialized ranges is harmless; if the source
has retained away **unprocessed** offsets, or a different log sits at the
bound path, the refresh raises (`HistoryGapError` /
`SourceIdentityError`) while the previous result stays readable.

```python
import txlog

store = txlog.open_materialized("./warehouse/mat.sqlite3", "./log")
report = store.refresh()
report["release_no"]          # None until the first publishable prefix
report["positions"]           # {partition: exclusive offset}
report["deferred"]            # held-back partitions with txn/offset reasons

store.status()                # binding, latest release, vector, counts
page = store.read_details(limit=100)                 # current release
page = store.read_details(limit=100, cursor=page["next_cursor"])
store.read_changes()          # latest release's net changes
store.read_changes(3)         # any historical release's changes
```

### Materialization CLI

```
txlog mat-open MAT-DB SOURCE-DIR           # bind a new store to a log
txlog mat-refresh MAT-DB [SOURCE-DIR]      # one incremental refresh
txlog mat-status MAT-DB                    # publish state
txlog mat-details MAT-DB [--limit N] [--cursor C]
txlog mat-changes MAT-DB [--release R] [--limit N] [--cursor C]
```

### Materialization fault points

```
materialize.before_commit   release fully computed, not yet committed
materialize.after_commit    release committed, receipt being returned
```

An exit before the commit leaves no release and the retry rebuilds the
round; an exit after the commit durably published, so the retry republishes
nothing and never re-sends that batch's changes under a new number.

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
Named resumable readers (read_committed only):
txlog reader-create DIR READER [--start-offset O]
txlog reader-open DIR READER
txlog reader-poll DIR READER --partition P [--max-records N] [--offset O]
txlog reader-ack DIR READER --partition P --offset O
txlog reader-status DIR READER [--partition P]
txlog reader-replay DIR READER --partition P [--offset O]
txlog reader-delete DIR READER
txlog readers DIR

Local incremental materialization (separate SQLite store):
txlog mat-open MAT-DB SOURCE-DIR
txlog mat-refresh MAT-DB [SOURCE-DIR]
txlog mat-status MAT-DB
txlog mat-details MAT-DB [--limit N] [--cursor C]
txlog mat-changes MAT-DB [--release R] [--limit N] [--cursor C]
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
reader.poll.after_meta  reader.ack.after_meta
materialize.before_commit  materialize.after_commit
```

## Tests

```
cd txlog
python3 -m unittest discover -s tests -t . -v
python3 scripts/stress.py --small            # tiny fixture, seconds
python3 scripts/stress.py --ops 20000 --writers 3 --readers 4
python3 scripts/mat_stress.py --small        # materialization stress
python3 scripts/mat_stress.py --rounds 200
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
* `tests/test_readers.py` — named-reader acceptance: independent readers,
  out-of-order gap blocking, abort holes, redelivery, replay, the
  unrecoverable-boundary contract and reader-pinned retention.
* `tests/test_reader_model_diff.py` — an independent serial reader model
  (ack sets, contiguous cursors, redelivery, reader retain floor)
  replaying random poll/ack/replay/retain/delete/reopen histories and
  compared step-by-step against the real implementation.
* `tests/test_reader_concurrency.py` — concurrent poll/ack/retain,
  competing handles and delete/poll interleavings.
* `tests/test_reader_recovery.py` — real `os._exit` crashes at reader
  poll/ack commit points and mid-retain, with lost-receipt retries.

Local incremental materialization:

* `tests/test_mat_handhistory.py` — the **handwritten** two-position
  history from the spec: first refresh `(2,2,0)` upper publishes only
  `(1,0,0)` (just unassigned E20); aborting the pending transaction reaches
  `(2,2,2)` with gold E20 / unassigned E9 / blue E12; plus late-rule
  correction scoping, high/low revision, the `effective_from == event_time`
  boundary, version conflicts and smallest-location duplicates.
* `tests/test_mat_model_diff.py` — an **independent brute-force model**
  enumerates every transaction-atomic prefix vector, takes the component-wise
  maximum and re-evaluates each selected event from scratch; random
  interleaved histories (cross-partition commit/abort/open txns, late rules,
  no-op refreshes and reopens) are compared release-by-release including the
  exact net-change set.  Trial count/seed via `TXLOG_MAT_TRIALS`/
  `TXLOG_MAT_SEED`.
* `tests/test_mat_reads.py` — release-pinned paging and `StaleCursorError`,
  source `log_id` identity refusal, retained-away history gaps, and atomic
  rejection of invalid UTF-8 / JSON / schema records with coordinates.
* `tests/test_mat_concurrency.py` — concurrent refreshes serialize into
  gapless unique releases, reads stay coherent during refreshes, and a
  second store loses the cross-process refresh flock.
* `tests/test_mat_recovery.py` — real `os._exit` at
  `materialize.before_commit` (no release, clean retry) and
  `materialize.after_commit` (release durable, lost receipt never
  republished), plus cross-process incremental continuation.
* `tests/test_mat_cli.py` / `tests/test_mat_large.py` — the `mat-*` CLI
  surface with structured errors, and runtime-generated large histories
  checking incremental scaling, change scoping and retention of processed
  segments.
* `scripts/stress.py` — concurrent workload generator (writers, raw
  fetchers, named readers and a retainer) with online and post-recovery
  invariant verification.  Data is generated at runtime and bounded by
  the CLI flags; no generated data files are checked in.
