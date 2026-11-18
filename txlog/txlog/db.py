"""SQLite metadata store for txlog.

SQLite holds ONLY metadata: producer epochs, transaction state, dedup
identity, sequence state, the segment catalog and per-partition watermarks.
Record payloads live exclusively in the segment files.

All connections use WAL + synchronous=FULL, so a committed SQLite
transaction is durable (checkpoint/fsync semantics are handled by SQLite).
"""

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS producers (
    producer_id TEXT PRIMARY KEY,
    epoch       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    producer_id TEXT NOT NULL,
    epoch       INTEGER NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('open','committed','aborted')),
    created_at  REAL NOT NULL,
    decided_at  REAL
);
CREATE INDEX IF NOT EXISTS idx_transactions_state ON transactions(state);

-- Offset range a transaction occupies in each partition it touched.
CREATE TABLE IF NOT EXISTS txn_parts (
    txn_id       INTEGER NOT NULL,
    partition    INTEGER NOT NULL,
    first_offset INTEGER NOT NULL,
    last_offset  INTEGER NOT NULL,
    PRIMARY KEY (txn_id, partition)
);

-- Retry identity: (producer, epoch, partition, seq) -> offset + content crc.
-- Rows older than the dedup window are garbage-collected; appends below the
-- window lower bound are then rejected as SequenceTooOld.
CREATE TABLE IF NOT EXISTS dedup (
    producer_id TEXT NOT NULL,
    epoch       INTEGER NOT NULL,
    partition   INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    offset      INTEGER NOT NULL,
    crc         INTEGER NOT NULL,
    txn_id      INTEGER NOT NULL,
    PRIMARY KEY (producer_id, epoch, partition, seq)
);
CREATE INDEX IF NOT EXISTS idx_dedup_partition ON dedup(partition, offset);

CREATE TABLE IF NOT EXISTS seq_state (
    producer_id      TEXT NOT NULL,
    epoch            INTEGER NOT NULL,
    partition        INTEGER NOT NULL,
    next_seq         INTEGER NOT NULL,
    min_retained_seq INTEGER NOT NULL,
    PRIMARY KEY (producer_id, epoch, partition)
);

CREATE TABLE IF NOT EXISTS partitions (
    partition      INTEGER PRIMARY KEY,
    next_offset    INTEGER NOT NULL,   -- high watermark (next offset to assign)
    active_segment INTEGER NOT NULL,   -- base offset of the active segment
    retained_below INTEGER NOT NULL    -- retention frontier: offsets < this are gone
);

CREATE TABLE IF NOT EXISTS segments (
    partition   INTEGER NOT NULL,
    base_offset INTEGER NOT NULL,
    path        TEXT NOT NULL,          -- relative to the log directory
    max_offset  INTEGER NOT NULL,       -- inclusive; -1 while empty
    bytes       INTEGER NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('active','sealed','deleted')),
    PRIMARY KEY (partition, base_offset)
);

-- Named, resumable application readers.  Only metadata lives here: record
-- payloads stay in the segment files.  A reader starts either at the current
-- retention boundary of each partition (start_mode='boundary') or at one
-- explicit offset (start_mode='offset'), recorded once at creation and
-- never rewritten by recreate/conflict/poll/replay.
CREATE TABLE IF NOT EXISTS readers (
    reader_id   TEXT PRIMARY KEY,
    start_mode  TEXT NOT NULL CHECK (start_mode IN ('boundary','offset')),
    start_offset INTEGER,               -- set iff start_mode='offset'
    created_at  REAL NOT NULL
);

-- Per-reader/per-partition progress:
--   read_offset: every read_committed-visible record with offset below this
--                has been delivered at least once by some poll() of this
--                reader (durable; used to resume after a crash).
--   safe_offset: contiguous-acknowledgement cursor.  All DELIVERED records
--                in [start, safe_offset) have been acknowledged; abort holes
--                are skipped.  It can never equal "highest acked offset":
--                an unacked gap before it blocks advancement.  This is the
--                value retention must not cross.
CREATE TABLE IF NOT EXISTS reader_positions (
    reader_id   TEXT NOT NULL,
    partition   INTEGER NOT NULL,
    read_offset INTEGER NOT NULL,
    safe_offset INTEGER NOT NULL,
    PRIMARY KEY (reader_id, partition)
);

-- The acknowledgement SET.  Rows below safe_offset are pruned when the
-- contiguous cursor advances; what remains are the out-of-order acks above
-- the gap (the blocking-gap evidence).  PRIMARY KEY makes a repeated ack
-- idempotent; there is no "max offset" shortcut anywhere.
CREATE TABLE IF NOT EXISTS reader_acks (
    reader_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset    INTEGER NOT NULL,
    PRIMARY KEY (reader_id, partition, offset)
);
CREATE INDEX IF NOT EXISTS idx_reader_acks
    ON reader_acks(reader_id, partition, offset);
"""

FORMAT_VERSION = "1"


def connect(db_path):
    con = sqlite3.connect(db_path, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.isolation_level = None  # autocommit; transactions are explicit
    con.execute("PRAGMA busy_timeout = 30000")
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = FULL")
    return con


def init_schema(con):
    con.executescript(SCHEMA)
