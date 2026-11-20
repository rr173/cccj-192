"""txlog — a transactional, partitioned, segmented local log.

Public API (mirrors the CLI verbs):

    log = open_log(dir)                      # or open_producer(dir, id)
    p   = open_producer(dir, "producer-1")   # bumps epoch, fences old one
    p.begin()                                # -> txn_id
    p.append(partition, data, seq)           # transactional -> offset
    p.commit() / p.abort()
    log.append(partition, data)              # non-transactional -> offset
    page = log.fetch(partition, offset, max_records=100,
                     isolation="read_committed")
    log.retain(partition, before_offset)
    log.inspect(partition=None, raw=False)
    log.txn_state(txn_id)

Named, resumable application readers:

    r = log.create_reader("etl-1")                 # tail of retention
    r = log.create_reader("etl-2", start=0)        # explicit readable start
    page = r.poll(partition, max_records=100)      # read_committed only
    r.ack(partition, offset)                       # idempotent, per record
    r.status(partition)                            # safe cursor + gaps
    r.replay(partition, offset)                    # re-delivery, keeps acks
    log.open_reader("etl-1") / log.list_readers()
    r.delete()                                     # explicit; releases retain hold
"""

from .errors import (BackupBusy, BackupError, BackupPointExists,
                     BackupPointNotFound, BackupTargetExists,
                     BackupUnsupportedVersion, BackupVerificationError,
                     ContentConflict, CorruptionError, HistoryGapError,
                     MaterializationBusy, MaterializationDataError,
                     MaterializationError, ProducerFenced, ReaderAckError,
                     ReaderConflict, ReaderDataUnavailable, ReaderError,
                     RuleConflictError, SequenceGap, SequenceTooOld,
                     SourceIdentityError, StaleCursorError, TxLogError,
                     TxnStateError, UnknownReader, UnknownRelease,
                     UnknownTransaction)
from .backup import BackupRepository, open_repository
from .log import (ISOLATION_READ_COMMITTED, ISOLATION_READ_UNCOMMITTED, Log)
from .materialize import MaterializedStore, open_materialized
from .producer import Producer
from .reader import Reader

__all__ = [
    "Log", "Producer", "Reader", "open_log", "open_producer",
    "ISOLATION_READ_COMMITTED", "ISOLATION_READ_UNCOMMITTED",
    "TxLogError", "CorruptionError", "ProducerFenced", "SequenceGap",
    "SequenceTooOld", "ContentConflict", "TxnStateError",
    "UnknownTransaction",
    "ReaderError", "UnknownReader", "ReaderConflict",
    "ReaderDataUnavailable", "ReaderAckError",
    "MaterializedStore", "open_materialized", "MaterializationError",
    "MaterializationBusy", "SourceIdentityError", "HistoryGapError",
    "MaterializationDataError", "RuleConflictError", "StaleCursorError",
    "UnknownRelease",
    "BackupRepository", "open_repository", "BackupError",
    "BackupPointExists", "BackupPointNotFound", "BackupBusy",
    "BackupVerificationError", "BackupUnsupportedVersion",
    "BackupTargetExists",
]

__version__ = "0.1.0"


def open_log(path, **kw):
    """Open (and create if needed) a log directory."""
    return Log(path, **kw)


def open_producer(path, producer_id, **kw):
    """Open the log and register a producer with a fresh, higher epoch.

    Any leftover open transactions of this producer_id are aborted by the
    fencing flow; operations of older epochs fail immediately.
    """
    log = Log(path, **kw)
    epoch, _ = log._open_producer(producer_id)
    return Producer(log, producer_id, epoch)
