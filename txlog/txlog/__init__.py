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
"""

from .errors import (ContentConflict, CorruptionError, ProducerFenced,
                     SequenceGap, SequenceTooOld, TxLogError, TxnStateError,
                     UnknownTransaction)
from .log import (ISOLATION_READ_COMMITTED, ISOLATION_READ_UNCOMMITTED, Log)
from .producer import Producer

__all__ = [
    "Log", "Producer", "open_log", "open_producer",
    "ISOLATION_READ_COMMITTED", "ISOLATION_READ_UNCOMMITTED",
    "TxLogError", "CorruptionError", "ProducerFenced", "SequenceGap",
    "SequenceTooOld", "ContentConflict", "TxnStateError",
    "UnknownTransaction",
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
