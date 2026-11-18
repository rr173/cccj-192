"""Exception hierarchy for txlog."""


class TxLogError(Exception):
    """Base class for all txlog errors."""


class CorruptionError(TxLogError):
    """Definite on-disk corruption was detected during recovery or read.

    Raised for: checksum failure with valid records after it, torn data in a
    sealed segment, offset gaps, catalog entries referencing missing segment
    files, or acknowledged records missing from disk.  A torn *tail* of the
    active segment is NOT corruption (it is the expected result of a crash)
    and is truncated by recovery instead.
    """


class ProducerFenced(TxLogError):
    """The producer epoch is stale: another instance has opened this
    producer_id with a newer epoch, so every write/finalize operation of the
    old epoch is rejected immediately."""


class SequenceGap(TxLogError):
    """append used a sequence number greater than the next expected one."""


class SequenceTooOld(TxLogError):
    """append used a sequence number below the dedup retention window lower
    bound.  Retries outside the window are *rejected*, never re-appended."""


class ContentConflict(TxLogError):
    """(producer, epoch, partition, seq) was already appended with different
    content.  The original offset is kept; the retry is rejected."""


class TxnStateError(TxLogError):
    """Invalid transaction lifecycle call (e.g. commit with no open txn)."""


class UnknownTransaction(TxLogError):
    """The referenced txn_id does not exist (or its metadata was GC'd)."""
