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


class ReaderError(TxLogError):
    """Base class for named-reader errors."""


class UnknownReader(ReaderError):
    """The referenced reader_id does not exist (or was deleted).  A stale
    reader handle can never move any cursor after its reader was deleted."""


class ReaderConflict(ReaderError):
    """create_reader() reused an existing reader_id with a DIFFERENT
    configuration.  The existing reader and its progress are left untouched;
    a configuration mismatch never silently resets a cursor."""


class ReaderDataUnavailable(ReaderError):
    """The requested position is below the partition's retention frontier:
    those segments were already deleted and the data cannot be recovered
    from this log.  The error carries the recoverable boundary
    (``retained_below``) so the caller can explicitly resume there instead
    of being handed an empty page or silently jumped forward."""

    def __init__(self, reader_id, partition, requested_offset,
                 retained_below):
        super().__init__(
            f"reader {reader_id!r} partition {partition}: offset "
            f"{requested_offset} is below the retention frontier "
            f"{retained_below}; the data is no longer available, resume at "
            f"offset {retained_below}")
        self.reader_id = reader_id
        self.partition = partition
        self.requested_offset = requested_offset
        self.retained_below = retained_below


class ReaderAckError(ReaderError):
    """An acknowledgement was rejected.  ``reason`` is one of:

    * ``wrong_partition`` — the reader never opened this partition;
    * ``not_delivered``   — the offset was never delivered to this reader;
    * ``beyond_stable``   — the offset is past the stable readable frontier;
    * ``abort_hole``      — the offset holds an aborted (skipped) record,
                            so no business record was ever delivered there.
    """

    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason
