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


# ---------------------------------------------------------------------------
# local incremental materialization (event assignment details)
# ---------------------------------------------------------------------------

class MaterializationError(TxLogError):
    """Base class for materialized-store errors."""


class MaterializationBusy(MaterializationError):
    """Another process/thread holds the materialized store's refresh lock.

    A refresh is a single critical section; two refreshes can never build on
    the same old release and overwrite each other.
    """


class SourceIdentityError(MaterializationError):
    """The opened source log is not the one the materialized store was bound
    to (its persistent ``log_id`` differs), even if the path looks the same.
    The previous materialized result stays readable and untouched."""

    def __init__(self, message, *, expected_log_id=None, actual_log_id=None,
                 expected_path=None, actual_path=None):
        super().__init__(message)
        self.expected_log_id = expected_log_id
        self.actual_log_id = actual_log_id
        self.expected_path = expected_path
        self.actual_path = actual_path


class HistoryGapError(MaterializationError):
    """The source has already retained away offsets the materialized store
    has not processed yet.  The refresh is refused (the previous result stays
    readable); the missing source coordinate range is carried on the error.
    """

    def __init__(self, message, *, partition, missing_from, missing_to):
        super().__init__(message)
        self.partition = partition
        self.missing_from = missing_from
        self.missing_to = missing_to


class MaterializationDataError(MaterializationError):
    """A record body selected for a publishable prefix is not a valid
    UTF-8 JSON event/assignment document.  The whole refresh fails atomically
    and the offending source coordinate (partition, offset) is carried on the
    error.  Nothing is half-published."""

    def __init__(self, message, *, partition, offset, reason):
        super().__init__(message)
        self.partition = partition
        self.offset = offset
        self.reason = reason


class RuleConflictError(MaterializationError):
    """Two assignment versions with the same (entity, effective_from,
    revision) carry different groups.  Such a version can never be resolved
    deterministically, so the refresh is rejected with both source locations.
    """

    def __init__(self, message, *, entity, effective_from, revision, locations):
        super().__init__(message)
        self.entity = entity
        self.effective_from = effective_from
        self.revision = revision
        # list of {"partition", "offset", "group"}
        self.locations = locations


class StaleCursorError(MaterializationError):
    """A pagination cursor belongs to an older release.  Paging is pinned to
    one release; an expired cursor is rejected explicitly instead of silently
    concatenating rows from two releases."""

    def __init__(self, message, *, cursor_release, current_release):
        super().__init__(message)
        self.cursor_release = cursor_release
        self.current_release = current_release


class UnknownRelease(MaterializationError):
    """The requested release number does not exist in this store."""
