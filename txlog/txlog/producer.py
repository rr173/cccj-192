"""Producer handle: epoch-bound transactional writer."""


class Producer:
    """A producer is bound to (producer_id, epoch).  Reopening the same
    producer_id anywhere yields a higher epoch and immediately fences this
    handle: its begin/append/commit/abort raise ProducerFenced.

    At most one transaction is open per producer at a time.  Within one
    epoch, per-partition sequence numbers run 0,1,2,... continuously across
    transactions of that epoch.
    """

    def __init__(self, log, producer_id, epoch):
        self.log = log
        self.producer_id = producer_id
        self.epoch = epoch

    def begin(self):
        """Open a transaction (idempotent: returns the still-open txn when
        retried).  Returns txn_id."""
        return self.log._begin(self.producer_id, self.epoch)

    def append(self, partition, data, seq):
        """Append inside the open transaction.

        (epoch, partition, seq) is the retry identity: a retry with the same
        content returns the original offset; different content raises
        ContentConflict; a skipped sequence raises SequenceGap; a sequence
        below the dedup retention window raises SequenceTooOld.
        Returns the record's offset.
        """
        offset, _dup = self.log._append_txn(
            self.producer_id, self.epoch, partition, data, seq)
        return offset

    def commit(self, txn_id=None):
        """Make the open transaction visible on ALL partitions atomically.
        Safe to retry: committing an already-committed txn_id is a no-op."""
        return self.log._commit(self.producer_id, self.epoch, txn_id)

    def abort(self, txn_id=None):
        """Abandon the open transaction.  Its records stay in the log as
        offset holes (evidence); later records are never moved."""
        return self.log._abort(self.producer_id, self.epoch, txn_id)

    @property
    def current_txn(self):
        """txn_id of the currently open transaction, or None."""
        con = self.log._connect()
        try:
            row = self.log._open_txn(con, self.producer_id)
            return row["txn_id"] if row else None
        finally:
            con.close()

    def __repr__(self):
        return f"Producer({self.producer_id!r}, epoch={self.epoch})"
