"""Injectable *real* exit points for crash-recovery testing.

Every durable operation in txlog passes through named fault points, e.g.:

    append.after_write      record written to segment, fsync not yet done
    append.after_fsync      record fsynced, metadata txn not yet committed
    append.after_meta       metadata committed, before returning to caller
    roll.after_file         new segment file created, catalog not yet updated
    roll.after_catalog      catalog roll done (inside the append meta txn)
    begin.after_meta        txn row committed
    commit.before_decision  commit validated, decision not yet durable
    commit.after_decision   commit decision durable, before returning
    abort.before_decision / abort.after_decision
    open_producer.after_fence   epoch bump + fencing aborts committed
    retain.after_catalog    segments marked deleted + frontier advanced
    retain.after_delete     segment files unlinked
    fetch.after_snapshot    fetch snapshot taken (hook-only, for tests)

Configuration:
  * Environment variable TXLOG_FAULT="<point>" or "<point>:<n>" — when the
    named point is hit for the n-th time (default 1st), the process performs
    a REAL exit via os._exit(42): no cleanup, no atexit, exactly like a kill.
  * Or pass fault_hook=callable to Log(); the hook receives the point name
    and may raise / run code (used by in-process tests).
"""

import os


class FaultInjector:
    def __init__(self, hook=None):
        self.hook = hook
        self.point = None
        self.remaining = 0
        spec = os.environ.get("TXLOG_FAULT", "") or None
        if spec:
            if ":" in spec:
                name, _, n = spec.rpartition(":")
                self.point = name
                self.remaining = int(n)
            else:
                self.point = spec
                self.remaining = 1

    def maybe(self, point):
        if self.hook is not None:
            self.hook(point)
            return
        if self.point == point and self.remaining > 0:
            self.remaining -= 1
            if self.remaining == 0:
                # Real exit: simulates power loss / kill -9 at this exact
                # point between file append, fsync and metadata state.
                os._exit(42)
