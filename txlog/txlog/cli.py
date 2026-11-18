"""Command line interface.  Every command prints JSON on stdout.

    txlog init DIR [--segment-bytes N] [--dedup-window N]
    txlog open-producer DIR PRODUCER
    txlog begin DIR PRODUCER --epoch E
    txlog append DIR PRODUCER --epoch E --partition P --seq S
                  (--data S | --data-file F | --data-hex H)
    txlog append DIR --partition P --data S            # non-transactional
    txlog commit DIR PRODUCER --epoch E [--txn-id T]
    txlog abort  DIR PRODUCER --epoch E [--txn-id T]
    txlog fetch DIR --partition P [--offset O] [--max-records N]
                  [--isolation read_committed|read_uncommitted] [--max-scan N]
    txlog retain DIR --partition P --before-offset N
    txlog inspect DIR [--partition P] [--raw]
    txlog txn-state DIR --txn-id T

Crash injection: set TXLOG_FAULT=<point>[:<n>] in the environment to make
the process os._exit(42) at the named fault point (see txlog/faults.py).
"""

import argparse
import base64
import json
import os
import sys

from .errors import TxLogError
from .log import Log


def _lock_timeout():
    return float(os.environ.get("TXLOG_LOCK_TIMEOUT", "30"))


def _open(args, writable):
    kw = {}
    if writable:
        kw["segment_bytes"] = getattr(args, "segment_bytes", None)
        kw["dedup_window"] = getattr(args, "dedup_window", None)
    return Log(args.dir, writable=writable, lock_timeout=_lock_timeout(),
               **kw)


def _payload(args):
    given = [args.data is not None, args.data_file is not None,
             args.data_hex is not None]
    if sum(given) != 1:
        raise TxLogError("exactly one of --data/--data-file/--data-hex "
                         "is required")
    if args.data is not None:
        return args.data.encode("utf-8")
    if args.data_file is not None:
        with open(args.data_file, "rb") as fh:
            return fh.read()
    return bytes.fromhex(args.data_hex)


def _enc_record(r):
    out = {k: v for k, v in r.items() if k != "payload"}
    out["payload_len"] = len(r["payload"])
    out["payload_b64"] = base64.b64encode(r["payload"]).decode("ascii")
    try:
        out["payload_text"] = r["payload"].decode("utf-8")
    except UnicodeDecodeError:
        out["payload_text"] = None
    return out


def _enc_page(page):
    out = dict(page)
    out["records"] = [_enc_record(r) for r in page["records"]]
    return out


def cmd_init(args):
    with _open(args, writable=True) as log:
        return {"dir": log.path, "segment_bytes": log.segment_bytes,
                "dedup_window": log.dedup_window}


def cmd_open_producer(args):
    with _open(args, writable=True) as log:
        epoch, aborted = log._open_producer(args.producer)
        return {"producer_id": args.producer, "epoch": epoch,
                "aborted_transactions": aborted}


def cmd_begin(args):
    with _open(args, writable=True) as log:
        return {"txn_id": log._begin(args.producer, args.epoch)}


def cmd_append(args):
    payload = _payload(args)
    with _open(args, writable=True) as log:
        if args.producer is None:
            offset = log.append(args.partition, payload)
            return {"offset": offset, "transactional": False}
        if args.epoch is None or args.seq is None:
            raise TxLogError("transactional append requires --epoch and "
                             "--seq")
        offset, dup = log._append_txn(args.producer, args.epoch,
                                      args.partition, payload, args.seq)
        return {"offset": offset, "transactional": True, "duplicate": dup}


def cmd_commit(args):
    with _open(args, writable=True) as log:
        txn_id = log._commit(args.producer, args.epoch, args.txn_id)
        return {"txn_id": txn_id, "state": "committed"}


def cmd_abort(args):
    with _open(args, writable=True) as log:
        txn_id = log._abort(args.producer, args.epoch, args.txn_id)
        return {"txn_id": txn_id, "state": "aborted"}


def cmd_fetch(args):
    with _open(args, writable=False) as log:
        page = log.fetch(args.partition, args.offset,
                         max_records=args.max_records,
                         isolation=args.isolation,
                         max_scan=args.max_scan)
        return _enc_page(page)


def cmd_retain(args):
    with _open(args, writable=True) as log:
        return log.retain(args.partition, args.before_offset)


def cmd_inspect(args):
    with _open(args, writable=False) as log:
        return log.inspect(partition=args.partition, raw=args.raw)


def cmd_txn_state(args):
    with _open(args, writable=False) as log:
        return log.txn_state(args.txn_id)


def build_parser():
    p = argparse.ArgumentParser(
        prog="txlog", description="transactional partitioned local log")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="create/configure a log directory")
    sp.add_argument("dir")
    sp.add_argument("--segment-bytes", type=int, default=None)
    sp.add_argument("--dedup-window", type=int, default=None)
    sp.set_defaults(fn=cmd_init)

    sp = sub.add_parser("open-producer", help="register producer, bump epoch")
    sp.add_argument("dir")
    sp.add_argument("producer")
    sp.set_defaults(fn=cmd_open_producer)

    sp = sub.add_parser("begin", help="begin a transaction")
    sp.add_argument("dir")
    sp.add_argument("producer")
    sp.add_argument("--epoch", type=int, required=True)
    sp.set_defaults(fn=cmd_begin)

    sp = sub.add_parser("append", help="append a record")
    sp.add_argument("dir")
    sp.add_argument("producer", nargs="?", default=None)
    sp.add_argument("--epoch", type=int, default=None)
    sp.add_argument("--partition", type=int, required=True)
    sp.add_argument("--seq", type=int, default=None)
    sp.add_argument("--data", default=None)
    sp.add_argument("--data-file", default=None)
    sp.add_argument("--data-hex", default=None)
    sp.set_defaults(fn=cmd_append)

    for name, fn in (("commit", cmd_commit), ("abort", cmd_abort)):
        sp = sub.add_parser(name, help=f"{name} the open transaction")
        sp.add_argument("dir")
        sp.add_argument("producer")
        sp.add_argument("--epoch", type=int, required=True)
        sp.add_argument("--txn-id", type=int, default=None)
        sp.set_defaults(fn=fn)

    sp = sub.add_parser("fetch", help="fetch a bounded page of records")
    sp.add_argument("dir")
    sp.add_argument("--partition", type=int, required=True)
    sp.add_argument("--offset", type=int, default=0)
    sp.add_argument("--max-records", type=int, default=100)
    sp.add_argument("--isolation", default="read_committed",
                    choices=["read_committed", "read_uncommitted"])
    sp.add_argument("--max-scan", type=int, default=None)
    sp.set_defaults(fn=cmd_fetch)

    sp = sub.add_parser("retain", help="drop old segments of a partition")
    sp.add_argument("dir")
    sp.add_argument("--partition", type=int, required=True)
    sp.add_argument("--before-offset", type=int, required=True)
    sp.set_defaults(fn=cmd_retain)

    sp = sub.add_parser("inspect", help="dump metadata / raw segments")
    sp.add_argument("dir")
    sp.add_argument("--partition", type=int, default=None)
    sp.add_argument("--raw", action="store_true")
    sp.set_defaults(fn=cmd_inspect)

    sp = sub.add_parser("txn-state", help="show a transaction's state")
    sp.add_argument("dir")
    sp.add_argument("--txn-id", type=int, required=True)
    sp.set_defaults(fn=cmd_txn_state)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        out = args.fn(args)
    except TxLogError as e:
        print(json.dumps({"error": type(e).__name__, "message": str(e)}),
              file=sys.stderr)
        return 1
    print(json.dumps(out, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
