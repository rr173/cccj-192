"""Shared helpers for txlog tests."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, ROOT)

import txlog  # noqa: E402


class LogTestCase(unittest.TestCase):
    """Base class: per-test temporary log directory."""

    SEGMENT_BYTES = 4096
    DEDUP_WINDOW = 8

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="txlog-test-")
        self.dir = os.path.join(self.tmp.name, "log")
        self.logs = []

    def tearDown(self):
        for log in self.logs:
            try:
                log.close()
            except Exception:
                pass
        self.tmp.cleanup()

    def make_log(self, **kw):
        kw.setdefault("segment_bytes", self.SEGMENT_BYTES)
        kw.setdefault("dedup_window", self.DEDUP_WINDOW)
        log = txlog.Log(self.dir, **kw)
        self.logs.append(log)
        return log

    def reopen(self, log, **kw):
        """Close ``log`` and open a fresh one on the same directory."""
        log.close()
        kw.setdefault("segment_bytes", self.SEGMENT_BYTES)
        kw.setdefault("dedup_window", self.DEDUP_WINDOW)
        new = txlog.Log(log.path, **kw)
        self.logs.append(new)
        return new


def run_cli(args, env_extra=None, expect=0):
    """Run the txlog CLI in a subprocess; returns (rc, stdout, stderr)."""
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    if env_extra:
        env.update(env_extra)
    cp = subprocess.run([sys.executable, "-m", "txlog"] + list(args),
                        capture_output=True, text=True, env=env, cwd="/")
    if expect is not None:
        assert cp.returncode == expect, (
            f"cli {args} -> rc={cp.returncode}\nstdout={cp.stdout}\n"
            f"stderr={cp.stderr}")
    return cp.returncode, cp.stdout, cp.stderr


def run_cli_json(args, env_extra=None, expect=0):
    rc, out, err = run_cli(args, env_extra, expect)
    return json.loads(out)


def run_py(code, env_extra=None, expect=0):
    """Run a python snippet with txlog importable; returns CompletedProcess."""
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    if env_extra:
        env.update(env_extra)
    cp = subprocess.run([sys.executable, "-c", code],
                        capture_output=True, text=True, env=env, cwd="/")
    if expect is not None:
        assert cp.returncode == expect, (
            f"snippet -> rc={cp.returncode}\nstdout={cp.stdout}\n"
            f"stderr={cp.stderr}\ncode={code}")
    return cp
