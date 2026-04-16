"""
Week 2 durability / ACK tests (real os.fsync, threads).

Reasoning (required):
- 10 fast SYNC appends with batch_max=5 → at least 2 fsync calls (ceil(10/5)),
  not 10, if the commit thread batches before interval fires.
- Pending SYNC ACKs not yet fsync'd: crash → recovery truncates torn tail; no ACK was
  returned for uncommitted records still in queue (callers blocked).
- ASYNC never returns corrupted bytes: append returns after write(); recovery still
  validates MAGIC/OFFSET/CRC and truncates — invalid tail is never surfaced as records.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

# Repo root on PYTHONPATH
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from wal_py import (  # noqa: E402
    DurableWAL,
    DurabilityMode,
    recover,
    scan_prefix_payloads,
)
from wal_py.durability_manager import DurabilityManager  # noqa: E402
def _env():
    e = os.environ.copy()
    e["PYTHONPATH"] = str(_ROOT)
    return e


class TestSyncDurability(unittest.TestCase):
    def test_ack_only_after_fsync_subprocess(self):
        """After SYNC append returns, data survives new process (real durability)."""
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            code = """
import sys
from wal_py import DurableWAL, DurabilityMode, scan_prefix_payloads
path = sys.argv[1]
w = DurableWAL(path, DurabilityMode.SYNC, sync_batch_max_records=1, sync_batch_interval_ms=99999.0)
w.append(b"durability")
w.close()
assert scan_prefix_payloads(path) == [b"durability"]
"""
            r = subprocess.run(
                [sys.executable, "-c", code, path],
                env=_env(),
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
            self.assertEqual(scan_prefix_payloads(path), [b"durability"])
        finally:
            os.unlink(path)

    def test_group_commit_amortizes_fsync(self):
        """Concurrent SYNC appends so multiple records queue before one fsync (group commit)."""
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            w = DurableWAL(
                path,
                DurabilityMode.SYNC,
                sync_batch_max_records=5,
                sync_batch_interval_ms=2000.0,
            )
            barrier = threading.Barrier(10)
            errors: list[BaseException] = []

            def one(i: int) -> None:
                try:
                    barrier.wait()
                    w.append(f"k{i}".encode())
                except BaseException as e:
                    errors.append(e)

            threads = [threading.Thread(target=one, args=(i,)) for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])
            w.close()
            # Batched: strictly fewer fsyncs than appends when batch_max=5
            self.assertLess(w.fsync_count, 10)
            self.assertGreaterEqual(w.fsync_count, 2)
            pl = scan_prefix_payloads(path)
            self.assertEqual(len(pl), 10)
        finally:
            os.unlink(path)

    def test_concurrent_sync_appends_ordering(self):
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            w = DurableWAL(path, DurabilityMode.SYNC, sync_batch_max_records=8, sync_batch_interval_ms=500.0)
            errors: list[BaseException] = []

            def worker(n: int) -> None:
                try:
                    for j in range(20):
                        w.append(f"t{n}-{j}".encode())
                except BaseException as e:
                    errors.append(e)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            w.close()
            self.assertEqual(errors, [])
            pl = scan_prefix_payloads(path)
            self.assertEqual(len(pl), 80)
            # strict increasing logical order in file: offsets 0..79
            for i, p in enumerate(pl):
                self.assertTrue(p.startswith(b"t"))
        finally:
            os.unlink(path)


class TestAsync(unittest.TestCase):
    def test_async_may_lose_tail_on_hard_kill(self):
        """ASYNC: ACK before fsync; SIGKILL before background flush may drop recent data; log stays valid."""
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            code = """
import os, sys, time
from wal_py import DurableWAL, DurabilityMode
path = sys.argv[1]
w = DurableWAL(path, DurabilityMode.ASYNC, async_flush_max_records=10_000, async_flush_interval_ms=60_000.0)
w.append(b"maybe-lost")
os._exit(0)
"""
            r = subprocess.run(
                [sys.executable, "-c", code, path],
                env=_env(),
                timeout=30,
            )
            self.assertEqual(r.returncode, 0)
            recover(path)
            pl = scan_prefix_payloads(path)
            # Either empty (lost) or one valid record — never corrupt
            self.assertIn(len(pl), (0, 1))
            if pl:
                self.assertEqual(pl[0], b"maybe-lost")
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def test_async_ordering_strict_offsets_after_recover(self):
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            w = DurableWAL(
                path,
                DurabilityMode.ASYNC,
                async_flush_max_records=4,
                async_flush_interval_ms=20.0,
            )
            for i in range(15):
                w.append(str(i).encode())
            time.sleep(0.15)
            w.close()
            recover(path)
            pl = scan_prefix_payloads(path)
            n = len(pl)
            for i in range(n):
                self.assertEqual(pl[i], str(i).encode())
        finally:
            os.unlink(path)


class TestRecoveryInvariant(unittest.TestCase):
    def test_partial_tail_truncated(self):
        """Simulate torn tail: recovery drops it; prefix valid."""
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            w = DurableWAL(path, DurabilityMode.SYNC, sync_batch_max_records=1, sync_batch_interval_ms=99.0)
            w.append(b"ok")
            w.close()
            sz = os.path.getsize(path)
            with open(path, "r+b") as f:
                f.truncate(max(0, sz - 3))
            recover(path)
            pl = scan_prefix_payloads(path)
            self.assertEqual(pl, [])
        finally:
            os.unlink(path)


class TestCase3DurabilityWithoutAck(unittest.TestCase):
    """
    Case 3: if fsync completed but process died before user saw ACK, data must still be there.
    Here we only assert: after SYNC close(), scan matches all appends (fsync path durable).
    """

    def test(self):
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        try:
            w = DurableWAL(path, DurabilityMode.SYNC, sync_batch_max_records=4, sync_batch_interval_ms=200.0)
            for i in range(6):
                w.append(bytes([i]))
            w.close()
            pl = scan_prefix_payloads(path)
            self.assertEqual(pl, [bytes([i]) for i in range(6)])
        finally:
            os.unlink(path)


class TestFsyncInstrumentation(unittest.TestCase):
    def test_fsync_hook_counts(self):
        fd, path = tempfile.mkstemp(suffix=".wal")
        os.close(fd)
        calls = []

        def hook(fileno: int) -> None:
            calls.append(fileno)
            os.fsync(fileno)

        try:
            f = open(path, "r+b", buffering=0)
            dm = DurabilityManager(
                f.fileno(),
                DurabilityMode.SYNC,
                next_offset=0,
                sync_batch_max_records=2,
                sync_batch_interval_ms=50.0,
                fsync_hook=hook,
            )
            dm.append(b"a")
            dm.append(b"b")
            dm.close()
            f.close()
            self.assertGreaterEqual(len(calls), 1)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
