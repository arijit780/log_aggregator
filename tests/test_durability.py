import os
import subprocess
import sys
import threading
import unittest
from tempfile import TemporaryDirectory

from log_aggregator import DurabilityMode, Log


def _run_crash_script(*, repo_root: str, data_dir: str, script: str) -> None:
    env = os.environ.copy()
    # Ensure the child process can import `wal` from this repo.
    env["PYTHONPATH"] = repo_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["WAL_DATA_DIR"] = data_dir
    subprocess.run([sys.executable, "-c", script], cwd=repo_root, env=env, check=True)


class TestDurabilityAndAckSemantics(unittest.TestCase):
    def test_sync_durability_after_ack(self) -> None:
        # SYNC guarantee: once append() returns (ACK), record must survive crash + restart.
        with TemporaryDirectory() as td:
            repo_root = os.path.dirname(os.path.dirname(__file__))
            data_dir = os.path.join(td, "data")

            script = r"""
import os
import os as _os
from wal import Log, DurabilityMode

data_dir = os.environ["WAL_DATA_DIR"]
log = Log(
    data_dir=data_dir,
    durability_mode=DurabilityMode.SYNC,
    fsync_batch_records=1,      # immediate durability per append (no batching dependence)
    fsync_interval_ms=10000,
)
log.append(b"SYNC_OK")
# Crash immediately after ACK (no graceful close).
_os._exit(0)
"""
            _run_crash_script(repo_root=repo_root, data_dir=data_dir, script=script)

            # Restart in parent and verify data is present.
            log2 = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log2.close)
            self.assertEqual(log2.read_all(), [b"SYNC_OK"])

    def test_async_may_lose_tail_but_never_corrupt(self) -> None:
        # ASYNC guarantee: ACK happens after write+flush, but crash before fsync may lose tail.
        # We cannot force the OS to drop cached writes deterministically in a unit test, so we
        # assert the weaker-but-required invariants: log is valid and any recovered data is a prefix.
        with TemporaryDirectory() as td:
            repo_root = os.path.dirname(os.path.dirname(__file__))
            data_dir = os.path.join(td, "data")

            N = 200
            script = rf"""
import os
import os as _os
from wal import Log, DurabilityMode

data_dir = os.environ["WAL_DATA_DIR"]
log = Log(
    data_dir=data_dir,
    durability_mode=DurabilityMode.ASYNC,
    # Make background fsync very unlikely to run before we crash.
    fsync_interval_ms=100000,
    fsync_batch_records=1000000,
)
for i in range({N}):
    log.append(f"R{{i}}".encode("ascii"))
_os._exit(0)
"""
            _run_crash_script(repo_root=repo_root, data_dir=data_dir, script=script)

            log2 = Log(
                data_dir=data_dir,
                durability_mode=DurabilityMode.SYNC,  # recovery mode is independent of durability choice
                fsync_batch_records=1,
            )
            self.addCleanup(log2.close)
            recovered = log2.read_all()

            expected = [f"R{i}".encode("ascii") for i in range(N)]
            self.assertLessEqual(len(recovered), N)
            self.assertEqual(recovered, expected[: len(recovered)])

    def test_group_commit_correctness_single_fsync_batch(self) -> None:
        # In SYNC mode we require group commit: multiple appends should be acknowledged
        # by a single fsync batch when batching threshold is hit.
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            threads = []
            n = 10

            log = Log(
                data_dir=data_dir,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=n,     # fsync triggers when we have all n pending
                fsync_interval_ms=100000,  # don't trigger by time
            )
            self.addCleanup(log.close)

            barrier = threading.Barrier(n)

            def do_append(i: int) -> None:
                barrier.wait()
                log.append(f"G{i}".encode("ascii"))

            for i in range(n):
                t = threading.Thread(target=do_append, args=(i,))
                t.start()
                threads.append(t)
            for t in threads:
                t.join(timeout=5)
                self.assertFalse(t.is_alive(), "append thread should not hang waiting for fsync")

            self.assertEqual(log.fsync_batches_completed, 1, "Expected a single group-commit fsync batch")

            log3 = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log3.close)
            recovered = log3.read_all()
            self.assertEqual(len(recovered), n)

    def test_ordering_guarantee_under_async(self) -> None:
        # Even under ASYNC, offsets are assigned monotonically and recovery must not reorder data.
        # After a crash, recovered data must be a prefix of the appended sequence.
        with TemporaryDirectory() as td:
            repo_root = os.path.dirname(os.path.dirname(__file__))
            data_dir = os.path.join(td, "data")
            N = 300

            script = rf"""
import os
import os as _os
from wal import Log, DurabilityMode

data_dir = os.environ["WAL_DATA_DIR"]
log = Log(
    data_dir=data_dir,
    durability_mode=DurabilityMode.ASYNC,
    fsync_interval_ms=100000,
    fsync_batch_records=1000000,
)
for i in range({N}):
    log.append(f"O{{i:06d}}".encode("ascii"))
_os._exit(0)
"""
            _run_crash_script(repo_root=repo_root, data_dir=data_dir, script=script)

            log2 = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log2.close)
            recovered = log2.read_all()
            expected = [f"O{i:06d}".encode("ascii") for i in range(N)]
            self.assertEqual(recovered, expected[: len(recovered)])


if __name__ == "__main__":
    unittest.main(verbosity=2)

