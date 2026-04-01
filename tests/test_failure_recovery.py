"""
Week 4: failure model + recovery correctness.

Invariant: after recovery, on-disk log is always a valid prefix of logical records
(no partial record at tail, no CRC-invalid record, no reordering).
"""

import os
import random
import unittest
from tempfile import TemporaryDirectory

from src import (
    ConsumerManager,
    DurabilityMode,
    FailureInjectionError,
    FailureInjector,
    FailureStage,
    Log,
)


class TestFailureRecovery(unittest.TestCase):
    def _prefill(self, data_dir: str, n: int = 5) -> list[bytes]:
        log = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
        payloads = [f"P{i}".encode("ascii") for i in range(n)]
        for p in payloads:
            log.append(p)
        log.close()
        return payloads

    def test_crash_at_every_append_and_fsync_stage(self) -> None:
        """Inject failure at each write/fsync stage; recovery always yields a valid prefix."""
        stages = [
            FailureStage.BEFORE_WRITE,
            FailureStage.AFTER_LENGTH,
            FailureStage.AFTER_CRC,
            FailureStage.MID_PAYLOAD,
            FailureStage.AFTER_WRITE,
            FailureStage.BEFORE_FSYNC,
            FailureStage.AFTER_FSYNC,
        ]
        for stage in stages:
            with self.subTest(stage=stage.value):
                with TemporaryDirectory() as td:
                    data_dir = os.path.join(td, "data")
                    pre = self._prefill(data_dir, n=5)

                    inj = FailureInjector(trigger=stage, action="raise")
                    log2 = Log(
                        data_dir=data_dir,
                        durability_mode=DurabilityMode.SYNC,
                        fsync_batch_records=1,
                        fsync_interval_ms=10_000,
                        failure_injector=inj,
                    )
                    try:
                        log2.append(b"POISON")
                    except (FailureInjectionError, RuntimeError):
                        pass
                    log2.close()

                    log3 = Log(
                        data_dir=data_dir,
                        durability_mode=DurabilityMode.SYNC,
                        fsync_batch_records=1,
                    )
                    self.addCleanup(log3.close)
                    got = log3.read_all()
                    self.assertTrue(
                        got == pre or got == pre + [b"POISON"],
                        f"unexpected prefix after {stage.value}: len={len(got)}",
                    )
                    # Full record is on disk after staged write completes (after_write) and after.
                    full_write_stages = {
                        FailureStage.AFTER_WRITE,
                        FailureStage.BEFORE_FSYNC,
                        FailureStage.AFTER_FSYNC,
                    }
                    if stage in full_write_stages:
                        self.assertEqual(len(got), 6)
                    else:
                        self.assertEqual(len(got), 5)

    def test_random_corruption_truncates_tail(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            pre = self._prefill(data_dir, n=20)
            seg = os.path.join(data_dir, [f for f in os.listdir(data_dir) if f.endswith(".seg")][0])
            rng = random.Random(42)
            FailureInjector.flip_random_bytes(seg, rng=rng, max_flips=8)

            logr = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(logr.close)
            got = logr.read_all()
            self.assertLessEqual(len(got), len(pre))
            self.assertEqual(got, pre[: len(got)])

    def test_async_tail_loss_no_corruption(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            log = Log(
                data_dir=data_dir,
                durability_mode=DurabilityMode.ASYNC,
                fsync_interval_ms=100_000,
                fsync_batch_records=1_000_000,
            )
            self.addCleanup(log.close)
            for i in range(50):
                log.append(f"A{i}".encode("ascii"))
            log.close()

            log2 = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log2.close)
            got = log2.read_all()
            expected = [f"A{i}".encode("ascii") for i in range(50)]
            self.assertLessEqual(len(got), 50)
            self.assertEqual(got, expected[: len(got)])

    def test_consumer_offset_clamped_after_log_truncation(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            off_dir = os.path.join(td, "offsets")
            log = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log.close)
            for i in range(10):
                log.append(f"C{i}".encode("ascii"))
            log.close()

            cm = ConsumerManager(log=Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1), offsets_dir=off_dir)
            self.addCleanup(cm.log.close)
            cm.commit("u1", 8)

            log_wrapped = cm.log
            log_wrapped.close()

            seg = os.path.join(data_dir, [f for f in os.listdir(data_dir) if f.endswith(".seg")][0])
            size = os.path.getsize(seg)
            with open(seg, "r+b") as f:
                f.truncate(max(0, size - 40))
                f.flush()

            log_after = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log_after.close)
            cm2 = ConsumerManager(log=log_after, offsets_dir=off_dir)
            latest = log_after.latest_record_offset()
            co = cm2.committed_offset("u1")
            self.assertLessEqual(co, latest)

    def test_deterministic_recovery_same_input(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            self._prefill(data_dir, n=8)
            seg = os.path.join(data_dir, [f for f in os.listdir(data_dir) if f.endswith(".seg")][0])
            with open(seg, "r+b") as f:
                f.truncate(os.path.getsize(seg) - 7)
                f.flush()

            def recover_once() -> tuple[list[bytes], int]:
                L = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
                try:
                    return L.read_all(), L.next_offset
                finally:
                    L.close()

            a, na = recover_once()
            b, nb = recover_once()
            self.assertEqual(a, b)
            self.assertEqual(na, nb)


if __name__ == "__main__":
    unittest.main(verbosity=2)
