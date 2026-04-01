import os
import unittest
from tempfile import TemporaryDirectory

from src import ConsumerManager, DurabilityMode, Log


class TestConsumerModel(unittest.TestCase):
    def test_basic_consumption_read_commit_progresses(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            offsets_dir = os.path.join(td, "offsets")

            log = Log(
                data_dir=data_dir,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(log.close)

            payloads = [f"V{i}".encode("ascii") for i in range(100)]
            for p in payloads:
                log.append(p)

            cm = ConsumerManager(log=log, offsets_dir=offsets_dir)
            consumer = "c1"

            # Read+commit sequentially in batches.
            cursor = 0
            while cursor < 100:
                recs = cm.read(consumer, cursor, max_bytes=1024)
                self.assertGreater(len(recs), 0)
                for r in recs:
                    self.assertEqual(r.offset, cursor)
                    self.assertEqual(r.payload, payloads[cursor])
                    cursor += 1
                cm.commit(consumer, cursor - 1)

            self.assertEqual(cm.committed_offset(consumer), 99)
            self.assertEqual(cm.next_offset(consumer), 100)

            # Reading beyond end returns empty.
            self.assertEqual(cm.read(consumer, 100, max_bytes=1024), [])

    def test_crash_before_commit_redelivers(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            offsets_dir = os.path.join(td, "offsets")

            log = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log.close)
            log.append(b"A")
            log.append(b"B")

            cm1 = ConsumerManager(log=log, offsets_dir=offsets_dir)
            consumer = "c2"

            recs = cm1.read_from_committed(consumer, max_bytes=1024)
            self.assertEqual([r.payload for r in recs], [b"A", b"B"])
            # Simulate crash: DO NOT commit, reconstruct manager.

            cm2 = ConsumerManager(log=log, offsets_dir=offsets_dir)
            recs2 = cm2.read_from_committed(consumer, max_bytes=1024)
            self.assertEqual([r.payload for r in recs2], [b"A", b"B"])

    def test_crash_after_commit_not_redelivered(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            offsets_dir = os.path.join(td, "offsets")

            log = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log.close)
            log.append(b"A")
            log.append(b"B")
            log.append(b"C")

            cm1 = ConsumerManager(log=log, offsets_dir=offsets_dir)
            consumer = "c3"

            recs = cm1.read_from_committed(consumer, max_bytes=1024)
            self.assertEqual([r.payload for r in recs], [b"A", b"B", b"C"])
            cm1.commit(consumer, 1)  # commit through B

            # "Crash" and restart: new manager must resume from offset 2.
            cm2 = ConsumerManager(log=log, offsets_dir=offsets_dir)
            self.assertEqual(cm2.next_offset(consumer), 2)
            recs2 = cm2.read_from_committed(consumer, max_bytes=1024)
            self.assertEqual([r.payload for r in recs2], [b"C"])

    def test_duplicate_handling_at_least_once(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            offsets_dir = os.path.join(td, "offsets")

            log = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log.close)
            for i in range(5):
                log.append(f"D{i}".encode("ascii"))

            consumer = "c4"
            cm1 = ConsumerManager(log=log, offsets_dir=offsets_dir)
            recs1 = cm1.read_from_committed(consumer, max_bytes=1024)
            # Process externally... crash before commit -> duplicate delivery.
            cm2 = ConsumerManager(log=log, offsets_dir=offsets_dir)
            recs2 = cm2.read_from_committed(consumer, max_bytes=1024)
            self.assertEqual([r.payload for r in recs1], [r.payload for r in recs2])

    def test_monotonic_commit_enforcement(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            offsets_dir = os.path.join(td, "offsets")

            log = Log(data_dir=data_dir, durability_mode=DurabilityMode.SYNC, fsync_batch_records=1)
            self.addCleanup(log.close)
            for i in range(20):
                log.append(f"M{i}".encode("ascii"))

            cm = ConsumerManager(log=log, offsets_dir=offsets_dir)
            consumer = "c5"
            cm.commit(consumer, 10)
            with self.assertRaises(ValueError):
                cm.commit(consumer, 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)

