"""Week 5: partitions, consumer groups, round-robin assignment, rebalancing."""

import unittest
from tempfile import TemporaryDirectory

from src import (
    DurabilityMode,
    PartitionedLogEngine,
    assign_partitions_round_robin,
)


class TestPartitioningAndGroups(unittest.TestCase):
    def test_partition_routing_same_key_stable(self) -> None:
        with TemporaryDirectory() as td:
            eng = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng.close)
            eng.create_topic("t1", num_partitions=8)
            key = b"user-42"
            parts = set()
            for _ in range(20):
                p, _ = eng.append("t1", key, b"v")
                parts.add(p)
            self.assertEqual(len(parts), 1, "same key must map to one partition")

            seen = set()
            for i in range(50):
                p, _ = eng.append("t1", f"k{i}".encode(), b"x")
                seen.add(p)
            self.assertGreater(len(seen), 1, "different keys should spread across partitions")

    def test_assign_round_robin_distribution(self) -> None:
        a = assign_partitions_round_robin([0, 1, 2, 3], ["C1", "C2"])
        self.assertEqual(sorted(a["C1"]), [0, 2])
        self.assertEqual(sorted(a["C2"]), [1, 3])

    def test_two_consumers_four_partitions(self) -> None:
        with TemporaryDirectory() as td:
            eng = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng.close)
            eng.create_topic("events", 4)
            eng.join_group("g1", "events", "c1")
            eng.join_group("g1", "events", "c2")
            a1 = eng.assigned_partitions("g1", "c1")
            a2 = eng.assigned_partitions("g1", "c2")
            self.assertEqual(len(a1) + len(a2), 4)
            self.assertEqual(a1 & a2, frozenset())
            self.assertEqual(a1 | a2, frozenset({0, 1, 2, 3}))

    def test_rebalance_on_join(self) -> None:
        with TemporaryDirectory() as td:
            eng = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng.close)
            eng.create_topic("t", 4)
            eng.join_group("g", "t", "c1")
            eng.join_group("g", "t", "c2")
            eng.join_group("g", "t", "c3")
            a1 = eng.assigned_partitions("g", "c1")
            a2 = eng.assigned_partitions("g", "c2")
            a3 = eng.assigned_partitions("g", "c3")
            self.assertEqual(len(a1 | a2 | a3), 4)
            self.assertEqual(len(a1 & a2), 0)
            self.assertEqual(len(a1 & a3), 0)
            self.assertEqual(len(a2 & a3), 0)
            self.assertEqual(len(a1) + len(a2) + len(a3), 4)

    def test_rebalance_on_leave(self) -> None:
        with TemporaryDirectory() as td:
            eng = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng.close)
            eng.create_topic("t", 4)
            eng.join_group("g", "t", "c1")
            eng.join_group("g", "t", "c2")
            eng.leave_group("g", "c2")
            a1 = eng.assigned_partitions("g", "c1")
            self.assertEqual(a1, frozenset({0, 1, 2, 3}))

    def test_at_least_once_after_rebalance_uncommitted(self) -> None:
        with TemporaryDirectory() as td:
            eng = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng.close)
            eng.create_topic("t", 2)
            eng.append("t", b"k", b"payload-A")

            eng.join_group("g", "t", "c1")
            # c1 may own p0 or p1 depending on sort order
            batch = eng.poll("g", "c1", max_records=10)
            self.assertTrue(any(r.record == b"payload-A" for r in batch))
            # no commit — simulate crash / leave
            for pr in batch:
                if pr.record == b"payload-A":
                    p_seen = pr.partition
                    off_seen = pr.offset
                    break
            else:
                self.fail("expected payload")
            eng.leave_group("g", "c1")
            eng.join_group("g", "t", "c2")
            batch2 = eng.poll("g", "c2", max_records=10)
            again = [r for r in batch2 if r.partition == p_seen and r.record == b"payload-A"]
            self.assertTrue(
                len(again) >= 1,
                "new assignee must see uncommitted record (at-least-once)",
            )
            eng.close()

            # Simulate new process: attach existing topic layout from disk
            eng2 = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng2.close)
            eng2.create_topic("t", 2, exist_ok=True)
            eng2.join_group("g", "t", "c2")
            b3 = eng2.poll("g", "c2", max_records=10)
            self.assertTrue(any(r.offset == off_seen and r.record == b"payload-A" for r in b3))

    def test_poll_commit_monoconsumer(self) -> None:
        with TemporaryDirectory() as td:
            eng = PartitionedLogEngine(
                data_root=td,
                durability_mode=DurabilityMode.SYNC,
                fsync_batch_records=1,
            )
            self.addCleanup(eng.close)
            eng.create_topic("t", 2)
            eng.append("t", b"k", b"m1")
            eng.append("t", b"k", b"m2")
            eng.join_group("g", "t", "c1")
            pr = eng.poll("g", "c1", max_records=1)[0]
            self.assertEqual(pr.record, b"m1")
            eng.commit("g", "t", pr.partition, pr.offset)
            second = eng.poll("g", "c1", max_records=5)
            payloads = [r.record for r in second]
            self.assertIn(b"m2", payloads)
            self.assertNotIn(b"m1", payloads)


if __name__ == "__main__":
    unittest.main(verbosity=2)
