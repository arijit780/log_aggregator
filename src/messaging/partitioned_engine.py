"""
High-level API: topics, groups, poll (assigned partitions only), commit per (group, topic, partition).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional

from ..durability import DurabilityMode
from ..formats import RecordEncoder
from ..read import LogReader
from .group_coordinator import GroupCoordinator
from .group_offset_store import GroupOffsetStore
from .topic_manager import TopicManager


@dataclass(frozen=True)
class PolledRecord:
    topic: str
    partition: int
    offset: int
    record: bytes


class PartitionedLogEngine:
    """
    Single-node façade: TopicManager + GroupCoordinator + durable group offsets.

    Offsets are per (group_id, topic, partition), never global across partitions.
    """

    def __init__(
        self,
        *,
        data_root: str,
        max_segment_size_bytes: int = 10 * 1024 * 1024,
        durability_mode: DurabilityMode = DurabilityMode.SYNC,
        fsync_interval_ms: int = 10,
        fsync_batch_records: int = 128,
        encoder: Optional[RecordEncoder] = None,
    ) -> None:
        self.data_root = data_root
        os.makedirs(data_root, exist_ok=True)
        self._topics = TopicManager(
            data_root=data_root,
            max_segment_size_bytes=max_segment_size_bytes,
            durability_mode=durability_mode,
            fsync_interval_ms=fsync_interval_ms,
            fsync_batch_records=fsync_batch_records,
            encoder=encoder,
        )
        self._coord = GroupCoordinator(topic_manager=self._topics)
        self._offsets = GroupOffsetStore(dir_path=os.path.join(data_root, "group_offsets"))

    @property
    def topics(self) -> TopicManager:
        return self._topics

    @property
    def coordinator(self) -> GroupCoordinator:
        return self._coord

    def create_topic(self, name: str, num_partitions: int, *, exist_ok: bool = False) -> None:
        self._topics.create_topic(name, num_partitions, exist_ok=exist_ok)

    def append(self, topic: str, key: Optional[bytes], value: bytes) -> tuple[int, int]:
        return self._topics.append(topic, key, value)

    def join_group(self, group_id: str, topic: str, consumer_id: str) -> None:
        self._coord.join_group(group_id, topic, consumer_id)

    def leave_group(self, group_id: str, consumer_id: str) -> None:
        self._coord.leave_group(group_id, consumer_id)

    def assigned_partitions(self, group_id: str, consumer_id: str):
        return self._coord.assigned_partitions(group_id, consumer_id)

    def _committed_clamped(self, group_id: str, topic: str, partition: int) -> int:
        log = self._topics.partition_log(topic, partition)
        stored = self._offsets.load(group_id, topic, partition)
        latest = log.latest_record_offset()
        if stored > latest:
            self._offsets.store(group_id, topic, partition, latest)
            return latest
        return stored

    def poll(self, group_id: str, consumer_id: str, max_records: int) -> List[PolledRecord]:
        """
        Returns records only from partitions assigned to this consumer.
        One pass per round tries at most one new record per assigned partition (fair order).
        """
        if max_records <= 0:
            return []
        topic = self._coord.topic_for_group(group_id)
        parts = sorted(self._coord.assigned_partitions(group_id, consumer_id))
        if not parts:
            return []

        readers: dict[int, LogReader] = {}
        for p in parts:
            log = self._topics.partition_log(topic, p)
            readers[p] = LogReader(
                data_dir=log.data_dir,
                max_segment_size_bytes=log.max_segment_size_bytes,
                encoder=log.encoder,
            )

        out: List[PolledRecord] = []
        while len(out) < max_records:
            round_got = 0
            for p in parts:
                if len(out) >= max_records:
                    break
                committed = self._committed_clamped(group_id, topic, p)
                next_off = committed + 1
                readers[p].refresh()
                lo = readers[p].latest_offset()
                if next_off > lo:
                    continue
                chunk = readers[p].read(offset=next_off, max_bytes=16 * 1024 * 1024)
                if not chunk:
                    continue
                r = chunk[0]
                out.append(
                    PolledRecord(
                        topic=topic,
                        partition=p,
                        offset=r.offset,
                        record=r.payload,
                    )
                )
                round_got += 1
            if round_got == 0:
                break
        return out

    def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        if offset < -1:
            raise ValueError("offset must be >= -1")
        bound_topic = self._coord.topic_for_group(group_id)
        if topic != bound_topic:
            raise ValueError(
                f"group {group_id!r} reads topic {bound_topic!r}, not {topic!r}"
            )
        log = self._topics.partition_log(topic, partition)
        latest = log.latest_record_offset()
        if offset > latest:
            raise ValueError(
                f"cannot commit beyond last record: partition={partition} offset={offset} latest={latest}"
            )
        current = self._committed_clamped(group_id, topic, partition)
        if offset < current:
            raise ValueError(
                f"non-monotonic commit: current={current} attempted={offset} "
                f"for ({group_id!r}, {topic!r}, p{partition})"
            )
        self._offsets.store(group_id, topic, partition, offset)

    def close(self) -> None:
        self._topics.close_all()
