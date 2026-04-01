"""
Topics → N independent partition logs. No global ordering across partitions.

Append routing:
  - key is None → round-robin (per topic, thread-safe)
  - else → partition = stable_hash(key) % num_partitions (same key → same partition)
"""

from __future__ import annotations

import hashlib
import os
import re
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional

from ..durability import DurabilityMode
from ..formats import RecordEncoder
from ..storage import Log


_TOPIC_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


@dataclass
class _Topic:
    name: str
    num_partitions: int
    logs: List[Log]


class TopicManager:
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
        self._max_seg = max_segment_size_bytes
        self._durability_mode = durability_mode
        self._fsync_interval_ms = fsync_interval_ms
        self._fsync_batch_records = fsync_batch_records
        self._encoder = encoder if encoder is not None else RecordEncoder()
        self._topics: Dict[str, _Topic] = {}
        self._lock = threading.Lock()
        self._rr: Dict[str, int] = {}
        self._rr_lock = threading.Lock()

    def create_topic(self, name: str, num_partitions: int, *, exist_ok: bool = False) -> None:
        if not _TOPIC_NAME_RE.match(name):
            raise ValueError("invalid topic name")
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")
        base = os.path.join(self.data_root, "topics", name)
        with self._lock:
            if name in self._topics:
                if exist_ok:
                    return
                raise ValueError(f"topic already exists: {name!r}")
            if os.path.isdir(base):
                if not exist_ok:
                    raise ValueError(
                        f"topic directory already exists on disk: {name!r} (use exist_ok=True)"
                    )
                nums = sorted(
                    int(x[1:])
                    for x in os.listdir(base)
                    if x.startswith("p") and x[1:].isdigit()
                )
                if nums != list(range(num_partitions)):
                    raise ValueError("existing topic layout does not match num_partitions")
                logs: List[Log] = [
                    Log(
                        data_dir=os.path.join(base, f"p{p}"),
                        max_segment_size_bytes=self._max_seg,
                        durability_mode=self._durability_mode,
                        fsync_interval_ms=self._fsync_interval_ms,
                        fsync_batch_records=self._fsync_batch_records,
                        encoder=self._encoder,
                    )
                    for p in range(num_partitions)
                ]
                self._topics[name] = _Topic(name=name, num_partitions=num_partitions, logs=logs)
                self._rr.setdefault(name, 0)
                return

            logs = []
            for p in range(num_partitions):
                d = os.path.join(base, f"p{p}")
                logs.append(
                    Log(
                        data_dir=d,
                        max_segment_size_bytes=self._max_seg,
                        durability_mode=self._durability_mode,
                        fsync_interval_ms=self._fsync_interval_ms,
                        fsync_batch_records=self._fsync_batch_records,
                        encoder=self._encoder,
                    )
                )
            self._topics[name] = _Topic(name=name, num_partitions=num_partitions, logs=logs)
            self._rr[name] = 0

    def has_topic(self, name: str) -> bool:
        with self._lock:
            return name in self._topics

    def num_partitions(self, topic: str) -> int:
        with self._lock:
            t = self._topics.get(topic)
            if t is None:
                raise KeyError(topic)
            return t.num_partitions

    def partition_log(self, topic: str, partition: int) -> Log:
        with self._lock:
            t = self._topics.get(topic)
            if t is None:
                raise KeyError(topic)
            if partition < 0 or partition >= t.num_partitions:
                raise ValueError("partition out of range")
            return t.logs[partition]

    def append(self, topic: str, key: Optional[bytes], value: bytes) -> tuple[int, int]:
        """
        Returns (partition_index, offset_within_partition).
        """
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("value must be bytes-like")
        vb = bytes(value)
        with self._lock:
            t = self._topics.get(topic)
            if t is None:
                raise KeyError(topic)

            if key is None:
                with self._rr_lock:
                    c = self._rr[topic]
                    partition = c % t.num_partitions
                    self._rr[topic] = c + 1
            else:
                if not isinstance(key, (bytes, bytearray, memoryview)):
                    raise TypeError("key must be bytes-like or None")
                kb = bytes(key)
                partition = _stable_partition(kb, t.num_partitions)

            offset = t.logs[partition].append(vb)
            return partition, offset

    def close_all(self) -> None:
        with self._lock:
            for t in self._topics.values():
                for lg in t.logs:
                    lg.close()

    def __enter__(self) -> TopicManager:
        return self

    def __exit__(self, *exc) -> None:
        self.close_all()


def _stable_partition(key: bytes, n: int) -> int:
    """Deterministic across processes (unlike hash())."""
    h = hashlib.md5(key).digest()
    return int.from_bytes(h[:8], "big", signed=False) % n
