"""
Public API — re-exports from subpackages so callers stay stable:

  from src import Log, ConsumerManager, ...

Layout:
  formats/     — record framing (length + CRC)
  storage/     — segments, append, recovery
  durability/  — SYNC/ASYNC fsync batching / group commit
  read/        — LogReader, Record (read-only)
  consumer/    — OffsetStore, ConsumerManager
  messaging/   — TopicManager, groups, PartitionedLogEngine
  testing/     — FailureInjector (for recovery / durability tests)
"""

from .consumer import ConsumerManager, ConsumerState, OffsetStore
from .durability import DurabilityManager, DurabilityMode
from .formats import RecordEncoder
from .messaging import (
    GroupCoordinator,
    GroupOffsetStore,
    PartitionedLogEngine,
    PolledRecord,
    TopicManager,
    assign_partitions_round_robin,
)
from .read import LogReader, Record
from .storage import Log, Segment, SegmentInfo
from .testing import FailureInjectionError, FailureInjector, FailureStage

__all__ = [
    "Log",
    "Segment",
    "SegmentInfo",
    "RecordEncoder",
    "DurabilityMode",
    "DurabilityManager",
    "ConsumerManager",
    "ConsumerState",
    "OffsetStore",
    "Record",
    "LogReader",
    "FailureInjector",
    "FailureStage",
    "FailureInjectionError",
    "TopicManager",
    "GroupCoordinator",
    "GroupOffsetStore",
    "PartitionedLogEngine",
    "PolledRecord",
    "assign_partitions_round_robin",
]
