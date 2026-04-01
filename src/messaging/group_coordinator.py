"""
Consumer groups: partition assignment + rebalancing (single-node, in-memory).

What is in memory: membership and current assignment only.
Committed offsets remain on disk (GroupOffsetStore).

Invariant: each partition of a topic is assigned to at most one consumer in the group.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Set

from .topic_manager import TopicManager


def assign_partitions_round_robin(
    partitions: List[int], consumer_ids: List[str]
) -> Dict[str, List[int]]:
    """
    Partitions [0,1,2,3], consumers [C1,C2] → C1→[0,2], C2→[1,3].
    """
    if not consumer_ids:
        return {}
    sc = sorted(consumer_ids)
    sp = sorted(partitions)
    out: Dict[str, List[int]] = {c: [] for c in sc}
    for i, p in enumerate(sp):
        out[sc[i % len(sc)]].append(p)
    return out


@dataclass
class _GroupState:
    topic: str
    members: Set[str] = field(default_factory=set)
    # consumer_id -> assigned partition indices
    assignment: Dict[str, FrozenSet[int]] = field(default_factory=dict)


class GroupCoordinator:
    """
    Tracks group membership and assignments. Call join / leave to trigger rebalance.

    Thread-safe. Assignment is recomputed entirely on each membership change (stop →
    redistribute → resume). Uncommitted offsets are not advanced, so at-least-once
    semantics are preserved across rebalance.
    """

    def __init__(self, *, topic_manager: TopicManager) -> None:
        self._tm = topic_manager
        self._lock = threading.Lock()
        self._groups: Dict[str, _GroupState] = {}

    def join_group(self, group_id: str, topic: str, consumer_id: str) -> None:
        with self._lock:
            if group_id not in self._groups:
                if not self._tm.has_topic(topic):
                    raise KeyError(f"unknown topic: {topic!r}")
                self._groups[group_id] = _GroupState(topic=topic)
            gs = self._groups[group_id]
            if gs.topic != topic:
                raise ValueError(
                    f"group {group_id!r} is bound to topic {gs.topic!r}, not {topic!r}"
                )
            gs.members.add(consumer_id)
            self._rebalance_locked(gs)

    def leave_group(self, group_id: str, consumer_id: str) -> None:
        with self._lock:
            gs = self._groups.get(group_id)
            if gs is None:
                return
            gs.members.discard(consumer_id)
            if not gs.members:
                gs.assignment.clear()
                del self._groups[group_id]
                return
            self._rebalance_locked(gs)

    def topic_for_group(self, group_id: str) -> str:
        with self._lock:
            gs = self._groups.get(group_id)
            if gs is None:
                raise KeyError(group_id)
            return gs.topic

    def assigned_partitions(self, group_id: str, consumer_id: str) -> FrozenSet[int]:
        with self._lock:
            gs = self._groups.get(group_id)
            if gs is None:
                raise KeyError(group_id)
            if consumer_id not in gs.members:
                raise ValueError(f"consumer {consumer_id!r} not in group {group_id!r}")
            return frozenset(gs.assignment.get(consumer_id, frozenset()))

    def _rebalance_locked(self, gs: _GroupState) -> None:
        consumers = sorted(gs.members)
        n = self._tm.num_partitions(gs.topic)
        parts = list(range(n))
        raw = assign_partitions_round_robin(parts, consumers)
        gs.assignment = {c: frozenset(raw.get(c, [])) for c in consumers}
