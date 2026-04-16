"""Open/create log file, run recovery (Week 1), attach DurabilityManager (Week 2)."""

from __future__ import annotations

import os
from typing import BinaryIO

from .durability_manager import DurabilityManager, DurabilityMode
from .recovery import recover


class DurableWAL:
    """Append-only WAL with explicit SYNC or ASYNC durability (see DurabilityManager)."""

    def __init__(
        self,
        path: str,
        mode: DurabilityMode,
        *,
        sync_batch_max_records: int = 16,
        sync_batch_interval_ms: float = 10.0,
        async_flush_max_records: int = 32,
        async_flush_interval_ms: float = 50.0,
        fsync_hook=None,
    ) -> None:
        self._path = path
        if not os.path.exists(path):
            open(path, "wb").close()
        # Recovery first (truncate invalid tail) — same contract as C++.
        next_off, _ = recover(path)
        self._f: BinaryIO = open(path, "r+b", buffering=0)
        self._fd = self._f.fileno()
        self._dm = DurabilityManager(
            self._fd,
            mode,
            next_offset=next_off,
            sync_batch_max_records=sync_batch_max_records,
            sync_batch_interval_ms=sync_batch_interval_ms,
            async_flush_max_records=async_flush_max_records,
            async_flush_interval_ms=async_flush_interval_ms,
            fsync_hook=fsync_hook,
        )

    def append(self, payload: bytes) -> int:
        return self._dm.append(payload)

    @property
    def fsync_count(self) -> int:
        return self._dm.fsync_count

    def close(self) -> None:
        self._dm.close()
        self._f.flush()
        os.fsync(self._fd)
        self._f.close()

    @property
    def path(self) -> str:
        return self._path
