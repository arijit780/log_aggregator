"""
DurabilityManager — SYNC vs ASYNC fsync strategy, group commit (SYNC), batched background fsync (ASYNC).

Design Q&A (Week 2):
- Ten fast SYNC appends with sync_batch_max_records=5: the commit thread performs one fsync per drained
  batch (up to 5 pending ACKs), so about ceil(10/5)=2 fsync calls — not one per append — *when multiple
  appends are in the queue before a drain* (e.g. concurrent appends). Sequential appends still ACK one
  at a time, so each may fsync alone unless the timer batches (interval wake).
- Crash during SYNC batching before fsync: no Event was set — callers still blocked; recovery truncates
  any torn tail; no ACK implied durability.
- ASYNC returns only after write(); recovery still validates records — corrupted/torn tails are truncated,
  so clients never observe invalid payloads from scan_prefix/recover.

Contract (mandatory):
  SYNC:  ACK (append returns) only after os.fsync(fd) completes for a batch that includes this write.
         Group commit: multiple appends may share one fsync; all wait until that fsync finishes.
  ASYNC: ACK immediately after os.write returns (no fsync). Background thread fsyncs periodically
         (every async_flush_interval_ms OR after async_flush_max_records writes since last fsync).

Real fsync: os.fsync(self._fd). Optional fsync_hook(fd) for tests (must still invoke real fsync unless hook replaces entirely).
"""

from __future__ import annotations

import os
import threading
from collections import deque
from enum import Enum
from typing import Callable

from .record_format import encode_record


class DurabilityMode(str, Enum):
    SYNC = "sync"
    ASYNC = "async"


class DurabilityManager:
    def __init__(
        self,
        fd: int,
        mode: DurabilityMode,
        *,
        next_offset: int = 0,
        # SYNC group commit: one fsync covers up to sync_batch_max pending ACKs, or fires on interval.
        sync_batch_max_records: int = 16,
        sync_batch_interval_ms: float = 10.0,
        # ASYNC: amortize fsync in background.
        async_flush_max_records: int = 32,
        async_flush_interval_ms: float = 50.0,
        fsync_hook: Callable[[int], None] | None = None,
    ) -> None:
        self._fd = fd
        self._mode = mode
        self._next_offset = next_offset
        self._lock = threading.Lock()
        self._fsync_hook = fsync_hook
        self.fsync_count = 0  # observable for tests

        self._sync_batch_max = max(1, sync_batch_max_records)
        self._sync_interval = sync_batch_interval_ms / 1000.0
        self._sync_pending: deque[threading.Event] = deque()
        self._sync_cv = threading.Condition(self._lock)

        self._async_max = max(1, async_flush_max_records)
        self._async_interval = async_flush_interval_ms / 1000.0
        self._writes_since_fsync = 0
        self._async_cv = threading.Condition(self._lock)

        self._stop = threading.Event()
        self._sync_thread: threading.Thread | None = None
        self._async_thread: threading.Thread | None = None

        if mode == DurabilityMode.SYNC:
            self._sync_thread = threading.Thread(target=self._sync_commit_loop, name="wal-sync-commit", daemon=True)
            self._sync_thread.start()
        else:
            self._async_thread = threading.Thread(target=self._async_flush_loop, name="wal-async-fsync", daemon=True)
            self._async_thread.start()

    def set_next_offset(self, off: int) -> None:
        with self._lock:
            self._next_offset = off

    def _do_fsync(self) -> None:
        if self._fsync_hook is not None:
            self._fsync_hook(self._fd)
        else:
            os.fsync(self._fd)
        self.fsync_count += 1

    def append(self, payload: bytes) -> int:
        """Write one record at current offset; semantics depend on mode (see module docstring)."""
        if self._mode == DurabilityMode.SYNC:
            return self._append_sync(payload)
        return self._append_async(payload)

    def _append_sync(self, payload: bytes) -> int:
        """
        write → (group commit / batch) → fsync → then ACK (Event set).
        Caller unblocks only after fsync for their record completes.
        """
        evt = threading.Event()
        with self._sync_cv:
            assigned = self._next_offset
            blob = encode_record(assigned, payload)
            os.write(self._fd, blob)
            self._next_offset = assigned + 1
            self._sync_pending.append(evt)
            self._sync_cv.notify()
        evt.wait()
        return assigned

    def _sync_commit_loop(self) -> None:
        """
        Batching: drain up to sync_batch_max_records pending ACKs, single fsync, signal all.
        Also wake on sync_batch_interval timer so small batches still flush.
        """
        while not self._stop.is_set():
            batch: list[threading.Event] = []
            with self._sync_cv:
                if not self._sync_pending:
                    self._sync_cv.wait(timeout=self._sync_interval)
                if self._stop.is_set():
                    break
                while self._sync_pending and len(batch) < self._sync_batch_max:
                    batch.append(self._sync_pending.popleft())

            if not batch:
                continue

            self._do_fsync()
            for e in batch:
                e.set()

        # Best-effort: any stragglers on shutdown (still durable before ACK)
        with self._sync_cv:
            tail = list(self._sync_pending)
            self._sync_pending.clear()
        if tail:
            self._do_fsync()
            for e in tail:
                e.set()

    def _append_async(self, payload: bytes) -> int:
        """write → ACK (return) immediately; fsync later in background (batched)."""
        with self._async_cv:
            assigned = self._next_offset
            blob = encode_record(assigned, payload)
            os.write(self._fd, blob)
            self._next_offset = assigned + 1
            self._writes_since_fsync += 1
            if self._writes_since_fsync >= self._async_max:
                self._async_cv.notify()
        return assigned

    def _async_flush_loop(self) -> None:
        while not self._stop.is_set():
            dirty = False
            with self._async_cv:
                self._async_cv.wait(timeout=self._async_interval)
                if self._stop.is_set():
                    break
                if self._writes_since_fsync > 0:
                    dirty = True
                    self._writes_since_fsync = 0
            if dirty:
                self._do_fsync()

        need = False
        with self._async_cv:
            if self._writes_since_fsync > 0:
                need = True
                self._writes_since_fsync = 0
        if need:
            self._do_fsync()

    def close(self) -> None:
        self._stop.set()
        with self._sync_cv:
            self._sync_cv.notify_all()
        with self._async_cv:
            self._async_cv.notify_all()
        if self._sync_thread:
            self._sync_thread.join(timeout=5.0)
        if self._async_thread:
            self._async_thread.join(timeout=5.0)
