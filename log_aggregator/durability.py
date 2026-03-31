import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class DurabilityMode(str, Enum):
    """
    Explicit durability/ACK contract.

    SYNC:
      write -> fsync -> ACK
      Once ACK returns, data must survive process crash and OS crash.

    ASYNC:
      write -> ACK -> fsync later (background)
      Data may be lost if crash happens before background fsync.
      Ordering is still preserved; recovery must never produce corruption.
    """

    SYNC = "sync"
    ASYNC = "async"


@dataclass
class _Pending:
    path: str
    # For SYNC, the caller waits for this event to be set after fsync completes.
    ack_event: Optional[threading.Event]


class DurabilityManager:
    """
    Batching + group commit manager.

    Core idea:
      - Append writes bytes and flushes user-space buffers.
      - We enqueue the file descriptor for fsync.
      - A background thread batches multiple pending appends and performs fsync
        on the involved file descriptors.

    ACK semantics:
      - SYNC: append blocks until its batch's fsync completes.
      - ASYNC: append returns immediately after write+flush; fsync happens later.
    """

    def __init__(
        self,
        *,
        mode: DurabilityMode,
        fsync_interval_ms: int = 10,
        fsync_batch_records: int = 128,
    ) -> None:
        if fsync_interval_ms <= 0:
            raise ValueError("fsync_interval_ms must be > 0")
        if fsync_batch_records <= 0:
            raise ValueError("fsync_batch_records must be > 0")

        self.mode = mode
        self.fsync_interval_ms = fsync_interval_ms
        self.fsync_batch_records = fsync_batch_records

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: list[_Pending] = []
        self._closing = False

        # Observability for tests (real fsync, no mocking).
        self.fsync_batches_completed = 0

        self._thread = threading.Thread(
            target=self._run,
            name="wal-fsync-batcher",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        with self._cv:
            self._closing = True
            self._cv.notify_all()
        self._thread.join(timeout=5)

    def on_append(self, *, path: str) -> None:
        """
        Called after bytes are written+flushed.

        SYNC:
          enqueue -> wait until background thread fsyncs -> return (ACK)

        ASYNC:
          enqueue -> return immediately (ACK), background fsync later
        """
        if self.mode == DurabilityMode.SYNC:
            ev = threading.Event()
            with self._cv:
                self._pending.append(_Pending(path=path, ack_event=ev))
                # Wake the batcher in case it is sleeping on interval.
                self._cv.notify()
            # Wait until fsync finishes for the batch that includes this append.
            ev.wait()
            return

        if self.mode == DurabilityMode.ASYNC:
            with self._cv:
                self._pending.append(_Pending(path=path, ack_event=None))
                # Wake the batcher for throughput (still bounded by batching settings).
                self._cv.notify()
            return

        raise ValueError(f"Unknown durability mode: {self.mode!r}")

    def _run(self) -> None:
        interval_s = self.fsync_interval_ms / 1000.0
        next_deadline = time.monotonic() + interval_s

        while True:
            batch: list[_Pending] = []
            with self._cv:
                while True:
                    if self._closing:
                        break
                    now = time.monotonic()
                    have_enough = len(self._pending) >= self.fsync_batch_records
                    expired = now >= next_deadline
                    if self._pending and (have_enough or expired):
                        # Take all pending at once for max group commit effect.
                        batch = self._pending
                        self._pending = []
                        next_deadline = time.monotonic() + interval_s
                        break

                    timeout = max(0.0, next_deadline - now)
                    self._cv.wait(timeout=timeout)

                if self._closing:
                    # Best-effort: flush any remaining pending items before exit.
                    if self._pending:
                        batch = self._pending
                        self._pending = []
                    else:
                        return

            # Perform fsync outside the lock.
            self._fsync_batch(batch)

    def _fsync_batch(self, batch: list[_Pending]) -> None:
        if not batch:
            return

        # fsync each distinct segment file once. This is group commit across records
        # and works safely even if the writer closed/rolled segments, because we
        # open the file at fsync time (no reliance on long-lived fds).
        paths = sorted({p.path for p in batch})
        for path in paths:
            # Use r+b so fsync operates on the same file, and close immediately after.
            with open(path, "r+b") as f:
                f.flush()
                os.fsync(f.fileno())

        self.fsync_batches_completed += 1

        # ACK all SYNC waiters whose appends are now durable.
        for p in batch:
            if p.ack_event is not None:
                p.ack_event.set()

