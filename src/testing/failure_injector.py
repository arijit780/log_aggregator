"""
Programmatic failure injection for WAL recovery tests.

Used with real file I/O and real fsync — no mocked persistence.

Stages mirror crash points in the append + durability pipeline:
  append path: before_write → after_length → after_crc → mid_payload → after_write
  durability:  before_fsync → after_fsync (per batch, all paths fsynced)
"""

from __future__ import annotations

import os
import random
from enum import Enum
from typing import Optional


class FailureStage(str, Enum):
    BEFORE_WRITE = "before_write"
    AFTER_LENGTH = "after_length"
    AFTER_CRC = "after_crc"
    MID_PAYLOAD = "mid_payload"
    AFTER_WRITE = "after_write"
    BEFORE_FSYNC = "before_fsync"
    AFTER_FSYNC = "after_fsync"


class FailureInjectionError(Exception):
    """Raised when an injected failure aborts the current operation (in-process tests)."""

    def __init__(self, stage: FailureStage) -> None:
        self.stage = stage
        super().__init__(f"injected failure at {stage.value}")


class FailureInjector:
    """
    Fires once when the execution reaches `trigger` stage.

    - action="raise": raise FailureInjectionError (parent test catches and verifies recovery).
    - action="exit": os._exit(code) for subprocess crash simulation.
    """

    def __init__(
        self,
        *,
        trigger: Optional[FailureStage] = None,
        action: str = "raise",
        exit_code: int = 77,
    ) -> None:
        self.trigger = trigger
        self.action = action
        self.exit_code = exit_code
        self.fired_count = 0

    def maybe_fire(self, stage: str) -> None:
        if self.trigger is None:
            return
        if stage != self.trigger.value:
            return
        self.fired_count += 1
        if self.action == "exit":
            os._exit(self.exit_code)
        raise FailureInjectionError(self.trigger)

    @staticmethod
    def flip_random_bytes(path: str, *, rng: random.Random, max_flips: int = 3) -> None:
        """Corrupt a segment file at arbitrary positions (CRC should catch on recovery)."""
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return
        size = os.path.getsize(path)
        with open(path, "r+b") as f:
            n = min(max_flips, max(1, size // 512))
            for _ in range(n):
                pos = rng.randrange(0, size)
                f.seek(pos)
                b = f.read(1)
                if len(b) == 1:
                    f.seek(pos)
                    f.write(bytes([b[0] ^ 0xFF]))
            f.flush()

    @staticmethod
    def truncate_random_suffix(path: str, *, rng: random.Random) -> None:
        """Truncate the file to a random length (simulates torn writes / partial tail)."""
        if not os.path.exists(path):
            return
        size = os.path.getsize(path)
        if size <= 0:
            return
        new_len = rng.randrange(0, size + 1)
        with open(path, "r+b") as f:
            f.truncate(new_len)
            f.flush()
