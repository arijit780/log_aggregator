import os
import re
import struct
from dataclasses import dataclass


_CONSUMER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


@dataclass(frozen=True)
class ConsumerOffset:
    consumer_id: str
    committed_offset: int  # -1 means "nothing committed yet"


class OffsetStore:
    """
    Server-managed committed offsets, persisted durably.

    Design choice: **separate file per consumer** with atomic overwrite.
    - Simple mapping: consumer_id -> committed_offset
    - Durable update: write tmp file -> fsync(tmp) -> os.replace -> fsync(dir)
    - Survives crashes; no in-memory-only state.

    File format (big-endian):
      | committed_offset (8 bytes, int64) |
    """

    _STRUCT = struct.Struct(">q")  # int64 big-endian

    def __init__(self, *, dir_path: str) -> None:
        self.dir_path = dir_path
        os.makedirs(self.dir_path, exist_ok=True)

    def _validate_consumer_id(self, consumer_id: str) -> None:
        if not isinstance(consumer_id, str) or not _CONSUMER_ID_RE.match(consumer_id):
            raise ValueError("consumer_id must match [A-Za-z0-9_.-]{1,128}")

    def _path_for(self, consumer_id: str) -> str:
        self._validate_consumer_id(consumer_id)
        return os.path.join(self.dir_path, f"{consumer_id}.offset")

    def load_committed(self, consumer_id: str) -> int:
        """
        Returns last committed offset, or -1 if consumer has no stored offset yet.
        """
        path = self._path_for(consumer_id)
        if not os.path.exists(path):
            return -1
        with open(path, "rb") as f:
            data = f.read(self._STRUCT.size)
            if len(data) != self._STRUCT.size:
                # Treat invalid offset file as "no commit". This keeps the WAL valid and
                # results in re-delivery (at-least-once) rather than data loss.
                return -1
            (off,) = self._STRUCT.unpack(data)
            return int(off)

    def store_committed(self, consumer_id: str, committed_offset: int) -> None:
        """
        Durably stores committed_offset for consumer_id.

        Guarantees:
          - Once this call returns, the committed offset survives process crash + OS crash.
        """
        if committed_offset < -1:
            raise ValueError("committed_offset must be >= -1")

        path = self._path_for(consumer_id)
        tmp = f"{path}.tmp"

        payload = self._STRUCT.pack(int(committed_offset))

        # Write tmp file and fsync it.
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        # Atomic replace.
        os.replace(tmp, path)

        # Ensure directory entry is durable.
        dir_fd = os.open(self.dir_path, os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
