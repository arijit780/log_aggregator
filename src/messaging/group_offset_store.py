"""
Durable offsets keyed by (group_id, topic, partition).

Same atomic replace + fsync pattern as Week 3 OffsetStore; no in-memory-only progress.
"""

import os
import re
import struct


_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_TOPIC_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class GroupOffsetStore:
    _STRUCT = struct.Struct(">q")

    def __init__(self, *, dir_path: str) -> None:
        self.dir_path = dir_path
        os.makedirs(self.dir_path, exist_ok=True)

    def _path(self, group_id: str, topic: str, partition: int) -> str:
        self._validate_ids(group_id, topic, partition)
        base = os.path.join(self.dir_path, group_id, topic)
        return os.path.join(base, f"p{partition}.offset")

    @staticmethod
    def _validate_ids(group_id: str, topic: str, partition: int) -> None:
        if not _ID_RE.match(group_id):
            raise ValueError("invalid group_id")
        if not _TOPIC_RE.match(topic):
            raise ValueError("invalid topic")
        if partition < 0:
            raise ValueError("invalid partition")

    def load(self, group_id: str, topic: str, partition: int) -> int:
        path = self._path(group_id, topic, partition)
        if not os.path.exists(path):
            return -1
        with open(path, "rb") as f:
            data = f.read(self._STRUCT.size)
            if len(data) != self._STRUCT.size:
                return -1
            (off,) = self._STRUCT.unpack(data)
            return int(off)

    def store(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        if offset < -1:
            raise ValueError("offset must be >= -1")
        path = self._path(group_id, topic, partition)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        payload = self._STRUCT.pack(int(offset))
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        dfd = os.open(os.path.dirname(path), os.O_DIRECTORY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
