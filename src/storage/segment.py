import os
import struct
from dataclasses import dataclass
from typing import Callable, Generator, Optional

from ..formats import RecordEncoder


@dataclass(frozen=True)
class SegmentInfo:
    base_offset: int
    path: str


class Segment:
    def __init__(
        self,
        *,
        base_offset: int,
        path: str,
        max_size_bytes: int,
        encoder: Optional[RecordEncoder] = None,
        append_stage_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        if base_offset < 0:
            raise ValueError("base_offset must be non-negative")
        if max_size_bytes <= 0:
            raise ValueError("max_size_bytes must be positive")

        self.base_offset = base_offset
        self.path = path
        self.max_size_bytes = max_size_bytes
        self.encoder = encoder if encoder is not None else RecordEncoder()
        self._append_stage_hook = append_stage_hook

        self._f = None  # file handle (BinaryIO)

    def _hook(self, stage: str) -> None:
        if self._append_stage_hook is not None:
            self._append_stage_hook(stage)

    def _open_for_append(self) -> None:
        if self._f is not None:
            return

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            self._f = open(self.path, "w+b")
        else:
            self._f = open(self.path, "r+b")

        # Ensure we append at the current end.
        self._f.seek(0, os.SEEK_END)

    def close(self) -> None:
        if self._f is not None:
            self._f.close()
            self._f = None

    def flush(self) -> None:
        """
        Flush Python's user-space buffers to the OS (no durability guarantee).

        This is used to ensure that an ASYNC-mode ACK reflects that the bytes were at least
        handed to the OS (page cache), while leaving durability to a later fsync.
        """
        if self._f is None:
            return
        self._f.flush()

    def fileno(self) -> int:
        self._open_for_append()
        assert self._f is not None
        return self._f.fileno()

    def fsync(self) -> None:
        """
        Ensure bytes are durable on stable storage.

        Note: correctness does not rely on fsync always succeeding; recovery truncation
        preserves log validity. But SYNC-mode ACK requires fsync completion.
        """
        if self._f is None:
            return
        self._f.flush()
        os.fsync(self._f.fileno())

    def append(self, payload: bytes) -> bool:
        """
        Append a single record.

        Returns:
          - True if appended
          - False if remaining space is insufficient (caller must roll)

        Crash-safety:
          - We write length+crc before payload.
          - Any partial record at the tail will be truncated during recovery.
        """
        record_size = 4 + 4 + len(payload)
        if record_size > self.max_size_bytes:
            raise ValueError(
                f"Record size {record_size} exceeds max segment size {self.max_size_bytes}"
            )

        self._open_for_append()
        assert self._f is not None

        self._f.seek(0, os.SEEK_END)
        current_size = self._f.tell()
        if current_size + record_size > self.max_size_bytes:
            return False

        # Staged write for failure injection (Week 4). Order: length → crc → payload.
        self._hook("before_write")

        length = len(payload)
        crc = self.encoder.crc32_payload(payload)

        self._f.write(struct.pack(">I", length))
        self._f.flush()
        self._hook("after_length")

        self._f.write(struct.pack(">I", crc))
        self._f.flush()
        self._hook("after_crc")

        if len(payload) > 1:
            split = len(payload) // 2
            self._f.write(payload[:split])
            self._f.flush()
            self._hook("mid_payload")
            self._f.write(payload[split:])
        elif payload:
            self._f.write(payload)
            self._f.flush()
            self._hook("mid_payload")
        else:
            # Empty payload still defines mid_payload as "between crc and after_write".
            self._hook("mid_payload")

        # Always flush user-space buffers so ASYNC ACK means bytes reached the OS
        # (not necessarily durable until fsync).
        self._f.flush()
        self._hook("after_write")
        return True

    def recover(self) -> int:
        """
        Strict crash recovery scan for this segment.

        Sequentially reads:
          length -> crc -> payload

        Validate:
          - full payload exists
          - CRC matches payload

        If ANY check fails:
          - truncate the file at last valid position
          - stop scanning this segment

        Returns:
          number of valid records remaining after recovery.
        """
        if not os.path.exists(self.path):
            return 0

        valid_records = 0
        with open(self.path, "r+b") as f:
            f.seek(0, os.SEEK_END)
            file_end = f.tell()
            f.seek(0, os.SEEK_SET)

            last_valid_pos = 0
            while True:
                record_start_pos = f.tell()
                if record_start_pos > file_end:
                    break

                length_bytes = f.read(4)
                if length_bytes == b"":
                    # Clean EOF.
                    break
                if len(length_bytes) < 4:
                    # Partial header (crash after length).
                    f.truncate(last_valid_pos)
                    f.flush()
                    os.fsync(f.fileno())
                    break

                length = struct.unpack(">I", length_bytes)[0]

                crc_bytes = f.read(4)
                if len(crc_bytes) < 4:
                    # Partial CRC.
                    f.truncate(last_valid_pos)
                    f.flush()
                    os.fsync(f.fileno())
                    break
                stored_crc = struct.unpack(">I", crc_bytes)[0]

                # Segment model check: record must fit in this segment's configured max size.
                record_end_pos = record_start_pos + 8 + length
                if record_end_pos > self.max_size_bytes:
                    f.truncate(last_valid_pos)
                    f.flush()
                    os.fsync(f.fileno())
                    break

                # Additional sanity: prevent nonsense lengths from exhausting scanning.
                if length > self.max_size_bytes - 8:
                    f.truncate(last_valid_pos)
                    f.flush()
                    os.fsync(f.fileno())
                    break

                payload = f.read(length)
                if len(payload) < length:
                    # Mid-payload partial write.
                    f.truncate(last_valid_pos)
                    f.flush()
                    os.fsync(f.fileno())
                    break

                computed_crc = self.encoder.crc32_payload(payload)
                if computed_crc != stored_crc:
                    # CRC mismatch: treat record as invalid and truncate from that point.
                    f.truncate(last_valid_pos)
                    f.flush()
                    os.fsync(f.fileno())
                    break

                last_valid_pos = f.tell()
                valid_records += 1

            # Ensure truncation is persisted.
            try:
                f.truncate(last_valid_pos)
                f.flush()
                os.fsync(f.fileno())
            except OSError:
                pass

        return valid_records

    def iter_payloads(self, *, limit: Optional[int] = None) -> Generator[bytes, None, None]:
        """
        Iterate valid records in file order.

        Intended for verification/testing. Recovery truncation should already remove
        corrupted tails; this iterator will stop if a record cannot be parsed.
        """
        if not os.path.exists(self.path):
            return

        with open(self.path, "rb") as f:
            remaining = limit
            while True:
                if remaining is not None and remaining <= 0:
                    return

                length_bytes = f.read(4)
                if length_bytes == b"":
                    return
                if len(length_bytes) < 4:
                    return

                length = struct.unpack(">I", length_bytes)[0]
                crc_bytes = f.read(4)
                if len(crc_bytes) < 4:
                    return
                stored_crc = struct.unpack(">I", crc_bytes)[0]

                payload = f.read(length)
                if len(payload) < length:
                    return

                if self.encoder.crc32_payload(payload) != stored_crc:
                    return

                yield payload
                if remaining is not None:
                    remaining -= 1
