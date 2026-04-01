import os
import re
import struct
from dataclasses import dataclass
from typing import Optional

from ..formats import RecordEncoder


SEGMENT_FILENAME_RE = re.compile(r"^log_(\d+)\.seg$")


@dataclass(frozen=True)
class Record:
    offset: int
    payload: bytes


@dataclass(frozen=True)
class _SegmentMeta:
    base_offset: int
    path: str
    # record_start_positions[i] is the byte position in file where record (base_offset+i) begins
    record_start_positions: list[int]


class LogReader:
    """
    Reads records from existing WAL segment files with strict ordering.

    Responsibilities:
      - Map logical offset -> (segment, byte position)
      - Read full records without splitting, respecting max_bytes

    Notes:
      - Assumes segment recovery already truncated any corrupted tail.
      - Still validates CRC while reading, and stops if it encounters invalid framing.
    """

    def __init__(
        self,
        *,
        data_dir: str,
        max_segment_size_bytes: int,
        encoder: Optional[RecordEncoder] = None,
    ) -> None:
        self.data_dir = data_dir
        self.max_segment_size_bytes = max_segment_size_bytes
        self.encoder = encoder if encoder is not None else RecordEncoder()

        os.makedirs(self.data_dir, exist_ok=True)
        self._segments: list[_SegmentMeta] = []
        self.refresh()

    def refresh(self) -> None:
        """
        Re-scan all segments and rebuild the in-memory offset->position index.

        This index is server-managed ephemeral metadata (safe to rebuild on restart).
        """
        metas: list[_SegmentMeta] = []
        for name in os.listdir(self.data_dir):
            m = SEGMENT_FILENAME_RE.match(name)
            if not m:
                continue
            base = int(m.group(1))
            metas.append(
                _SegmentMeta(
                    base_offset=base,
                    path=os.path.join(self.data_dir, name),
                    record_start_positions=[],
                )
            )
        metas.sort(key=lambda x: x.base_offset)

        # Build per-segment record start positions.
        rebuilt: list[_SegmentMeta] = []
        for meta in metas:
            starts = self._scan_record_starts(meta.path)
            rebuilt.append(
                _SegmentMeta(
                    base_offset=meta.base_offset,
                    path=meta.path,
                    record_start_positions=starts,
                )
            )
        self._segments = rebuilt

    def latest_offset(self) -> int:
        if not self._segments:
            return -1
        last = self._segments[-1]
        return last.base_offset + len(last.record_start_positions) - 1

    def read(self, *, offset: int, max_bytes: int) -> list[Record]:
        """
        Returns sequential records starting at offset, bounded by max_bytes.

        - Does NOT modify any consumer state.
        - If offset is beyond end of log, returns [].
        - Does not split records: a record is returned whole or not returned at all.
          If the next record alone would exceed max_bytes, returns [].
        """
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")

        if offset > self.latest_offset():
            return []

        out: list[Record] = []
        remaining = max_bytes
        cur = offset

        while True:
            loc = self._locate(cur)
            if loc is None:
                self._assert_contiguous_offsets(out, start_offset=offset)
                return out
            meta, idx_in_seg = loc

            rec = self._read_one(meta=meta, idx_in_seg=idx_in_seg, offset=cur)
            if rec is None:
                self._assert_contiguous_offsets(out, start_offset=offset)
                return out

            size = len(rec.payload)
            if size > remaining:
                self._assert_contiguous_offsets(out, start_offset=offset)
                return out

            out.append(rec)
            remaining -= size
            cur += 1

            if remaining <= 0:
                self._assert_contiguous_offsets(out, start_offset=offset)
                return out

    def _assert_contiguous_offsets(self, records: list[Record], *, start_offset: int) -> None:
        for i, r in enumerate(records):
            assert r.offset == start_offset + i, "read must return strictly increasing contiguous offsets"

    def _locate(self, offset: int) -> Optional[tuple[_SegmentMeta, int]]:
        # Find the last segment whose base_offset <= offset.
        seg: Optional[_SegmentMeta] = None
        for meta in self._segments:
            if meta.base_offset <= offset:
                seg = meta
            else:
                break
        if seg is None:
            return None
        idx = offset - seg.base_offset
        if idx < 0 or idx >= len(seg.record_start_positions):
            return None
        return seg, idx

    def _scan_record_starts(self, path: str) -> list[int]:
        if not os.path.exists(path):
            return []
        starts: list[int] = []
        with open(path, "rb") as f:
            while True:
                start = f.tell()
                length_bytes = f.read(4)
                if length_bytes == b"":
                    break
                if len(length_bytes) < 4:
                    break
                length = struct.unpack(">I", length_bytes)[0]
                crc_bytes = f.read(4)
                if len(crc_bytes) < 4:
                    break

                # Skip payload bytes.
                if length > self.max_segment_size_bytes - 8:
                    break
                payload = f.read(length)
                if len(payload) < length:
                    break

                stored_crc = struct.unpack(">I", crc_bytes)[0]
                if self.encoder.crc32_payload(payload) != stored_crc:
                    break

                starts.append(start)
        return starts

    def _read_one(self, *, meta: _SegmentMeta, idx_in_seg: int, offset: int) -> Optional[Record]:
        start = meta.record_start_positions[idx_in_seg]
        with open(meta.path, "rb") as f:
            f.seek(start, os.SEEK_SET)
            length_bytes = f.read(4)
            if len(length_bytes) < 4:
                return None
            length = struct.unpack(">I", length_bytes)[0]
            crc_bytes = f.read(4)
            if len(crc_bytes) < 4:
                return None
            stored_crc = struct.unpack(">I", crc_bytes)[0]
            payload = f.read(length)
            if len(payload) < length:
                return None
            if self.encoder.crc32_payload(payload) != stored_crc:
                return None
            return Record(offset=offset, payload=payload)
