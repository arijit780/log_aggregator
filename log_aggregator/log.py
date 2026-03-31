import os
import re
import threading
from typing import Optional

from .record_encoder import RecordEncoder
from .durability import DurabilityManager, DurabilityMode
from .segment import Segment, SegmentInfo


SEGMENT_FILENAME_RE = re.compile(r"^log_(\d+)\.seg$")


class Log:
    """
    Segment-based append-only log with crash recovery.

    Public API:
      - append(data: bytes) -> offset
    """

    def __init__(
        self,
        *,
        data_dir: str = "./data",
        max_segment_size_bytes: int = 10 * 1024 * 1024,
        filename_base_width: int = 20,
        durability_mode: DurabilityMode = DurabilityMode.SYNC,
        fsync_interval_ms: int = 10,
        fsync_batch_records: int = 128,
        encoder: RecordEncoder = RecordEncoder(),
    ) -> None:
        if max_segment_size_bytes <= 0:
            raise ValueError("max_segment_size_bytes must be positive")

        self.data_dir = data_dir
        self.max_segment_size_bytes = max_segment_size_bytes
        self.filename_base_width = filename_base_width
        self.encoder = encoder

        os.makedirs(self.data_dir, exist_ok=True)

        self._next_offset = 0  # logical record index, NOT byte offset
        self._active_segment: Optional[Segment] = None
        # Serialize appends to prevent concurrent interleaving writes which would
        # violate strict record framing and recovery invariants.
        self._append_lock = threading.Lock()
        self._durability = DurabilityManager(
            mode=durability_mode,
            fsync_interval_ms=fsync_interval_ms,
            fsync_batch_records=fsync_batch_records,
        )

        self._startup_recover()

    def _segment_path(self, base_offset: int) -> str:
        filename = f"log_{base_offset:0{self.filename_base_width}d}.seg"
        return os.path.join(self.data_dir, filename)

    def _list_segment_infos(self) -> list[SegmentInfo]:
        entries = os.listdir(self.data_dir)
        infos: list[SegmentInfo] = []
        for name in entries:
            m = SEGMENT_FILENAME_RE.match(name)
            if not m:
                continue
            base_offset = int(m.group(1))
            infos.append(
                SegmentInfo(
                    base_offset=base_offset,
                    path=os.path.join(self.data_dir, name),
                )
            )
        infos.sort(key=lambda x: x.base_offset)
        return infos

    def _startup_recover(self) -> None:
        segment_infos = self._list_segment_infos()
        if not segment_infos:
            # Fresh log: create initial active segment at offset 0.
            self._next_offset = 0
            self._active_segment = Segment(
                base_offset=0,
                path=self._segment_path(0),
                max_size_bytes=self.max_segment_size_bytes,
                encoder=self.encoder,
            )
            return

        expected_base = 0
        next_offset = 0
        last_segment: Optional[Segment] = None

        # Strictly scan segments in filename/base_offset order.
        for info in segment_infos:
            if info.base_offset != expected_base:
                raise RuntimeError(
                    "Non-contiguous segments: "
                    f"expected base_offset={expected_base}, found {info.base_offset}"
                )

            seg = Segment(
                base_offset=info.base_offset,
                path=info.path,
                max_size_bytes=self.max_segment_size_bytes,
                encoder=self.encoder,
            )
            valid_count = seg.recover()

            next_offset = expected_base + valid_count
            expected_base = next_offset
            last_segment = seg

        self._next_offset = next_offset
        self._active_segment = last_segment

    @property
    def next_offset(self) -> int:
        return self._next_offset

    @property
    def fsync_batches_completed(self) -> int:
        """
        Observability for tests: number of fsync batches completed by the durability manager.
        """
        return self._durability.fsync_batches_completed

    def append(self, data: bytes) -> int:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("append(data) expects bytes-like")
        payload = bytes(data)

        with self._append_lock:
            if self._active_segment is None:
                # Should not happen due to startup recovery.
                self._active_segment = Segment(
                    base_offset=self._next_offset,
                    path=self._segment_path(self._next_offset),
                    max_size_bytes=self.max_segment_size_bytes,
                    encoder=self.encoder,
                )

            offset = self._next_offset
            appended = self._active_segment.append(payload)
            if not appended:
                # Roll to a new segment whose base offset is the next logical record index.
                self._active_segment.close()
                self._active_segment = Segment(
                    base_offset=offset,
                    path=self._segment_path(offset),
                    max_size_bytes=self.max_segment_size_bytes,
                    encoder=self.encoder,
                )
                appended = self._active_segment.append(payload)
                if not appended:
                    # Should only happen if the record is larger than the segment limit.
                    raise RuntimeError("Failed to append after rolling")

            # STRICT append flow:
            #   SYNC  => write+flush -> fsync(batch) -> ACK
            #   ASYNC => write+flush -> ACK -> fsync(batch later)
            self._durability.on_append(path=self._active_segment.path)

            self._next_offset += 1
            return offset

    def read_all(self) -> list[bytes]:
        """
        For testing/verification: read all valid records in order.
        """
        infos = self._list_segment_infos()
        payloads: list[bytes] = []
        for info in infos:
            seg = Segment(
                base_offset=info.base_offset,
                path=info.path,
                max_size_bytes=self.max_segment_size_bytes,
                encoder=self.encoder,
            )
            for p in seg.iter_payloads():
                payloads.append(p)
        return payloads

    def close(self) -> None:
        if getattr(self, "_durability", None) is not None:
            self._durability.close()
        if self._active_segment is not None:
            self._active_segment.close()
            self._active_segment = None

    def __enter__(self) -> "Log":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

