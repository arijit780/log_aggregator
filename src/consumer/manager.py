from dataclasses import dataclass

from ..read import LogReader, Record
from ..storage import Log
from .offset_store import OffsetStore


@dataclass(frozen=True)
class ConsumerState:
    consumer_id: str
    committed_offset: int


class ConsumerManager:
    """
    Enforces the consumer lifecycle:
      read -> process (external) -> commit

    CORE RULE:
      - reading does NOT advance progress
      - only commit advances progress, and commit is durable

    Delivery guarantee:
      - At-least-once delivery.
        If the consumer commits correctly, a record is delivered >= 1 times, never 0.

    Failure semantics:
      - Crash BEFORE commit: record may be re-delivered (duplicate processing possible).
      - Commit BEFORE processing (consumer bug): record may be LOST (not re-delivered).
      - Crash AFTER commit: record is NOT re-delivered.
      """

    def __init__(
        self,
        *,
        log: Log,
        offsets_dir: str,
    ) -> None:
        self.log = log
        self.offset_store = OffsetStore(dir_path=offsets_dir)
        self.reader = LogReader(
            data_dir=log.data_dir,
            max_segment_size_bytes=log.max_segment_size_bytes,
            encoder=log.encoder,
        )

    def committed_offset(self, consumer_id: str) -> int:
        """
        Load durable committed offset, then clamp to log end if log was truncated
        by recovery (e.g. corruption). No ghost progress beyond readable records.

        Justification for clamp (not reset-to-start): preserves monotonic semantic;
        consumer may re-read from (clamped+1) without skipping records that still exist.
        """
        stored = self.offset_store.load_committed(consumer_id)
        latest = self.log.latest_record_offset()
        if stored > latest:
            self.offset_store.store_committed(consumer_id, latest)
            return latest
        return stored

    def next_offset(self, consumer_id: str) -> int:
        return self.committed_offset(consumer_id) + 1

    def read(self, consumer_id: str, offset: int, max_bytes: int) -> list[Record]:
        # Reading never changes committed offsets.
        self.reader.refresh()
        return self.reader.read(offset=offset, max_bytes=max_bytes)

    def read_from_committed(self, consumer_id: str, max_bytes: int) -> list[Record]:
        return self.read(consumer_id, self.next_offset(consumer_id), max_bytes)

    def commit(self, consumer_id: str, offset: int) -> None:
        """
        Marks all records <= offset as processed for this consumer.

        Monotonic commit enforcement:
          - commit offsets must be non-decreasing; committing an older offset is rejected.
        """
        if offset < -1:
            raise ValueError("offset must be >= -1")

        latest = self.log.latest_record_offset()
        if offset > latest:
            raise ValueError(
                f"cannot commit beyond last log record: offset={offset}, latest={latest}"
            )

        # Apply same clamp as committed_offset() so monotonic checks vs durable state
        # stay consistent after log truncation.
        current = self.committed_offset(consumer_id)
        if offset < current:
            raise ValueError(
                f"Out-of-order commit rejected: current_committed={current}, attempted={offset}"
            )

        # Durable commit (survives crashes).
        self.offset_store.store_committed(consumer_id, offset)
