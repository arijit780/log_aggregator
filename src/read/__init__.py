"""Read-only access to WAL segments (consumer / query path)."""

from .log_reader import LogReader, Record

__all__ = ["LogReader", "Record"]
