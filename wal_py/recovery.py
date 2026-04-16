"""Scan from offset 0, validate records, truncate to last good byte (same contract as C++ RecoveryManager)."""

from __future__ import annotations

import os

from .record_format import DecodeStatus, try_decode_one_record


def scan_prefix_payloads(path: str) -> list[bytes]:
    """Read valid record payloads in order without mutating the file (for tests / inspection)."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    out: list[bytes] = []
    with open(path, "rb") as f:
        expected = 0
        while True:
            st, pl, _end = try_decode_one_record(f, expected)
            if st == DecodeStatus.CLEAN_EOF:
                break
            if st == DecodeStatus.INVALID:
                break
            assert pl is not None
            out.append(pl)
            expected += 1
    return out


def recover(path: str) -> tuple[int, int]:
    """
    Returns (next_offset, truncated_size_bytes).
    File is truncated in place to the end of the last valid record.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return 0, 0

    with open(path, "r+b") as f:
        f.seek(0, os.SEEK_SET)
        expected = 0
        last_valid = 0
        while True:
            st, _payload, end_pos = try_decode_one_record(f, expected)
            if st == DecodeStatus.CLEAN_EOF:
                break
            if st == DecodeStatus.INVALID:
                break
            last_valid = end_pos
            expected += 1

        f.truncate(last_valid)
        f.flush()
        os.fsync(f.fileno())

    return expected, last_valid
