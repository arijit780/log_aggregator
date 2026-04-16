"""
Binary record layout — MUST match C++ log_storage (record_format.hpp / record_layout.cpp).
[MAGIC u32 LE][OFFSET u64 LE][LENGTH u64 LE][PAYLOAD][CRC u32 LE]
CRC32 over: pack(offset) || pack(length) || payload (IEEE, same table as C++).
"""

from __future__ import annotations

import struct
from typing import Final

RECORD_MAGIC: Final[int] = 0x0A15E10D
MAGIC_BYTES = 4
OFFSET_BYTES = 8
LENGTH_BYTES = 8
CRC_BYTES = 4
FIXED_HEADER_BYTES = MAGIC_BYTES + OFFSET_BYTES + LENGTH_BYTES
RECORD_OVERHEAD = FIXED_HEADER_BYTES + CRC_BYTES
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024

_CRC_TABLE: list[int] = []


def _init_crc_table() -> list[int]:
    poly = 0xEDB88320
    t = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (poly ^ (c >> 1)) if (c & 1) else (c >> 1)
        t.append(c & 0xFFFFFFFF)
    return t


_CRC_TABLE = _init_crc_table()


def crc32_compute(data: bytes | bytearray | memoryview) -> int:
    c = 0xFFFFFFFF
    for b in data:
        c = _CRC_TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
    return (c ^ 0xFFFFFFFF) & 0xFFFFFFFF


def encode_record(offset: int, payload: bytes) -> bytes:
    if offset < 0 or len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError("invalid offset or payload size")
    ln = len(payload)
    header = struct.pack("<IQQ", RECORD_MAGIC, offset, ln)
    crc_body = struct.pack("<QQ", offset, ln) + payload
    crc = crc32_compute(crc_body)
    tail = struct.pack("<I", crc)
    return header + payload + tail


def read_exact(f, n: int) -> bytes | None:
    """Return n bytes or None if EOF before n."""
    chunks: list[bytes] = []
    got = 0
    while got < n:
        chunk = f.read(n - got)
        if not chunk:
            return None
        chunks.append(chunk)
        got += len(chunk)
    return b"".join(chunks)


class DecodeStatus:
    CLEAN_EOF = 0
    INVALID = 1
    OK = 2


def try_decode_one_record(f, expected_offset: int) -> tuple[int, bytes | None, int]:
    """
    Returns (status, payload_or_none, end_file_position).
    end_file_position is file offset after this record on OK; on INVALID, caller uses last good.
    """
    pos_start = f.tell()
    magic_b = f.read(MAGIC_BYTES)
    if len(magic_b) == 0:
        return DecodeStatus.CLEAN_EOF, None, pos_start
    if len(magic_b) != MAGIC_BYTES:
        return DecodeStatus.INVALID, None, pos_start  # torn header / garbage tail

    (magic,) = struct.unpack("<I", magic_b)
    if magic != RECORD_MAGIC:
        return DecodeStatus.INVALID, None, pos_start

    off_b = read_exact(f, OFFSET_BYTES)
    len_b = read_exact(f, LENGTH_BYTES)
    if off_b is None or len_b is None:
        return DecodeStatus.INVALID, None, pos_start
    rec_off = struct.unpack("<Q", off_b)[0]
    length = struct.unpack("<Q", len_b)[0]
    if rec_off != expected_offset:
        return DecodeStatus.INVALID, None, pos_start
    if length > MAX_PAYLOAD_BYTES:
        return DecodeStatus.INVALID, None, pos_start

    if length == 0:
        payload = b""
    else:
        pl = read_exact(f, length)
        if pl is None:
            return DecodeStatus.INVALID, None, pos_start
        payload = pl

    cr = read_exact(f, CRC_BYTES)
    if cr is None:
        return DecodeStatus.INVALID, None, pos_start
    (crc_file,) = struct.unpack("<I", cr)
    crc_body = off_b + len_b + payload
    if crc32_compute(crc_body) != crc_file:
        return DecodeStatus.INVALID, None, pos_start

    end_pos = f.tell()
    return DecodeStatus.OK, payload, end_pos
