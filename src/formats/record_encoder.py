import struct
import zlib


class RecordEncoder:
    """
    Record format (MANDATORY):
      | length (4 bytes, uint32) |
      | crc32  (4 bytes)         |
      | payload (N bytes)       |

    - length is payload length only
    - CRC32 computed on payload only
    - big-endian encoding
    """

    _LEN_STRUCT = struct.Struct(">I")  # uint32, big-endian
    _CRC_STRUCT = struct.Struct(">I")  # uint32, big-endian

    @staticmethod
    def crc32_payload(payload: bytes) -> int:
        # zlib.crc32 returns an int; normalize to unsigned 32-bit.
        return zlib.crc32(payload) & 0xFFFFFFFF

    @classmethod
    def encode_full(cls, payload: bytes) -> bytes:
        length = len(payload)
        crc = cls.crc32_payload(payload)
        return cls._LEN_STRUCT.pack(length) + cls._CRC_STRUCT.pack(crc) + payload

    @classmethod
    def encode_header(cls, payload: bytes) -> bytes:
        length = len(payload)
        crc = cls.crc32_payload(payload)
        return cls._LEN_STRUCT.pack(length) + cls._CRC_STRUCT.pack(crc)

    @classmethod
    def total_record_size(cls, payload: bytes) -> int:
        return 4 + 4 + len(payload)
