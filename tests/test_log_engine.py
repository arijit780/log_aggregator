import os
import struct
import unittest
from tempfile import TemporaryDirectory

from src import Log, RecordEncoder


FILENAME_BASE_WIDTH = 20


def seg_path(data_dir: str, base_offset: int) -> str:
    filename = f"log_{base_offset:0{FILENAME_BASE_WIDTH}d}.seg"
    return os.path.join(data_dir, filename)


class TestSegmentedWALLog(unittest.TestCase):
    def test_basic_append(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            log = Log(data_dir=data_dir, max_segment_size_bytes=1024 * 1024)
            self.addCleanup(log.close)

            expected_payloads = [f"payload-{i}".encode("ascii") for i in range(100)]
            offsets = [log.append(p) for p in expected_payloads]
            self.assertEqual(offsets, list(range(100)))
            self.assertEqual(log.next_offset, 100)

            self.assertEqual(log.read_all(), expected_payloads)

            # Restart should rebuild state deterministically.
            log2 = Log(data_dir=data_dir, max_segment_size_bytes=1024 * 1024)
            self.addCleanup(log2.close)
            self.assertEqual(log2.next_offset, 100)
            self.assertEqual(log2.read_all(), expected_payloads)

    def test_segment_roll(self) -> None:
        # Use a small segment size to force multiple segment rolls.
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            max_seg = 1024
            log = Log(data_dir=data_dir, max_segment_size_bytes=max_seg)
            self.addCleanup(log.close)

            payloads = [struct.pack(">I", i) + b"X" * 20 for i in range(200)]
            offsets = [log.append(p) for p in payloads]
            self.assertEqual(offsets, list(range(200)))
            self.assertEqual(log.next_offset, 200)

            segment_files = sorted([f for f in os.listdir(data_dir) if f.endswith(".seg")])
            self.assertGreater(len(segment_files), 1, "Expected multiple segments to be created")

            # Verify each segment's base_offset matches the first record index in that segment.
            # Payload embeds the record index in its first 4 bytes.
            for fname in segment_files:
                base_offset_str = fname[len("log_") : -len(".seg")]
                base_offset = int(base_offset_str)
                sp = os.path.join(data_dir, fname)

                # Read the first record from the segment file.
                with open(sp, "rb") as f:
                    length_bytes = f.read(4)
                    self.assertEqual(len(length_bytes), 4)
                    length = struct.unpack(">I", length_bytes)[0]
                    crc_bytes = f.read(4)
                    self.assertEqual(len(crc_bytes), 4)
                    stored_crc = struct.unpack(">I", crc_bytes)[0]
                    payload = f.read(length)
                    self.assertEqual(len(payload), length)
                    computed = RecordEncoder.crc32_payload(payload)
                    self.assertEqual(computed, stored_crc)

                first_record_index = struct.unpack(">I", payload[:4])[0]
                self.assertEqual(
                    first_record_index,
                    base_offset,
                    "Segment base offset must match the first record's logical offset",
                )

            # Global order must still be intact.
            self.assertEqual(log.read_all(), payloads)
            log.close()

    def test_crash_simulation_partial_write(self) -> None:
        # Simulate different partial write crash points:
        # 1) after writing length (4 bytes)
        # 2) after writing length+crc (8 bytes)
        # 3) mid-payload
        cases = [
            ("after_length", 4),
            ("after_length_plus_crc", 8),
            ("mid_payload", 8 + 5),
        ]
        N = 5
        for case_name, partial_len in cases:
            with self.subTest(case=case_name):
                with TemporaryDirectory() as td:
                    data_dir = os.path.join(td, "data")
                    log = Log(data_dir=data_dir, max_segment_size_bytes=1024 * 1024)
                    self.addCleanup(log.close)

                    good_payloads = [f"good-{i}".encode("ascii") for i in range(N)]
                    for p in good_payloads:
                        log.append(p)

                    seg_file = seg_path(data_dir, 0)
                    self.assertTrue(os.path.exists(seg_file))

                    # Write a partial new record at the end of the active segment.
                    partial_payload = b"PARTIAL_PAYLOAD_12345"  # length doesn't matter as long as it's valid
                    full_record = RecordEncoder.encode_full(partial_payload)
                    self.assertLess(partial_len, len(full_record))
                    partial_bytes = full_record[:partial_len]

                    with open(seg_file, "r+b") as f:
                        f.seek(0, os.SEEK_END)
                        f.write(partial_bytes)
                        f.flush()

                    # Restart and verify tail is truncated, previous records intact.
                    log2 = Log(data_dir=data_dir, max_segment_size_bytes=1024 * 1024)
                    self.addCleanup(log2.close)
                    self.assertEqual(log2.next_offset, N)
                    self.assertEqual(log2.read_all(), good_payloads)

                    expected_size = sum(RecordEncoder.total_record_size(p) for p in good_payloads)
                    self.assertEqual(os.path.getsize(seg_file), expected_size)

    def test_crc_corruption_truncates_tail(self) -> None:
        with TemporaryDirectory() as td:
            data_dir = os.path.join(td, "data")
            log = Log(data_dir=data_dir, max_segment_size_bytes=1024 * 1024)
            self.addCleanup(log.close)

            # Fixed payload sizes makes byte-level corruption easy.
            payloads = [bytes([i]) * 10 for i in range(3)]  # each payload length = 10
            for p in payloads:
                log.append(p)

            seg_file = seg_path(data_dir, 0)
            record_size = 4 + 4 + 10  # length+crc+payload
            last_record_start = 2 * record_size
            # Corrupt within payload byte 3 of the last record payload.
            corrupt_pos = last_record_start + 4 + 4 + 3

            with open(seg_file, "r+b") as f:
                f.seek(corrupt_pos)
                b = f.read(1)
                self.assertEqual(len(b), 1)
                flipped = bytes([b[0] ^ 0xFF])
                f.seek(corrupt_pos)
                f.write(flipped)
                f.flush()

            # Restart should detect CRC mismatch and truncate the corrupted tail record.
            log2 = Log(data_dir=data_dir, max_segment_size_bytes=1024 * 1024)
            self.addCleanup(log2.close)
            self.assertEqual(log2.next_offset, 2)
            self.assertEqual(log2.read_all(), payloads[:2])

            expected_size_after = 2 * record_size
            self.assertEqual(os.path.getsize(seg_file), expected_size_after)


if __name__ == "__main__":
    unittest.main(verbosity=2)

