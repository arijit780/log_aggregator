#include "log_storage/storage/recovery.hpp"

#include "log_storage/storage/record.hpp"
#include "log_storage/storage/segment.hpp"

#include <stdexcept>
#include <vector>

namespace log_storage {

/// Full-segment linear scan: decode until failure, then truncate file at last good byte.
RecoveryResult recover_segment(Segment& seg) {
  // Strategy: read the full segment into memory, then decode records in a
  // linear scan. This is intentionally simple — no streaming, no lookahead,
  // no attempt to skip past corruption.
  //
  // The scan stops at the FIRST record that fails validation. Everything
  // after that byte position is considered unreliable and is truncated.

  std::vector<std::uint8_t> data(static_cast<std::size_t>(seg.size));

  if (seg.size > 0) {
    const std::size_t got = segment_read_at(seg, 0, data.data(), data.size());
    if (got != data.size()) {
      throw std::runtime_error("recover_segment: short read (expected " +
                               std::to_string(data.size()) + ", got " +
                               std::to_string(got) + ")");
    }
  }

  std::uint64_t pos = 0;
  std::uint64_t record_count = 0;

  while (pos < seg.size) {
    DecodedRecord decoded;
    const DecodeResult result =
        decode_record(data.data() + pos,
                      static_cast<std::size_t>(seg.size - pos), decoded);

    if (result == DecodeResult::Ok) {
      pos += decoded.bytes_consumed;
      ++record_count;
      continue;
    }

    // Truncated or Corrupt: everything from `pos` onward is unreliable.
    //
    // Truncated: partial write at EOF (e.g. crash mid-record).
    // Corrupt:   CRC mismatch or invalid length (bit rot, garbage tail).
    //
    // In both cases, the correct action is to truncate at `pos`.
    break;
  }

  const bool needs_truncation = (pos < seg.size);

  if (needs_truncation) {
    // Remove the invalid tail. After this, the segment is a strict prefix
    // of valid records.
    //
    // CRASH NOTE: if we crash during ftruncate, the filesystem guarantees
    // atomic size update. On next startup, recovery re-runs and either
    // finds the file already truncated or truncates again. Idempotent.
    segment_truncate(seg, pos);
  }

  return RecoveryResult{pos, record_count, needs_truncation};
}

}  // namespace log_storage
