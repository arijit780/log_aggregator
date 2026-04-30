#pragma once

// Recovery — the single function that establishes on-disk truth after a crash.
//
// Invariant enforced: after recover_segment() returns, the segment file contains
// ONLY complete, CRC-valid records. Any partial or corrupted tail is gone.
//
// This function is called once per segment at startup. In a multi-segment log,
// only the last segment can realistically have a torn tail (earlier segments were
// finalized before rolling), but we validate the last segment unconditionally.

#include "log_storage/storage/segment.hpp"

#include <cstdint>

namespace log_storage {

/// Summary of what `recover_segment` did: valid prefix length, record count, whether tail was cut.
struct RecoveryResult {
  std::uint64_t valid_bytes;   ///< Byte length of the valid prefix after recovery (equals new file size).
  std::uint64_t record_count;  ///< Number of complete records fully validated in that prefix.
  bool truncated;              ///< True if bytes were removed from the end (partial/corrupt tail).
};

/// Scan a segment from the start, validate each record (length bounds + CRC32).
///
/// Stops at the first record that is `Truncated` or `Corrupt`. Everything from that
/// byte position onward is removed with `ftruncate` so the file is a strict prefix
/// of valid frames only.
///
/// **Crash during recovery:** If we crash before `ftruncate`, next boot repeats recovery.
/// Idempotent. After success, the segment matches `valid_bytes` and contains only whole records.
///
/// \param seg Segment to read and possibly truncate in place (`seg.size` updated).
RecoveryResult recover_segment(Segment& seg);

}  // namespace log_storage
