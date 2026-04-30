#pragma once

#include "log_storage/format/irecord_codec.hpp"

#include <cstdint>

namespace log_storage {

/// Recovery for the **week 1–2 single-file** log: scan from byte 0 using `IRecordCodec`.
///
/// Stops at first invalid or incomplete record; truncates the file to the last complete record end.
class RecoveryManager {
 public:
  /// Result of recovery: next logical offset to assign, and byte position after truncation.
  struct Result {
    std::uint64_t next_offset;      ///< Count of valid records = next logical id to write.
    std::uint64_t truncated_bytes;  ///< File size after `ftruncate` (last valid byte offset).
  };

  /// Recover using the default V1 binary codec.
  static Result recover(int fd);

  /// Recover using an explicit codec (same scan rules).
  static Result recover(int fd, IRecordCodec const& codec);
};

}  // namespace log_storage
