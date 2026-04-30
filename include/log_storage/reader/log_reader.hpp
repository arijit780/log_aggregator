#pragma once

#include "log_storage/format/irecord_codec.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace log_storage {

/// Sequential reader for the **week 1–2 single-file** log format (`IRecordCodec`).
///
/// Opens read-only, does **not** run recovery or truncate — invalid records throw.
/// Expected logical offsets advance by one per successful `read_next`.
class LogReader {
 public:
  /// Open `path` for reading; optional codec defaults to V1 binary codec.
  ///
  /// \throws std::runtime_error if open or seek fails.
  explicit LogReader(std::string path, IRecordCodec const* optional_codec = nullptr);
  LogReader(const LogReader&) = delete;
  LogReader& operator=(const LogReader&) = delete;
  LogReader(LogReader&& other) noexcept;
  LogReader& operator=(LogReader&& other) noexcept;
  ~LogReader();

  /// Read the next record’s payload in logical-order (`expected_offset_`, then increment).
  ///
  /// \return False at clean EOF before any bad bytes.
  /// \throws std::runtime_error if the next bytes are invalid / truncated (caller may run recovery separately).
  bool read_next(std::vector<std::uint8_t>& payload_out);

  /// Next logical record index the reader expects (monotonic after successful reads).
  std::uint64_t expected_next_offset() const { return expected_offset_; }

 private:
  /// Resolved codec: explicit pointer or process-wide default V1 codec.
  IRecordCodec const& codec() const;

  std::string path_;
  int fd_ = -1;
  IRecordCodec const* codec_ptr_ = nullptr;
  std::uint64_t expected_offset_ = 0;
};

}  // namespace log_storage
