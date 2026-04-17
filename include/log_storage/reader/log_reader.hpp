#pragma once

#include "log_storage/format/irecord_codec.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace log_storage {

class LogReader {
 public:
  explicit LogReader(std::string path, IRecordCodec const* optional_codec = nullptr);
  LogReader(const LogReader&) = delete;
  LogReader& operator=(const LogReader&) = delete;
  LogReader(LogReader&& other) noexcept;
  LogReader& operator=(LogReader&& other) noexcept;
  ~LogReader();

  bool read_next(std::vector<std::uint8_t>& payload_out);

  std::uint64_t expected_next_offset() const { return expected_offset_; }

 private:
  IRecordCodec const& codec() const;

  std::string path_;
  int fd_ = -1;
  IRecordCodec const* codec_ptr_ = nullptr;
  std::uint64_t expected_offset_ = 0;
};

}  // namespace log_storage
