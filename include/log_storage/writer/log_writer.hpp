#pragma once

#include "log_storage/format/irecord_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace log_storage {

class LogWriter {
 public:
  explicit LogWriter(std::string path, IRecordCodec const* optional_codec = nullptr);
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;
  LogWriter(LogWriter&& other) noexcept;
  LogWriter& operator=(LogWriter&& other) noexcept;
  ~LogWriter();

  std::uint64_t append(const void* payload, std::size_t len);
  std::uint64_t append(const std::vector<std::uint8_t>& payload);

  std::uint64_t next_offset() const { return next_offset_; }

 private:
  IRecordCodec const& codec() const;

  std::string path_;
  int fd_ = -1;
  IRecordCodec const* codec_ptr_ = nullptr;
  std::uint64_t next_offset_ = 0;
};

}  // namespace log_storage
