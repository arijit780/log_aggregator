#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace log_storage {

// Sequential reader: same validation as recovery, without truncation.
class LogReader {
 public:
  explicit LogReader(std::string path);
  LogReader(const LogReader&) = delete;
  LogReader& operator=(const LogReader&) = delete;
  LogReader(LogReader&& other) noexcept;
  LogReader& operator=(LogReader&& other) noexcept;
  ~LogReader();

  // Returns false on CleanEof (no more records at a valid boundary).
  // Throws if structural integrity fails (Invalid) — no resync, no skip.
  bool read_next(std::vector<std::uint8_t>& payload_out);

  std::uint64_t expected_next_offset() const { return expected_offset_; }

 private:
  std::string path_;
  int fd_ = -1;
  std::uint64_t expected_offset_ = 0;
};

}  // namespace log_storage
