#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace log_storage {

// Optimistic append path: serializes full record and issues write(). Crash may
// leave a torn tail; RecoveryManager is the sole authority on what exists.
class LogWriter {
 public:
  explicit LogWriter(std::string path);
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;
  LogWriter(LogWriter&& other) noexcept;
  LogWriter& operator=(LogWriter&& other) noexcept;
  ~LogWriter();

  // Returns assigned monotonic offset; advances only after writeAll succeeds.
  std::uint64_t append(const void* payload, std::size_t len);
  std::uint64_t append(const std::vector<std::uint8_t>& payload);

  std::uint64_t next_offset() const { return next_offset_; }

 private:
  std::string path_;
  int fd_ = -1;
  std::uint64_t next_offset_ = 0;
};

}  // namespace log_storage
