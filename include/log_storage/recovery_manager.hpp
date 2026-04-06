#pragma once

#include <cstdint>

namespace log_storage {

// Authoritative reconstruction of durable prefix. The write path is optimistic:
// append() may return success while the record is still torn on disk until fsync
// policies elsewhere; only recovery decides what survives across crashes.
class RecoveryManager {
 public:
  struct Result {
    std::uint64_t next_offset;       // next logical id assigned by append()
    std::uint64_t truncated_bytes;   // resulting file size
  };

  // Seeks to 0, scans strictly forward, stops at first invalid byte sequence,
  // truncates garbage tail to the end of the last fully validated record.
  static Result recover(int fd);
};

}  // namespace log_storage
