#pragma once

#include "log_storage/format/irecord_codec.hpp"

#include <cstdint>

namespace log_storage {

class RecoveryManager {
 public:
  struct Result {
    std::uint64_t next_offset;
    std::uint64_t truncated_bytes;
  };

  static Result recover(int fd);
  static Result recover(int fd, IRecordCodec const& codec);
};

}  // namespace log_storage
