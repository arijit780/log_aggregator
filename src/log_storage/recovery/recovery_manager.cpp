#include "log_storage/recovery/recovery_manager.hpp"

#include "log_storage/format/record_decode_status.hpp"
#include "log_storage/format/v1_binary_codec.hpp"

#include <unistd.h>

#include <stdexcept>

namespace log_storage {

RecoveryManager::Result RecoveryManager::recover(int fd) {
  return recover(fd, default_v1_binary_codec());
}

RecoveryManager::Result RecoveryManager::recover(int fd, IRecordCodec const& codec) {
  if (::lseek(fd, 0, SEEK_SET) < 0) {
    throw std::runtime_error("RecoveryManager: lseek(SEEK_SET) failed");
  }

  std::uint64_t expected_offset = 0;
  std::uint64_t last_valid_position = 0;

  for (;;) {
    std::uint64_t end_pos = 0;
    const RecordDecodeStatus st =
        codec.try_decode_one_record(fd, expected_offset, end_pos, nullptr);
    if (st == RecordDecodeStatus::CleanEof) {
      break;
    }
    if (st == RecordDecodeStatus::Invalid) {
      break;
    }
    last_valid_position = end_pos;
    ++expected_offset;
  }

  if (::ftruncate(fd, static_cast<off_t>(last_valid_position)) < 0) {
    throw std::runtime_error("RecoveryManager: ftruncate failed");
  }
  if (::lseek(fd, 0, SEEK_END) < 0) {
    throw std::runtime_error("RecoveryManager: lseek(SEEK_END) failed");
  }

  return {expected_offset, last_valid_position};
}

}  // namespace log_storage
