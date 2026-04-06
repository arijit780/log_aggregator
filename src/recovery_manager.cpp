#include "log_storage/recovery_manager.hpp"

#include "log_storage/record_layout.hpp"

#include <unistd.h>

#include <stdexcept>

namespace log_storage {

RecoveryManager::Result RecoveryManager::recover(int fd) {
  /*
   * Core recovery algorithm (authoritative):
   * expected_offset starts at 0; last_valid_position marks end of last good byte.
   * For each record: MAGIC must match; OFFSET must equal expected_offset; LENGTH
   * bounded; PAYLOAD full; CRC present and matching. Any failure: STOP (never skip).
   * Then truncate at last_valid_position so the file is exactly a strict prefix.
   */
  if (::lseek(fd, 0, SEEK_SET) < 0) {
    throw std::runtime_error("RecoveryManager: lseek(SEEK_SET) failed");
  }

  std::uint64_t expected_offset = 0;
  std::uint64_t last_valid_position = 0;

  for (;;) {
    std::uint64_t end_pos = 0;
    const RecordDecodeStatus st =
        try_decode_one_record(fd, expected_offset, end_pos, nullptr);
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
