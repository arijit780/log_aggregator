#include "log_storage/recovery/recovery_manager.hpp"

#include "log_storage/format/record_decode_status.hpp"
#include "log_storage/format/v1_binary_codec.hpp"

#include <unistd.h>

#include <stdexcept>

namespace log_storage {

/// Delegate to codec-specific recovery using the default V1 codec.
RecoveryManager::Result RecoveryManager::recover(int fd) {
  return recover(fd, default_v1_binary_codec());
}

/// Scan from start of file: decode records in order until EOF or first invalid frame.
///
/// Recovery is the authority for what is considered "durable state" after a crash.
///
/// Flow:
/// - Seek to byte 0.
/// - Repeatedly ask the codec to decode the next record at the expected logical offset.
/// - Stop at the first CleanEof (normal) or Invalid (torn tail / garbage / misalignment).
/// - Truncate the file to the last known-good record boundary and seek to end.
///
/// Invariant we enforce: after recovery, the file is a strict prefix of valid records with contiguous
/// logical offsets (0,1,2,...) — no skipping, no resync after damage.
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
