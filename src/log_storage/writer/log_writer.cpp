#include "log_storage/writer/log_writer.hpp"

#include "log_storage/format/v1_binary_codec.hpp"
#include "log_storage/recovery/recovery_manager.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <utility>

namespace log_storage {

/// Codec from config override or static default V1 codec.
IRecordCodec const& LogWriter::codec() const {
  return durability_config_.record_codec != nullptr ? *durability_config_.record_codec
                                                    : default_v1_binary_codec();
}

/// Open RW, create if missing, run recovery to establish `next_offset_`.
///
/// Flow on open:
/// - open() the file read/write (create if needed)
/// - run RecoveryManager to ensure the file is a strict prefix of valid records
/// - remember next_offset (the next logical record id to assign)
LogWriter::LogWriter(std::string path, DurabilityManager::Config durability_config)
    : path_(std::move(path)), durability_config_(std::move(durability_config)) {
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("LogWriter: open failed");
  }
  const RecoveryManager::Result rr = RecoveryManager::recover(fd_, codec());
  next_offset_ = rr.next_offset;
}

/// Move-construct: transfer fd, offsets, and lazily-created durability manager.
LogWriter::LogWriter(LogWriter&& other) noexcept {
  path_ = std::move(other.path_);
  fd_ = other.fd_;
  next_offset_ = other.next_offset_;
  durability_config_ = std::move(other.durability_config_);
  dm_ = std::move(other.dm_);
  other.fd_ = -1;
  other.next_offset_ = 0;
}

/// Move-assign: destroy manager, close old fd, take state from `other`.
LogWriter& LogWriter::operator=(LogWriter&& other) noexcept {
  if (this != &other) {
    dm_.reset();
    if (fd_ >= 0) {
      ::close(fd_);
    }
    path_ = std::move(other.path_);
    fd_ = other.fd_;
    next_offset_ = other.next_offset_;
    durability_config_ = std::move(other.durability_config_);
    dm_ = std::move(other.dm_);
    other.fd_ = -1;
    other.next_offset_ = 0;
  }
  return *this;
}

/// Destroy durability manager first, then close fd.
LogWriter::~LogWriter() {
  dm_.reset();
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

/// Vector overload — default durability mode `None`.
std::uint64_t LogWriter::append(const std::vector<std::uint8_t>& payload) {
  return append(payload.data(), payload.size(), DurabilityMode::None);
}

/// Raw pointer overload — default durability mode `None`.
std::uint64_t LogWriter::append(const void* payload, std::size_t len) {
  return append(payload, len, DurabilityMode::None);
}

/// Vector overload with explicit durability mode.
std::uint64_t LogWriter::append(const std::vector<std::uint8_t>& payload, DurabilityMode mode) {
  return append(payload.data(), payload.size(), mode);
}

/// Encode with codec, write via `DurabilityManager` (lazy-created), update cached next offset.
///
/// Flow on append(payload, mode):
/// - encode record bytes with the chosen codec (default V1)
/// - write_all the full record
/// - depending on mode: return immediately (None/Async) or wait for a batched fsync (Sync)
std::uint64_t LogWriter::append(const void* payload, std::size_t len, DurabilityMode mode) {
  if (dm_ == nullptr) {
    dm_ = std::make_unique<DurabilityManager>(fd_, next_offset_, durability_config_);
  }
  const std::uint64_t assigned = dm_->append(payload, len, mode);
  next_offset_ = dm_->next_offset();
  return assigned;
}

/// Forward fsync counter from manager (0 if never appended).
std::uint64_t LogWriter::fsync_count() const { return dm_ ? dm_->fsync_count() : 0u; }

}  // namespace log_storage
