#include "log_storage/writer/durable_log_writer.hpp"

#include "log_storage/format/irecord_codec.hpp"
#include "log_storage/recovery/recovery_manager.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <utility>

namespace log_storage {

DurableLogWriter::DurableLogWriter(std::string path, DurabilityManager::Config config)
    : path_(std::move(path)) {
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("DurableLogWriter: open failed");
  }
  IRecordCodec const* c = config.record_codec;
  const RecoveryManager::Result rr =
      c != nullptr ? RecoveryManager::recover(fd_, *c) : RecoveryManager::recover(fd_);
  dm_ = std::make_unique<DurabilityManager>(fd_, rr.next_offset, std::move(config));
}

DurableLogWriter::DurableLogWriter(DurableLogWriter&& other) noexcept {
  path_ = std::move(other.path_);
  fd_ = other.fd_;
  dm_ = std::move(other.dm_);
  other.fd_ = -1;
}

DurableLogWriter& DurableLogWriter::operator=(DurableLogWriter&& other) noexcept {
  if (this != &other) {
    if (dm_) {
      dm_->shutdown();
    }
    dm_.reset();
    if (fd_ >= 0) {
      ::fsync(fd_);
      ::close(fd_);
    }
    path_ = std::move(other.path_);
    fd_ = other.fd_;
    dm_ = std::move(other.dm_);
    other.fd_ = -1;
  }
  return *this;
}

DurableLogWriter::~DurableLogWriter() {
  if (dm_) {
    dm_->shutdown();
  }
  dm_.reset();
  if (fd_ >= 0) {
    (void)::fsync(fd_);
    ::close(fd_);
  }
}

std::uint64_t DurableLogWriter::append(const std::vector<std::uint8_t>& payload) {
  return append(payload.data(), payload.size());
}

std::uint64_t DurableLogWriter::append(const void* payload, std::size_t len) {
  return dm_->append(payload, len);
}

std::uint64_t DurableLogWriter::next_offset() const { return dm_->next_offset(); }

std::uint64_t DurableLogWriter::fsync_count() const { return dm_->fsync_count(); }

}  // namespace log_storage
