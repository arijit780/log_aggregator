#include "log_storage/log_writer.hpp"

#include "log_storage/record_format.hpp"
#include "log_storage/record_layout.hpp"
#include "log_storage/recovery_manager.hpp"

#include "log_storage/io_util.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <utility>

namespace log_storage {

LogWriter::LogWriter(std::string path) : path_(std::move(path)) {
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("LogWriter: open failed");
  }
  const RecoveryManager::Result rr = RecoveryManager::recover(fd_);
  next_offset_ = rr.next_offset;
  /*
   * Critical insight: append() is optimistic. Even when write() returns success,
   * a crash before the page cache is durable can drop the record. On reopen,
   * recover() may truncate a suffix — the in-memory offset after a "successful"
   * append is not a durability guarantee.
   *
   * Why record_10 survives and partial record_11 is discarded (reasoning check):
   * recover() commits byte-by-byte validation up to and including record_10's
   * CRC, updating last_valid_position past that record. record_11 begins with
   * MAGIC at the next byte; a crash mid-write yields wrong MAGIC, wrong OFFSET,
   * short PAYLOAD, or bad CRC. The first check that fails stops the scan;
   * truncate(last_valid_after_10) removes the torn 11th record bytes entirely.
   */
}

LogWriter::LogWriter(LogWriter&& other) noexcept {
  path_ = std::move(other.path_);
  fd_ = other.fd_;
  next_offset_ = other.next_offset_;
  other.fd_ = -1;
  other.next_offset_ = 0;
}

LogWriter& LogWriter::operator=(LogWriter&& other) noexcept {
  if (this != &other) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    path_ = std::move(other.path_);
    fd_ = other.fd_;
    next_offset_ = other.next_offset_;
    other.fd_ = -1;
    other.next_offset_ = 0;
  }
  return *this;
}

LogWriter::~LogWriter() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

std::uint64_t LogWriter::append(const std::vector<std::uint8_t>& payload) {
  return append(payload.data(), payload.size());
}

std::uint64_t LogWriter::append(const void* payload, std::size_t len) {
  if (len > kMaxPayloadBytes) {
    throw std::runtime_error("LogWriter: payload exceeds kMaxPayloadBytes");
  }
  const std::uint64_t assigned = next_offset_;
  std::vector<std::uint8_t> record_bytes;
  encode_record(assigned, payload, len, record_bytes);
  if (!write_all(fd_, record_bytes.data(), record_bytes.size())) {
    throw std::runtime_error("LogWriter: write failed");
  }
  // Spec: update only after write returns; we still do not assert durability.
  ++next_offset_;
  return assigned;
}

}  // namespace log_storage
