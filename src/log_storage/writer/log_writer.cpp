#include "log_storage/writer/log_writer.hpp"

#include "log_storage/format/v1_binary_codec.hpp"
#include "log_storage/io/byte_io.hpp"
#include "log_storage/recovery/recovery_manager.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <utility>

namespace log_storage {

IRecordCodec const& LogWriter::codec() const {
  return codec_ptr_ != nullptr ? *codec_ptr_ : default_v1_binary_codec();
}

LogWriter::LogWriter(std::string path, IRecordCodec const* optional_codec)
    : path_(std::move(path)), codec_ptr_(optional_codec) {
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("LogWriter: open failed");
  }
  const RecoveryManager::Result rr = RecoveryManager::recover(fd_, codec());
  next_offset_ = rr.next_offset;
}

LogWriter::LogWriter(LogWriter&& other) noexcept {
  path_ = std::move(other.path_);
  fd_ = other.fd_;
  codec_ptr_ = other.codec_ptr_;
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
    codec_ptr_ = other.codec_ptr_;
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
  if (len > codec().max_payload_bytes()) {
    throw std::runtime_error("LogWriter: payload too large for codec");
  }
  const std::uint64_t assigned = next_offset_;
  std::vector<std::uint8_t> record_bytes;
  codec().encode(assigned, payload, len, record_bytes);
  if (!write_all(fd_, record_bytes.data(), record_bytes.size())) {
    throw std::runtime_error("LogWriter: write failed");
  }
  ++next_offset_;
  return assigned;
}

}  // namespace log_storage
