#include "log_storage/reader/log_reader.hpp"

#include "log_storage/format/record_decode_status.hpp"
#include "log_storage/format/v1_binary_codec.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <utility>

namespace log_storage {

/// Resolve optional codec pointer to default V1 implementation when null.
IRecordCodec const& LogReader::codec() const {
  return codec_ptr_ != nullptr ? *codec_ptr_ : default_v1_binary_codec();
}

/// Open read-only, seek to start, reset expected logical offset to 0.
LogReader::LogReader(std::string path, IRecordCodec const* optional_codec)
    : path_(std::move(path)), codec_ptr_(optional_codec) {
  fd_ = ::open(path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
    throw std::runtime_error("LogReader: open failed");
  }
  if (::lseek(fd_, 0, SEEK_SET) < 0) {
    throw std::runtime_error("LogReader: seek failed");
  }
  expected_offset_ = 0;
}

/// Move-construct: steal fd and offset state from `other`.
LogReader::LogReader(LogReader&& other) noexcept {
  path_ = std::move(other.path_);
  fd_ = other.fd_;
  codec_ptr_ = other.codec_ptr_;
  expected_offset_ = other.expected_offset_;
  other.fd_ = -1;
  other.expected_offset_ = 0;
}

/// Move-assign: close old fd, take ownership from `other`.
LogReader& LogReader::operator=(LogReader&& other) noexcept {
  if (this != &other) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    path_ = std::move(other.path_);
    fd_ = other.fd_;
    codec_ptr_ = other.codec_ptr_;
    expected_offset_ = other.expected_offset_;
    other.fd_ = -1;
    other.expected_offset_ = 0;
  }
  return *this;
}

/// Close fd if still open.
LogReader::~LogReader() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

/// Decode one record at `expected_offset_`; advance offset on success.
///
/// Reader is intentionally strict: it never truncates and it never tries to "skip past" corruption.
/// If it encounters an Invalid record, it throws so callers can decide whether to run recovery.
bool LogReader::read_next(std::vector<std::uint8_t>& payload_out) {
  // The codec validates the record boundary (MAGIC), chain (expected_offset_), and CRC.
  std::uint64_t end_pos = 0;
  const RecordDecodeStatus st =
      codec().try_decode_one_record(fd_, expected_offset_, end_pos, &payload_out);
  if (st == RecordDecodeStatus::CleanEof) {
    return false;
  }
  if (st == RecordDecodeStatus::Invalid) {
    throw std::runtime_error("LogReader: invalid record or truncated tail");
  }
  ++expected_offset_;
  return true;
}

}  // namespace log_storage
