#pragma once

#include "log_storage/format/record_decode_status.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace log_storage {

/*
 * Pluggable on-disk record encoding/decoding. Recovery and readers depend only on this
 * interface; add a new final class (e.g. V2RecordCodec) and pass it to LogWriter / LogReader /
 * RecoveryManager::recover(fd, codec).
 */
class IRecordCodec {
 public:
  virtual ~IRecordCodec() = default;

  virtual void encode(std::uint64_t offset, const void* payload, std::size_t payload_len,
                      std::vector<std::uint8_t>& out) const = 0;

  virtual RecordDecodeStatus try_decode_one_record(int fd, std::uint64_t expected_offset,
                                                   std::uint64_t& end_file_offset,
                                                   std::vector<std::uint8_t>* payload_out) const = 0;

  virtual const char* id() const noexcept = 0;

  virtual std::uint64_t max_payload_bytes() const noexcept = 0;
};

}  // namespace log_storage
