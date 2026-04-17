#pragma once

#include "log_storage/format/irecord_codec.hpp"
#include "log_storage/format/v1_constants.hpp"

namespace log_storage {

class V1BinaryCodec final : public IRecordCodec {
 public:
  void encode(std::uint64_t offset, const void* payload, std::size_t payload_len,
              std::vector<std::uint8_t>& out) const override;

  RecordDecodeStatus try_decode_one_record(int fd, std::uint64_t expected_offset,
                                           std::uint64_t& end_file_offset,
                                           std::vector<std::uint8_t>* payload_out) const override;

  const char* id() const noexcept override { return "v1-binary"; }

  std::uint64_t max_payload_bytes() const noexcept override { return kMaxPayloadBytes; }
};

/** Process-wide default codec (V1). Safe to pass by pointer/reference to writers/readers. */
IRecordCodec const& default_v1_binary_codec() noexcept;

}  // namespace log_storage
