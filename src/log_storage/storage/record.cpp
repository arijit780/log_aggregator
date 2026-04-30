#include "log_storage/storage/record.hpp"

#include "log_storage/crypto/crc32.hpp"
#include "log_storage/io/byte_io.hpp"

#include <cstring>
#include <stdexcept>

namespace log_storage {

/// Turn application bytes into one on-disk frame: LE length, payload copy, LE CRC32 over both.
std::size_t encode_record(const void* payload, std::size_t len,
                          std::vector<std::uint8_t>& out) {
  if (len > kMaxRecordPayload) {
    throw std::runtime_error("encode_record: payload exceeds kMaxRecordPayload");
  }

  const std::size_t total = record_wire_size(len);
  out.resize(total);
  std::uint8_t* p = out.data();

  // [uint32 length] — payload size, little-endian.
  pack_le_u32(p, static_cast<std::uint32_t>(len));

  // [payload]
  if (len > 0) {
    std::memcpy(p + kLengthFieldBytes, payload, len);
  }

  // [uint32 crc32] — CRC over the length field + payload bytes.
  // Covering the length field means a flipped bit in the length is detected
  // even if the payload happens to be valid at the wrong length.
  const std::uint32_t crc = crc32_compute(p, kLengthFieldBytes + len);
  pack_le_u32(p + kLengthFieldBytes + len, crc);

  return total;
}

/// Validate framing and CRC for one record in-memory; copy payload out on success.
DecodeResult decode_record(const std::uint8_t* buf, std::size_t buflen,
                           DecodedRecord& out) {
  // Step 1: need at least the length field.
  if (buflen < kLengthFieldBytes) {
    return DecodeResult::Truncated;
  }

  std::uint32_t payload_len = 0;
  unpack_le_u32(buf, &payload_len);

  // Step 2: reject obviously invalid lengths before trusting the value.
  // Without this check, a corrupted length could cause a huge allocation.
  if (payload_len > kMaxRecordPayload) {
    return DecodeResult::Corrupt;
  }

  // Step 3: do we have the full record (length + payload + crc)?
  const std::size_t total = record_wire_size(payload_len);
  if (buflen < total) {
    return DecodeResult::Truncated;
  }

  // Step 4: verify CRC over [length_bytes || payload_bytes].
  const std::uint32_t expected_crc =
      crc32_compute(buf, kLengthFieldBytes + payload_len);

  std::uint32_t stored_crc = 0;
  unpack_le_u32(buf + kLengthFieldBytes + payload_len, &stored_crc);

  if (expected_crc != stored_crc) {
    return DecodeResult::Corrupt;
  }

  // Record is valid. Extract payload.
  out.record.payload.assign(buf + kLengthFieldBytes,
                            buf + kLengthFieldBytes + payload_len);
  out.bytes_consumed = total;

  return DecodeResult::Ok;
}

}  // namespace log_storage
