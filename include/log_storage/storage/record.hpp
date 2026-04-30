#pragma once

// Week 3–4 on-disk record format
//
// Wire layout (all little-endian):
//
//   ┌──────────────┬─────────────────┬──────────────┐
//   │ uint32 length│ payload[length] │ uint32 crc32 │
//   └──────────────┴─────────────────┴──────────────┘
//
// CRC32 covers the concatenation of the raw length bytes and the payload bytes.
// This means both the length value AND the payload content are integrity-protected.
//
// Crash note: a partially written record will either have a truncated length field
// (decode returns Truncated), a truncated payload/CRC (Truncated), or a CRC mismatch
// (Corrupt). Recovery truncates at the last fully valid record boundary.

#include <cstddef>
#include <cstdint>
#include <vector>

namespace log_storage {

/// User-visible record: only the application payload (no length/CRC on this struct).
struct Record {
  std::vector<std::uint8_t> payload;
};

// -------------------------------------------------------------------
// Wire-format constants
// -------------------------------------------------------------------

/// Maximum allowed payload size per record (bounds corrupted length fields).
inline constexpr std::uint32_t kMaxRecordPayload = 16u * 1024u * 1024u;  // 16 MiB
/// Size of the little-endian length prefix on disk.
inline constexpr std::size_t kLengthFieldBytes = sizeof(std::uint32_t);   // 4
/// Size of the little-endian CRC suffix on disk.
inline constexpr std::size_t kChecksumBytes = sizeof(std::uint32_t);      // 4
/// Total framing overhead: length + CRC (payload follows length).
inline constexpr std::size_t kFrameOverhead = kLengthFieldBytes + kChecksumBytes;  // 8

/// Total bytes one record occupies on disk for a given payload length.
///
/// \param payload_len Number of user bytes in the payload (before framing).
/// \return `kFrameOverhead + payload_len`.
constexpr std::size_t record_wire_size(std::size_t payload_len) {
  return kFrameOverhead + payload_len;
}

// -------------------------------------------------------------------
// Encode / Decode
// -------------------------------------------------------------------

/// Serialize `payload` into the segment wire format: [length][payload][crc32].
///
/// Clears `out` and writes the full frame. CRC is computed over the raw length
/// bytes plus payload bytes (so tampering with length or payload is detected).
///
/// \param payload Pointer to user bytes (may be null if \p len is 0).
/// \param len Payload length; must not exceed `kMaxRecordPayload`.
/// \param out Output buffer; resized to `record_wire_size(len)`.
/// \return Number of bytes written to `out` (same as `record_wire_size(len)`).
/// \throws std::runtime_error if `len` exceeds `kMaxRecordPayload`.
std::size_t encode_record(const void* payload, std::size_t len,
                          std::vector<std::uint8_t>& out);

/// Result of attempting to parse one record from a byte buffer.
enum class DecodeResult {
  Ok,        ///< Record is structurally valid (CRC matches, length in bounds).
  Truncated, ///< Fewer bytes than needed — partial write, EOF, or need more input.
  Corrupt,   ///< CRC mismatch or absurd length — damaged data; do not skip forward.
};

/// Output of a successful decode: the logical record plus how much of the buffer it used.
struct DecodedRecord {
  Record record;               ///< Decoded application payload.
  std::size_t bytes_consumed;  ///< On-disk bytes read for this record (frame size).
};

/// Parse exactly one record starting at `buf[0]`.
///
/// Does not read from disk — only interprets the provided buffer. Used by recovery
/// and readers after loading bytes with `read`/`pread`.
///
/// \param buf Start of available bytes.
/// \param buflen Number of bytes available starting at `buf`.
/// \param out On `Ok`, receives `record` and `bytes_consumed`; unchanged otherwise.
/// \return `Ok` if one complete valid record was parsed; `Truncated` if not enough
///         bytes for a full frame; `Corrupt` if length is out of range or CRC fails.
DecodeResult decode_record(const std::uint8_t* buf, std::size_t buflen,
                           DecodedRecord& out);

}  // namespace log_storage
