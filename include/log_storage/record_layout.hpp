#pragma once

#include "log_storage/record_format.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace log_storage {

enum class RecordDecodeStatus {
  // No more full records; file ends exactly at a record boundary (valid prefix).
  CleanEof,
  // First invalid byte sequence: partial header, wrong MAGIC, OFFSET mismatch,
  // LENGTH out of bounds, partial payload, missing CRC, or CRC mismatch.
  // Recovery must STOP here; never skip forward.
  Invalid,
  Ok,
};

// Serialize one record into `out` (reallocated). CRC covers OFFSET||LENGTH||PAYLOAD
// as little-endian bytes for offset/length plus raw payload.
void encode_record(std::uint64_t offset, const void* payload, std::size_t payload_len,
                   std::vector<std::uint8_t>& out);

/*
 * Attempt to read and validate one record from `fd` at current offset.
 * On Ok: `expected_offset` is incremented by caller; `end_file_offset` is byte
 * position in file after this record; optional payload copy.
 * On CleanEof: file at valid end (no trash consumed).
 * On Invalid: file position is left at the first byte of the bad/tail region
 * (caller stops and truncates to last known good `end_file_offset`).
 */
RecordDecodeStatus try_decode_one_record(int fd, std::uint64_t expected_offset,
                                         std::uint64_t& end_file_offset,
                                         std::vector<std::uint8_t>* payload_out);

}  // namespace log_storage
