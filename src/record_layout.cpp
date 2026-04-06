#include "log_storage/record_layout.hpp"

#include "log_storage/crc32.hpp"
#include "log_storage/io_util.hpp"

#include <unistd.h>

#include <cstring>

namespace log_storage {

void encode_record(std::uint64_t offset, const void* payload, std::size_t payload_len,
                   std::vector<std::uint8_t>& out) {
  const std::uint64_t len_u64 = static_cast<std::uint64_t>(payload_len);
  out.resize(kRecordOverhead + payload_len);
  std::uint8_t* base = out.data();
  pack_le_u32(base, kRecordMagic);
  pack_le_u64(base + kMagicBytes, offset);
  pack_le_u64(base + kMagicBytes + kOffsetBytes, len_u64);
  if (payload_len > 0) {
    std::memcpy(base + kFixedHeaderBytes, payload, payload_len);
  }
  std::vector<std::uint8_t> crc_body(kOffsetBytes + kLengthBytes + payload_len);
  pack_le_u64(crc_body.data(), offset);
  pack_le_u64(crc_body.data() + kOffsetBytes, len_u64);
  if (payload_len > 0) {
    std::memcpy(crc_body.data() + kOffsetBytes + kLengthBytes, payload, payload_len);
  }
  const std::uint32_t crc = crc32_compute(crc_body.data(), crc_body.size());
  pack_le_u32(base + kFixedHeaderBytes + payload_len, crc);
}

namespace {

ssize_t read_once(int fd, void* buf, std::size_t n) {
  return ::read(fd, buf, n);
}

}  // namespace

RecordDecodeStatus try_decode_one_record(int fd, std::uint64_t expected_offset,
                                         std::uint64_t& end_file_offset,
                                         std::vector<std::uint8_t>* payload_out) {
  /*
   * Failure scenarios (explicit):
   * 1) Crash during header write: MAGIC or OFFSET/LENGTH incomplete or wrong —
   *    first short read or MAGIC/OFFSET check fails; we stop; last_valid truncates.
   * 2) Crash during payload: LENGTH read ok but payload read short → Invalid.
   * 3) Crash during CRC: payload ok, CRC bytes missing → Invalid.
   * 4) Extra garbage at end: first garbage byte fails MAGIC or spills into
   *    invalid OFFSET/LENGTH/CRC path → Invalid at that boundary; truncate removes.
   * 5) Valid-looking misaligned data: we never hunt for MAGIC; if previous record
   *    ended cleanly, we read next MAGIC from aligned position. If garbage starts
   *    mid-stream without truncate, MAGIC mismatch or OFFSET != expected stops us.
   */

  std::uint8_t magic_buf[kMagicBytes];
  ssize_t r = read_once(fd, magic_buf, kMagicBytes);
  if (r == 0) {
    return RecordDecodeStatus::CleanEof;
  }
  if (r < 0 || static_cast<std::size_t>(r) != kMagicBytes) {
    return RecordDecodeStatus::Invalid;
  }

  std::uint32_t magic = 0;
  unpack_le_u32(magic_buf, &magic);
  if (magic != kRecordMagic) {
    return RecordDecodeStatus::Invalid;
  }

  std::uint8_t off_buf[kOffsetBytes];
  if (!read_exact(fd, off_buf, kOffsetBytes)) {
    return RecordDecodeStatus::Invalid;
  }
  std::uint64_t rec_off = 0;
  unpack_le_u64(off_buf, &rec_off);
  if (rec_off != expected_offset) {
    return RecordDecodeStatus::Invalid;
  }

  std::uint8_t len_buf[kLengthBytes];
  if (!read_exact(fd, len_buf, kLengthBytes)) {
    return RecordDecodeStatus::Invalid;
  }
  std::uint64_t length = 0;
  unpack_le_u64(len_buf, &length);
  if (length > kMaxPayloadBytes) {
    return RecordDecodeStatus::Invalid;
  }

  std::vector<std::uint8_t> payload(static_cast<std::size_t>(length));
  if (length > 0) {
    if (!read_exact(fd, payload.data(), static_cast<std::size_t>(length))) {
      return RecordDecodeStatus::Invalid;
    }
  }

  std::uint8_t crc_buf[kCrcBytes];
  if (!read_exact(fd, crc_buf, kCrcBytes)) {
    return RecordDecodeStatus::Invalid;
  }
  std::uint32_t crc_file = 0;
  unpack_le_u32(crc_buf, &crc_file);

  std::vector<std::uint8_t> crc_body;
  crc_body.resize(kOffsetBytes + kLengthBytes + static_cast<std::size_t>(length));
  std::memcpy(crc_body.data(), off_buf, kOffsetBytes);
  std::memcpy(crc_body.data() + kOffsetBytes, len_buf, kLengthBytes);
  if (length > 0) {
    std::memcpy(crc_body.data() + kOffsetBytes + kLengthBytes, payload.data(),
                static_cast<std::size_t>(length));
  }
  const std::uint32_t crc_calc = crc32_compute(crc_body.data(), crc_body.size());
  if (crc_calc != crc_file) {
    return RecordDecodeStatus::Invalid;
  }

  const off_t pos_after = ::lseek(fd, 0, SEEK_CUR);
  if (pos_after < 0) {
    return RecordDecodeStatus::Invalid;
  }
  end_file_offset = static_cast<std::uint64_t>(pos_after);

  if (payload_out != nullptr) {
    *payload_out = std::move(payload);
  }

  return RecordDecodeStatus::Ok;
}

}  // namespace log_storage
