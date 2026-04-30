#include "log_storage/crypto/crc32.hpp"

#include <array>

namespace log_storage {

namespace {

// CRC-32 (IEEE, reflected) using a 256-entry table.
//
// Why a table?
// - The bit-by-bit version is simpler, but materially slower; in this codebase CRC runs on every record
//   and the slowdown can change batching behavior in durability tests (more wakeups / fsyncs).
// - The table keeps the implementation small while preserving the standard polynomial + init/final XOR.
constexpr std::uint32_t kCrc32PolyReversed = 0xEDB88320u;

std::array<std::uint32_t, 256> make_crc32_table() {
  std::array<std::uint32_t, 256> table{};

  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t crc = i;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc & 1u) ? (kCrc32PolyReversed ^ (crc >> 1)) : (crc >> 1);
    }
    table[i] = crc;
  }

  return table;
}

const std::array<std::uint32_t, 256> kCrc32Table = make_crc32_table();

}  // namespace

std::uint32_t crc32_compute(const void* data, std::size_t len) {
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::uint32_t crc = 0xFFFFFFFFu;

  for (std::size_t i = 0; i < len; ++i) {
    crc = kCrc32Table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
  }

  return crc ^ 0xFFFFFFFFu;
}

}  // namespace log_storage
