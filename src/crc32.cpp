#include "log_storage/crc32.hpp"

#include <array>

namespace log_storage {

namespace {

constexpr std::uint32_t kPoly = 0xEDB88320u;

std::array<std::uint32_t, 256> make_table() {
  std::array<std::uint32_t, 256> t{};
  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t c = i;
    for (int k = 0; k < 8; ++k) {
      c = (c & 1u) ? (kPoly ^ (c >> 1)) : (c >> 1);
    }
    t[i] = c;
  }
  return t;
}

const std::array<std::uint32_t, 256> kTable = make_table();

}  // namespace

std::uint32_t crc32_compute(const void* data, std::size_t len) {
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::uint32_t c = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < len; ++i) {
    c = kTable[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
  }
  return c ^ 0xFFFFFFFFu;
}

}  // namespace log_storage
