#include "log_storage/io/byte_io.hpp"

#include <unistd.h>

namespace log_storage {

// Thin wrappers around POSIX read/write that:
// - handle short reads/writes by looping, and
// - expose a simple boolean "all bytes transferred" contract to higher layers.
//
// The record codec + recovery logic relies on these helpers to distinguish:
// - clean EOF (no bytes) vs
// - partial/torn record tails (short transfer), which are treated as Invalid and truncated away.
bool read_exact(int fd, void* buf, std::size_t n) {
  auto* p = static_cast<std::uint8_t*>(buf);
  std::size_t got = 0;
  while (got < n) {
    const ssize_t r = ::read(fd, p + got, n - got);
    if (r < 0) {
      return false;
    }
    if (r == 0) {
      return false;
    }
    got += static_cast<std::size_t>(r);
  }
  return true;
}

bool write_all(int fd, const void* buf, std::size_t n) {
  const auto* p = static_cast<const std::uint8_t*>(buf);
  std::size_t sent = 0;
  while (sent < n) {
    const ssize_t w = ::write(fd, p + sent, n - sent);
    if (w < 0) {
      return false;
    }
    if (w == 0) {
      return false;
    }
    sent += static_cast<std::size_t>(w);
  }
  return true;
}

void pack_le_u32(std::uint8_t* out, std::uint32_t v) {
  out[0] = static_cast<std::uint8_t>(v & 0xFFu);
  out[1] = static_cast<std::uint8_t>((v >> 8) & 0xFFu);
  out[2] = static_cast<std::uint8_t>((v >> 16) & 0xFFu);
  out[3] = static_cast<std::uint8_t>((v >> 24) & 0xFFu);
}

// Pack/unpack helpers make the on-disk format explicitly little-endian.
// This avoids depending on host endianness or alignment when reading raw bytes.
void pack_le_u64(std::uint8_t* out, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out[i] = static_cast<std::uint8_t>((v >> (8 * i)) & 0xFFu);
  }
}

bool unpack_le_u32(const std::uint8_t* in, std::uint32_t* v) {
  *v = static_cast<std::uint32_t>(in[0]) | (static_cast<std::uint32_t>(in[1]) << 8) |
       (static_cast<std::uint32_t>(in[2]) << 16) |
       (static_cast<std::uint32_t>(in[3]) << 24);
  return true;
}

bool unpack_le_u64(const std::uint8_t* in, std::uint64_t* v) {
  *v = 0;
  for (int i = 0; i < 8; ++i) {
    *v |= static_cast<std::uint64_t>(in[i]) << (8 * i);
  }
  return true;
}

}  // namespace log_storage
