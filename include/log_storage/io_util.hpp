#pragma once

#include <cstddef>
#include <cstdint>

namespace log_storage {

// Returns false on EOF before `n` bytes or read error.
bool read_exact(int fd, void* buf, std::size_t n);

// Returns false on short write or error.
bool write_all(int fd, const void* buf, std::size_t n);

// Pack/unpack little-endian unsigned 32/64-bit integers.
void pack_le_u32(std::uint8_t* out, std::uint32_t v);
void pack_le_u64(std::uint8_t* out, std::uint64_t v);
bool unpack_le_u32(const std::uint8_t* in, std::uint32_t* v);
bool unpack_le_u64(const std::uint8_t* in, std::uint64_t* v);

}  // namespace log_storage
