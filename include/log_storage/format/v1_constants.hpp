#pragma once

/*
 * V1 on-disk layout: [MAGIC][OFFSET][LENGTH][PAYLOAD][CRC] — rationale in docs/EXTENSION.md
 * and legacy comments archived in git history (pre-modular record_format.hpp).
 */

#include <cstddef>
#include <cstdint>

namespace log_storage {

inline constexpr std::uint32_t kRecordMagic = 0xA15E10Du;

inline constexpr std::size_t kMagicBytes = sizeof(std::uint32_t);
inline constexpr std::size_t kOffsetBytes = sizeof(std::uint64_t);
inline constexpr std::size_t kLengthBytes = sizeof(std::uint64_t);
inline constexpr std::size_t kCrcBytes = sizeof(std::uint32_t);

inline constexpr std::size_t kFixedHeaderBytes = kMagicBytes + kOffsetBytes + kLengthBytes;
inline constexpr std::size_t kRecordOverhead = kFixedHeaderBytes + kCrcBytes;

inline constexpr std::uint64_t kMaxPayloadBytes = 16 * 1024 * 1024;
inline constexpr std::uint64_t kMaxRecordBytes = kRecordOverhead + kMaxPayloadBytes;

}  // namespace log_storage
