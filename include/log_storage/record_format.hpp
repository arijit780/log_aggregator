#pragma once

/*
 * Record on-disk layout (mandatory):
 *
 *   [MAGIC][OFFSET][LENGTH][PAYLOAD][CRC]
 *
 * Why each field exists:
 *
 * - MAGIC: Without MAGIC, random file corruption or reading from a misaligned
 *   byte boundary can make arbitrary bytes look like a plausible header and
 *   LENGTH+payload+CRC tuple. A CRC computed only on (OFFSET, LENGTH, PAYLOAD)
 *   does not tell you *where* a record starts: if you decode from the wrong
 *   offset, you may still read a self-consistent LENGTH and PAYLOAD slice whose
 *   CRC matches that wrong interpretation. MAGIC gives an explicit boundary
 *   marker so the reader declares "a record starts here" only when the first
 *   four bytes match the constant.
 *
 * - OFFSET: Monotonic logical id (0,1,2,...). Recovery expects OFFSET to
 *   equal expected_offset. If someone appends a CRC-valid forged record with
 *   wrong OFFSET, recovery stops: global ordering is part of integrity, not
 *   just payload honesty. This also detects skipped regions without seeking
 *   past damage (forbidden): we never "resync forward" — first mismatch ends
 *   the valid prefix.
 *
 * - LENGTH: Self-delimiting payload size; bounded to detect absurd allocations
 *   and truncated files.
 *
 * - CRC: Detects bit/byte corruption in (OFFSET || LENGTH || PAYLOAD) as
 *   serialized on disk. A record with valid MAGIC and correct OFFSET sequence
 *   but flipped payload bytes fails here.
 *
 * Why CRC alone is insufficient (alignment problem):
 *   CRC is verified *after* you have already chosen a starting byte, parsed
 *   OFFSET and LENGTH, and sliced PAYLOAD. If you started reading one byte
 *   late, you might still parse a coherent OFFSET/LENGTH/PAYLOAD triple from
 *   interior bytes of a previous record; the CRC could match that *wrong*
 *   triple. MAGIC anchors the start; OFFSET enforces global structure; CRC
 *   protects payload+header fields once alignment and sequence are established.
 *
 * Extension awareness (not implemented here):
 *   OFFSET is the natural durable message id within a partition. Later,
 *   consumer offsets, compaction, and replication can refer to this logical
 *   position; partitions shard the OFFSET space per topic-partition.
 *
 * Final reasoning check (commentary only):
 *   If record_10 fully persists and record_11 is only partially written:
 *   - Recovery validates record_10 completely (MAGIC, offset==10, full payload,
 *     CRC) and advances last_valid_position past it — record_10 is preserved.
 *   - At record_11, either MAGIC fails (header/payload torn), OFFSET!=11,
 *     LENGTH implies bytes past EOF (partial payload), or CRC mismatches.
 *     We STOP at the first failed check; we never expose a truncated 11.
 *   - MAGIC ensures we are reading a true record start; OFFSET==expected ties
 *     the chain; CRC confirms the bits for that claimed (offset,length,payload).
 */

#include <cstddef>
#include <cstdint>

namespace log_storage {

// Distinct constant for boundary detection (not ASCII text to avoid common payloads.
inline constexpr std::uint32_t kRecordMagic = 0xA15E10Du;  // visual: "A^SE\x0D"

inline constexpr std::size_t kMagicBytes = sizeof(std::uint32_t);  // 4
inline constexpr std::size_t kOffsetBytes = sizeof(std::uint64_t);  // 8
inline constexpr std::size_t kLengthBytes = sizeof(std::uint64_t);  // 8
inline constexpr std::size_t kCrcBytes = sizeof(std::uint32_t);     // 4

inline constexpr std::size_t kFixedHeaderBytes =
    kMagicBytes + kOffsetBytes + kLengthBytes;
inline constexpr std::size_t kRecordOverhead =
    kFixedHeaderBytes + kCrcBytes;

// Hard cap so LENGTH cannot force huge allocations (structural bound).
inline constexpr std::uint64_t kMaxPayloadBytes = 16 * 1024 * 1024;

inline constexpr std::uint64_t kMaxRecordBytes = kRecordOverhead + kMaxPayloadBytes;

}  // namespace log_storage
