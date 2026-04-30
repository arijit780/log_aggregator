#pragma once

// Segment — a single file in the append-only log.
//
// Each segment stores a contiguous range of bytes. The log is composed of an
// ordered sequence of segments. Only the LAST segment is open for writes.
//
// Crash safety contract:
//   segment_append() → data lands in kernel page cache.
//   segment_fsync()  → data reaches stable storage.
//   Between append and fsync, a crash loses the write. Recovery truncates.

#include <cstddef>
#include <cstdint>
#include <string>

namespace log_storage {

/// One on-disk segment file: path, fd, its global byte range start, and valid length.
struct Segment {
  std::string path;           ///< Filesystem path (e.g. `.../00000000.seg`).
  int fd = -1;                ///< Open file descriptor, or -1 after close.
  std::uint64_t base_offset = 0;  ///< Global byte offset of this file's first byte in the log.
  std::uint64_t size = 0;         ///< Number of valid bytes in this segment (file logical size).
};

/// Open an existing segment file read-write and load its size from the filesystem.
///
/// \param path Absolute or relative path to the segment file.
/// \param base_offset Global offset of byte 0 of this file in the logical log.
/// \return Filled `Segment` with `fd` open and `size == file length`.
/// \throws std::runtime_error if open or stat fails.
Segment segment_open(const std::string& path, std::uint64_t base_offset);

/// Create a new segment file (truncate if it exists) for appending.
///
/// \param path Path for the new segment file.
/// \param base_offset Global offset for the first byte of this segment.
/// \return `Segment` with empty file (`size == 0`).
/// \throws std::runtime_error if create fails.
Segment segment_create(const std::string& path, std::uint64_t base_offset);

/// Append raw bytes at the end of the segment's valid data (`size`).
///
/// Seeks to `size`, writes `len` bytes, then advances `size`. Returns the global
/// byte offset where the first appended byte lives (`base_offset + old_size`).
///
/// **Crash:** After return, bytes may be only in page cache until `fsync`. If the
/// process dies before durability policy runs, recovery may truncate this tail.
///
/// \throws std::runtime_error on `lseek`/`write` failure; `size` is not advanced on write failure.
std::uint64_t segment_append(Segment& seg, const void* data, std::size_t len);

/// Read up to `max_bytes` from this segment at `local_offset` (offset within this file).
///
/// Uses `pread` so the file offset used for appending is unchanged — safe under a mutex
/// with concurrent append if externally serialized.
///
/// \return Number of bytes read (0 if `local_offset >= size`, or at EOF).
/// \throws std::runtime_error if `pread` fails.
std::size_t segment_read_at(const Segment& seg, std::uint64_t local_offset,
                            void* buf, std::size_t max_bytes);

/// Flush the segment file descriptor to stable storage (`fsync`).
///
/// \throws std::runtime_error if `fsync` fails.
void segment_fsync(const Segment& seg);

/// Set the segment's file size to `local_offset` bytes (drop tail from that byte onward).
///
/// Used after scanning finds corruption: everything from `local_offset` is discarded.
/// Updates `seg.size` to match. **Crash:** filesystem metadata update is atomic.
///
/// \throws std::runtime_error if `ftruncate` fails.
void segment_truncate(Segment& seg, std::uint64_t local_offset);

/// Close the segment fd if open; sets `fd` to -1. Safe to call multiple times.
void segment_close(Segment& seg);

}  // namespace log_storage
