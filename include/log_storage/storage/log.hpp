#pragma once

// Log — segment-based append-only log with explicit durability.
//
// Public API:
//   append(payload, len) → global byte offset
//   read(offset, max_bytes) → vector<Record>
//
// On construction:
//   1. Scan the directory for existing segment files (sorted by name).
//   2. Recover the last segment (validate records, truncate corrupt tail).
//   3. Create the first segment if the directory is empty.
//   4. Start the durability guard (sync or async, per config).
//
// Segment rolling:
//   When the active segment would exceed max_segment_bytes, the log:
//   1. Shuts down the durability guard (which does a final fsync).
//   2. Creates a new segment file.
//   3. Starts a new durability guard for the new segment.
//
//   If crash happens between steps 1 and 2: old segment is fsynced and clean.
//   If crash happens during step 2: new file may be empty; recovery handles it.
//   If crash happens during a write to the new segment: recovery truncates.

#include "log_storage/storage/durability.hpp"
#include "log_storage/storage/record.hpp"
#include "log_storage/storage/segment.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace log_storage {

/// Tunables for `Log`: directory path, roll size, and durability mode for new segments.
struct LogConfig {
  std::string directory;      ///< Directory holding `NNNNNNNN.seg` files (created if missing).
  std::uint64_t max_segment_bytes = 64 * 1024 * 1024;  ///< Roll active segment before exceeding this size.
  DurabilityConfig durability;  ///< Applied when opening / rolling segments.
};

/// Segment-based append-only log: framed records, global byte offsets, crash recovery.
///
/// Thread-safe: `append` and `read` take an internal mutex. Open directory must be stable.
class Log {
 public:
  /// Open or create the log at `config.directory`, discover segments, recover last segment.
  ///
  /// \throws std::runtime_error if directory cannot be created or segment I/O fails.
  explicit Log(LogConfig config);

  /// Flush durability (final fsync in async), close all segment fds.
  ~Log();

  Log(const Log&) = delete;
  Log& operator=(const Log&) = delete;

  /// Append one record; returns the **global** byte offset where the encoded frame starts.
  ///
  /// Encodes `encode_record`, may roll to a new segment if size limit would be exceeded,
  /// writes bytes, then runs durability (`after_write`). Use this offset only as `read`'s start
  /// when you intend to read full records from a record boundary.
  ///
  /// **Sync:** returned offset is durable when the call returns.
  /// **Async:** returned offset may not be on disk yet — crash can lose it until fsync.
  std::uint64_t append(const void* payload, std::size_t len);
  /// Convenience overload: same as `append(payload.data(), payload.size())`.
  std::uint64_t append(const std::vector<std::uint8_t>& payload);

  /// Decode consecutive records starting at global `offset` until total decoded payload
  /// reaches at least `max_bytes` (always returns at least one record if data is valid).
  ///
  /// `offset` should be an offset previously returned by `append`. Mid-record offsets yield
  /// decode failures and typically an empty result.
  std::vector<Record> read(std::uint64_t offset, std::size_t max_bytes) const;

  /// Next byte position where the next append would write (end of logical log).
  std::uint64_t byte_offset() const;

  /// Number of fsyncs (or hook calls) performed by the active segment's guard — for tests.
  std::uint64_t fsync_count() const;

 private:
  /// Build path `directory/XXXXXXXX.seg` for the given segment index.
  std::string segment_path(std::uint32_t index) const;

  /// Scan `config_.directory` for `*.seg`, open in order, recover the last one only.
  void discover_segments();

  /// Finish current segment durably, append new empty segment, attach new `DurabilityGuard`.
  void roll_segment();

  LogConfig config_;

  mutable std::mutex mu_;
  std::vector<Segment> segments_;
  std::unique_ptr<DurabilityGuard> durability_;
};

}  // namespace log_storage
