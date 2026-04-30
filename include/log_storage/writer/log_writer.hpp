#pragma once

#include "log_storage/durability/durability_manager.hpp"
#include "log_storage/durability/durability_mode.hpp"
#include "log_storage/format/irecord_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace log_storage {

/// Single-file append writer for **week 1–2** format: recovery on open, durability per append.
///
/// On construction runs `RecoveryManager::recover` so the file is a valid prefix of records.
/// Appends assign monotonic logical offsets (0, 1, 2, …) via the codec.
class LogWriter {
 public:
  /// Open-or-create `path`, recover tail, prepare optional durability manager config.
  explicit LogWriter(std::string path, DurabilityManager::Config durability_config = {});
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;
  LogWriter(LogWriter&& other) noexcept;
  LogWriter& operator=(LogWriter&& other) noexcept;
  ~LogWriter();

  /// Append with default mode `None` (write only; no fsync policy — see `DurabilityMode`).
  std::uint64_t append(const void* payload, std::size_t len);
  std::uint64_t append(const std::vector<std::uint8_t>& payload);

  /// Append with explicit durability: `Sync`, `Async`, or `None`.
  std::uint64_t append(const void* payload, std::size_t len, DurabilityMode mode);
  std::uint64_t append(const std::vector<std::uint8_t>& payload, DurabilityMode mode);

  /// Next logical offset that `append` would assign (after recovery + prior appends).
  std::uint64_t next_offset() const { return next_offset_; }

  /// Number of fsync operations performed by the embedded `DurabilityManager` (if created).
  std::uint64_t fsync_count() const;

 private:
  /// Codec from config or default V1 binary codec.
  IRecordCodec const& codec() const;

  std::string path_;
  int fd_ = -1;
  std::uint64_t next_offset_ = 0;

  DurabilityManager::Config durability_config_;
  std::unique_ptr<DurabilityManager> dm_;
};

}  // namespace log_storage
