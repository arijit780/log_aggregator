#pragma once

// Consumer Offset Store — server-side tracking of (group, partition) → offset.
//
// Backed by a separate append-only metadata log using the same record format
// as the data log: [uint32 length][payload][uint32 crc32].
//
// Metadata payload layout:
//   [uint32 group_len][group_bytes][uint32 partition][uint64 offset]
//
// On construction: the store scans the metadata log from byte 0, replays every
// valid entry, and rebuilds the in-memory offset map. Last-writer-wins: the
// final commit for a given (group, partition) key is the authoritative value.
//
// Commits are always fsynced immediately. If the process crashes before fsync,
// the commit is lost and the consumer re-reads from its previous offset
// (at-least-once delivery semantics).
//
// Corrupt tail in the metadata log is truncated on recovery, just like the
// data log. This means some recent commits may be lost after a crash, but
// the store is always in a consistent state.

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace log_storage {

/// Key for consumer offset map: consumer group id string + partition number.
struct ConsumerKey {
  std::string group;
  std::uint32_t partition = 0;

  bool operator==(const ConsumerKey& o) const {
    return group == o.group && partition == o.partition;
  }
};

/// Hash functor so `ConsumerKey` can be used in `std::unordered_map`.
struct ConsumerKeyHash {
  std::size_t operator()(const ConsumerKey& k) const {
    std::size_t h = std::hash<std::string>{}(k.group);
    h ^= std::hash<std::uint32_t>{}(k.partition) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

/// Append-only metadata log of committed consumer offsets, rebuilt on open.
class OffsetStore {
 public:
  /// Open `metadata_log_path` (create if needed), run `recover()` to replay disk state.
  ///
  /// \throws std::runtime_error if open/stat/read fails during recovery.
  explicit OffsetStore(const std::string& metadata_log_path);

  /// Close the metadata file descriptor.
  ~OffsetStore();

  OffsetStore(const OffsetStore&) = delete;
  OffsetStore& operator=(const OffsetStore&) = delete;

  /// Append one commit record and `fsync` before updating memory.
  ///
  /// Durably stores `(group, partition) → offset`. Latest commit for a key wins when
  /// replaying. **Crash:** if process dies after write but before fsync, entry may be
  /// dropped on next recovery — consumer may see duplicate delivery (at-least-once).
  void commit(const std::string& group, std::uint32_t partition, std::uint64_t offset);

  /// Read last committed offset for `group` + `partition`.
  ///
  /// \param out Receives the offset when returning true.
  /// \return False if no commit exists for that key.
  bool get(const std::string& group, std::uint32_t partition,
           std::uint64_t& out) const;

 private:
  /// Scan file from start, decode frames, apply entries to `offsets_`, truncate bad tail.
  void recover();

  std::string path_;
  int fd_ = -1;
  std::uint64_t write_pos_ = 0;  ///< Next append offset in the metadata file (end of valid prefix).

  mutable std::mutex mu_;
  std::unordered_map<ConsumerKey, std::uint64_t, ConsumerKeyHash> offsets_;
};

}  // namespace log_storage
