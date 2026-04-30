#include "log_storage/storage/log.hpp"

#include "log_storage/storage/recovery.hpp"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <stdexcept>

namespace log_storage {

/// Create directory if needed, discover or create segments, attach durability to tail segment.
Log::Log(LogConfig config) : config_(std::move(config)) {
  // Ensure the log directory exists. mkdir returns -1 if it already exists,
  // which is fine — we only care if it fails for a real reason.
  if (::mkdir(config_.directory.c_str(), 0755) < 0 && errno != EEXIST) {
    throw std::runtime_error("Log: cannot create directory " + config_.directory);
  }

  discover_segments();

  // If no segments found (fresh start), create the first one.
  if (segments_.empty()) {
    segments_.push_back(segment_create(segment_path(0), 0));
  }

  // Start durability for the active (last) segment.
  durability_ = std::make_unique<DurabilityGuard>(
      segments_.back().fd, config_.durability);
}

/// Tear down durability (flush), then close every segment fd.
Log::~Log() {
  // Shutdown durability first (does final fsync), then close all segments.
  durability_.reset();
  for (auto& seg : segments_) {
    segment_close(seg);
  }
}

/// Format `NNNNNNNN.seg` under the configured directory.
std::string Log::segment_path(std::uint32_t index) const {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%08u.seg", index);
  return config_.directory + "/" + buf;
}

/// List `.seg` files, sort, open each with running `base_offset`, recover only the last file.
void Log::discover_segments() {
  DIR* dir = ::opendir(config_.directory.c_str());
  if (!dir) {
    return;
  }

  std::vector<std::string> names;
  struct dirent* entry = nullptr;
  while ((entry = ::readdir(dir)) != nullptr) {
    std::string name(entry->d_name);
    // Only consider files ending with ".seg".
    if (name.size() > 4 && name.substr(name.size() - 4) == ".seg") {
      names.push_back(name);
    }
  }
  ::closedir(dir);

  // Lexicographic sort gives chronological order because names are zero-padded.
  std::sort(names.begin(), names.end());

  std::uint64_t running_offset = 0;

  for (std::size_t i = 0; i < names.size(); ++i) {
    const std::string path = config_.directory + "/" + names[i];
    Segment seg = segment_open(path, running_offset);

    if (i == names.size() - 1) {
      // Last segment: may have a torn tail from a crash. Recover it.
      RecoveryResult rr = recover_segment(seg);
      running_offset += rr.valid_bytes;
    } else {
      // Earlier segments were finalized (fsynced + closed) before rolling.
      // They should be fully valid. We trust their size without re-scanning
      // to keep startup fast. A paranoid mode could validate these too.
      running_offset += seg.size;
    }

    segments_.push_back(std::move(seg));
  }
}

/// Close old guard (fsync), create next numbered segment, open new guard on new fd.
void Log::roll_segment() {
  // Step 1: shut down durability for the current segment.
  // This performs a final fsync, ensuring all data is on disk.
  durability_.reset();

  // Step 2: compute the new segment's base offset.
  const Segment& last = segments_.back();
  const std::uint64_t new_base = last.base_offset + last.size;
  const auto new_index = static_cast<std::uint32_t>(segments_.size());

  // Step 3: create the new segment file.
  //
  // CRASH NOTE: if we crash between step 1 and here, the old segment is
  // clean (fsynced). On restart, no new segment file exists, so we reopen
  // the old segment and continue appending.
  segments_.push_back(segment_create(segment_path(new_index), new_base));

  // Step 4: start durability for the new segment.
  durability_ = std::make_unique<DurabilityGuard>(
      segments_.back().fd, config_.durability);
}

/// Delegate to pointer + length append.
std::uint64_t Log::append(const std::vector<std::uint8_t>& payload) {
  return append(payload.data(), payload.size());
}

/// Encode, optional roll, append bytes, run durability policy; return global frame offset.
std::uint64_t Log::append(const void* payload, std::size_t len) {
  std::lock_guard<std::mutex> lk(mu_);

  // Encode the record into wire format: [length][payload][crc32]
  std::vector<std::uint8_t> wire;
  encode_record(payload, len, wire);

  // Roll to a new segment if this write would exceed the size limit.
  // Exception: if the segment is empty, allow one record regardless of size.
  // This prevents infinite roll loops for records larger than max_segment_bytes.
  Segment& active = segments_.back();
  if (active.size > 0 && active.size + wire.size() > config_.max_segment_bytes) {
    roll_segment();
  }

  // Write the encoded record to the active segment.
  const std::uint64_t offset =
      segment_append(segments_.back(), wire.data(), wire.size());

  // Apply the configured durability policy.
  //
  // SYNC:  blocks here until fsync completes. Data is durable on return.
  // ASYNC: returns immediately. Background thread fsyncs later.
  //        If crash happens before that fsync, this record is lost.
  durability_->after_write();

  return offset;
}

/// Find segment containing `offset`, decode forward until payload budget reached or EOF.
std::vector<Record> Log::read(std::uint64_t offset,
                              std::size_t max_bytes) const {
  std::lock_guard<std::mutex> lk(mu_);

  std::vector<Record> results;
  std::size_t total_payload = 0;

  // Binary search would be optimal, but linear scan is fine for typical
  // segment counts (hundreds at most).
  std::size_t seg_idx = 0;
  bool found = false;
  for (std::size_t i = 0; i < segments_.size(); ++i) {
    const Segment& s = segments_[i];
    if (offset >= s.base_offset && offset < s.base_offset + s.size) {
      seg_idx = i;
      found = true;
      break;
    }
  }

  if (!found) {
    return results;
  }

  // Decode records across segments until we've filled max_bytes of payload.
  std::uint64_t read_pos = offset;

  while (seg_idx < segments_.size()) {
    const Segment& seg = segments_[seg_idx];
    const std::uint64_t local = read_pos - seg.base_offset;
    const std::size_t remaining =
        static_cast<std::size_t>(seg.size - local);

    std::vector<std::uint8_t> buf(remaining);
    const std::size_t got = segment_read_at(seg, local, buf.data(), remaining);

    std::size_t buf_pos = 0;
    while (buf_pos < got) {
      DecodedRecord decoded;
      const DecodeResult dr =
          decode_record(buf.data() + buf_pos, got - buf_pos, decoded);

      if (dr != DecodeResult::Ok) {
        break;
      }

      total_payload += decoded.record.payload.size();
      results.push_back(std::move(decoded.record));
      buf_pos += decoded.bytes_consumed;

      // Respect max_bytes but always return at least one record.
      if (total_payload >= max_bytes && !results.empty()) {
        return results;
      }
    }

    // Move to the next segment.
    read_pos = seg.base_offset + seg.size;
    ++seg_idx;
  }

  return results;
}

/// Sum of last segment base + size — next append starts here.
std::uint64_t Log::byte_offset() const {
  std::lock_guard<std::mutex> lk(mu_);
  if (segments_.empty()) {
    return 0;
  }
  const Segment& last = segments_.back();
  return last.base_offset + last.size;
}

/// Forward to active `DurabilityGuard` counter (0 if none — should not happen after ctor).
std::uint64_t Log::fsync_count() const {
  return durability_ ? durability_->fsync_count() : 0;
}

}  // namespace log_storage
