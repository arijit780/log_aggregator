#include "log_storage/consumer/offset_store.hpp"

#include "log_storage/crypto/crc32.hpp"
#include "log_storage/io/byte_io.hpp"
#include "log_storage/storage/record.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>
#include <vector>

namespace log_storage {

namespace {

/// Serialize one logical commit into the variable-length payload inside a record frame.
///
/// Layout: [uint32 group_len][group_bytes][uint32 partition][uint64 offset]
std::vector<std::uint8_t> encode_offset_entry(const std::string& group,
                                              std::uint32_t partition,
                                              std::uint64_t offset) {
  const std::size_t payload_len = 4 + group.size() + 4 + 8;
  std::vector<std::uint8_t> payload(payload_len);
  std::uint8_t* p = payload.data();

  pack_le_u32(p, static_cast<std::uint32_t>(group.size()));
  p += 4;
  std::memcpy(p, group.data(), group.size());
  p += group.size();
  pack_le_u32(p, partition);
  p += 4;
  pack_le_u64(p, offset);

  return payload;
}

/// Parse payload bytes from a decoded record body into structured fields.
///
/// \return True if `len` is sufficient and lengths are self-consistent.
bool decode_offset_entry(const std::uint8_t* data, std::size_t len,
                         std::string& group_out, std::uint32_t& partition_out,
                         std::uint64_t& offset_out) {
  if (len < 4) {
    return false;
  }

  std::uint32_t group_len = 0;
  unpack_le_u32(data, &group_len);

  // 4 (group_len) + group_len + 4 (partition) + 8 (offset)
  if (len < 4 + group_len + 4 + 8) {
    return false;
  }

  group_out.assign(reinterpret_cast<const char*>(data + 4), group_len);
  unpack_le_u32(data + 4 + group_len, &partition_out);
  unpack_le_u64(data + 4 + group_len + 4, &offset_out);

  return true;
}

}  // namespace

/// Open metadata file and replay `recover()` before serving reads/writes.
OffsetStore::OffsetStore(const std::string& path) : path_(path) {
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("OffsetStore: cannot open " + path_);
  }
  recover();
}

/// Close backing fd.
OffsetStore::~OffsetStore() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

/// Full-file replay: decode frames sequentially, apply last value per key, truncate junk suffix.
void OffsetStore::recover() {
  // Scan the metadata log from byte 0, decode every valid record,
  // and rebuild the in-memory offset map.
  //
  // Last-writer-wins: for each (group, partition), the value from the
  // most recent valid record is the truth.

  struct stat st {};
  if (::fstat(fd_, &st) < 0) {
    throw std::runtime_error("OffsetStore: fstat failed");
  }

  const auto file_size = static_cast<std::uint64_t>(st.st_size);
  if (file_size == 0) {
    write_pos_ = 0;
    return;
  }

  // Read entire metadata log into memory.
  std::vector<std::uint8_t> data(static_cast<std::size_t>(file_size));
  if (::lseek(fd_, 0, SEEK_SET) < 0) {
    throw std::runtime_error("OffsetStore: lseek to start failed");
  }
  if (!read_exact(fd_, data.data(), data.size())) {
    throw std::runtime_error("OffsetStore: short read during recovery");
  }

  std::uint64_t pos = 0;

  while (pos < file_size) {
    DecodedRecord decoded;
    const DecodeResult dr = decode_record(
        data.data() + pos,
        static_cast<std::size_t>(file_size - pos), decoded);

    if (dr != DecodeResult::Ok) {
      // Truncated or Corrupt: stop here.
      break;
    }

    // Parse the record payload as a consumer offset entry.
    std::string group;
    std::uint32_t partition = 0;
    std::uint64_t offset = 0;
    if (decode_offset_entry(decoded.record.payload.data(),
                            decoded.record.payload.size(),
                            group, partition, offset)) {
      offsets_[ConsumerKey{group, partition}] = offset;
    }

    pos += decoded.bytes_consumed;
  }

  // Truncate any corrupt tail in the metadata log.
  // Same logic as data-log recovery: only keep the valid prefix.
  if (pos < file_size) {
    if (::ftruncate(fd_, static_cast<off_t>(pos)) < 0) {
      throw std::runtime_error("OffsetStore: ftruncate failed");
    }
  }

  write_pos_ = pos;
}

/// Serialize commit, append at `write_pos_`, fsync, then update map and cursor.
void OffsetStore::commit(const std::string& group, std::uint32_t partition,
                         std::uint64_t offset) {
  std::lock_guard<std::mutex> lk(mu_);

  // Step 1: encode the offset entry as a record payload.
  std::vector<std::uint8_t> payload =
      encode_offset_entry(group, partition, offset);

  // Step 2: frame it with the standard record format [length][payload][crc32].
  std::vector<std::uint8_t> wire;
  encode_record(payload.data(), payload.size(), wire);

  // Step 3: seek and write.
  if (::lseek(fd_, static_cast<off_t>(write_pos_), SEEK_SET) < 0) {
    throw std::runtime_error("OffsetStore: lseek for commit failed");
  }
  if (!write_all(fd_, wire.data(), wire.size())) {
    throw std::runtime_error("OffsetStore: write failed on commit");
  }

  // Step 4: fsync BEFORE updating in-memory state.
  // This ordering guarantees that if we crash after fsync, the commit is
  // durable. If we crash before fsync, the commit is lost and the
  // in-memory state was never updated, so everything is consistent.
  //
  // CRASH NOTE: if we crash between write and fsync, the metadata log
  // has unsynced data. On recovery, ftruncate will remove it (CRC won't
  // match a partial record). This is at-least-once semantics.
  if (::fsync(fd_) != 0) {
    throw std::runtime_error("OffsetStore: fsync failed on commit");
  }

  // Step 5: update in-memory state and write position.
  write_pos_ += wire.size();
  offsets_[ConsumerKey{group, partition}] = offset;
}

/// Lookup under mutex; copies offset to `out` when found.
bool OffsetStore::get(const std::string& group, std::uint32_t partition,
                      std::uint64_t& out) const {
  std::lock_guard<std::mutex> lk(mu_);
  const auto it = offsets_.find(ConsumerKey{group, partition});
  if (it == offsets_.end()) {
    return false;
  }
  out = it->second;
  return true;
}

}  // namespace log_storage
