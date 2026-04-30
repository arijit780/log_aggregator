#include "log_storage/storage/segment.hpp"

#include "log_storage/io/byte_io.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdexcept>

namespace log_storage {

/// Implementation of `segment_open`: POSIX open + fstat to learn current file length.
Segment segment_open(const std::string& path, std::uint64_t base_offset) {
  const int fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) {
    throw std::runtime_error("segment_open: cannot open " + path);
  }

  struct stat st {};
  if (::fstat(fd, &st) < 0) {
    ::close(fd);
    throw std::runtime_error("segment_open: fstat failed on " + path);
  }

  return Segment{path, fd, base_offset, static_cast<std::uint64_t>(st.st_size)};
}

/// Implementation of `segment_create`: create/truncate empty segment file.
Segment segment_create(const std::string& path, std::uint64_t base_offset) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    throw std::runtime_error("segment_create: cannot create " + path);
  }
  return Segment{path, fd, base_offset, 0};
}

/// Seek to end of valid data, append, bump `size`, return global start offset of write.
std::uint64_t segment_append(Segment& seg, const void* data, std::size_t len) {
  // Seek to end-of-valid-data before writing. This is necessary because
  // a prior pread() or recovery scan may have moved the fd position.
  if (::lseek(seg.fd, static_cast<off_t>(seg.size), SEEK_SET) < 0) {
    throw std::runtime_error("segment_append: lseek failed");
  }

  if (!write_all(seg.fd, data, len)) {
    // write(2) failed. The segment may now have a partial write at the tail.
    // We do NOT update seg.size, so the next recovery will truncate it.
    throw std::runtime_error("segment_append: write_all failed");
  }

  // CRASH WINDOW: data is in kernel page cache but NOT on disk.
  // If power fails now, recovery will truncate this write on next startup.

  const std::uint64_t record_offset = seg.base_offset + seg.size;
  seg.size += len;
  return record_offset;
}

/// Positional read without affecting append cursor — see header.
std::size_t segment_read_at(const Segment& seg, std::uint64_t local_offset,
                            void* buf, std::size_t max_bytes) {
  if (local_offset >= seg.size) {
    return 0;
  }

  const std::size_t available = static_cast<std::size_t>(seg.size - local_offset);
  const std::size_t to_read = available < max_bytes ? available : max_bytes;

  // pread(2): positional read that does NOT change the fd offset.
  // This makes reads safe to interleave with appends (under external locking).
  const ssize_t r = ::pread(seg.fd, buf, to_read, static_cast<off_t>(local_offset));
  if (r < 0) {
    throw std::runtime_error("segment_read_at: pread failed");
  }
  return static_cast<std::size_t>(r);
}

/// Flush segment fd (caller typically uses `DurabilityGuard` instead for policy).
void segment_fsync(const Segment& seg) {
  if (::fsync(seg.fd) != 0) {
    throw std::runtime_error("segment_fsync: fsync failed");
  }
}

/// Shrink file and `seg.size` to drop corrupt/partial suffix bytes.
void segment_truncate(Segment& seg, std::uint64_t local_offset) {
  // ftruncate(2): atomically sets the file size.
  // If the process crashes during ftruncate, the filesystem guarantees
  // the file is either at the old size or the new size — never in between.
  if (::ftruncate(seg.fd, static_cast<off_t>(local_offset)) < 0) {
    throw std::runtime_error("segment_truncate: ftruncate failed");
  }
  seg.size = local_offset;
}

/// Close fd once.
void segment_close(Segment& seg) {
  if (seg.fd >= 0) {
    ::close(seg.fd);
    seg.fd = -1;
  }
}

}  // namespace log_storage
