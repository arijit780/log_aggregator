// Recovery Tests — week 3-4
//
// Exercises every recovery scenario:
//   1. Normal operation (clean write/read cycle)
//   2. Partial write (simulated crash mid-record)
//   3. Garbage tail (random bytes appended)
//   4. CRC corruption (bit flip in a valid record)
//   5. Consumer offset store recovery
//   6. Offset store corrupt tail
//   7. Multi-segment read across segment boundaries
//
// Each test creates a fresh temp directory, runs the scenario, verifies the
// invariant ("after recovery, the log is a valid prefix"), and cleans up.

#include "log_storage/consumer/offset_store.hpp"
#include "log_storage/crypto/crc32.hpp"
#include "log_storage/io/byte_io.hpp"
#include "log_storage/storage/log.hpp"
#include "log_storage/storage/record.hpp"
#include "log_storage/storage/recovery.hpp"
#include "log_storage/storage/segment.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

std::string make_temp_dir() {
  char tmpl[] = "/tmp/log_test_XXXXXX";
  if (::mkdtemp(tmpl) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return std::string(tmpl);
}

std::string make_temp_file() {
  char tmpl[] = "/tmp/log_file_XXXXXX";
  const int fd = ::mkstemp(tmpl);
  if (fd < 0) {
    throw std::runtime_error("mkstemp failed");
  }
  ::close(fd);
  return std::string(tmpl);
}

void cleanup(const std::string& path) {
  std::string cmd = "rm -rf " + path;
  (void)::system(cmd.c_str());
}

#define TEST_ASSERT(cond, msg)                                       \
  do {                                                               \
    if (!(cond)) {                                                   \
      std::cerr << "  FAILED: " << (msg) << "\n"                    \
                << "    condition: " #cond "\n"                      \
                << "    at " << __FILE__ << ":" << __LINE__ << "\n"; \
      return false;                                                  \
    }                                                                \
  } while (0)

using namespace log_storage;

// ---------------------------------------------------------------
// Test 1: Recovery correctness — normal case
// ---------------------------------------------------------------
// Write N records, close cleanly, reopen and verify all are readable.
// This is the baseline: recovery should be a no-op on a clean log.

bool test_recovery_normal() {
  const std::string dir = make_temp_dir();

  constexpr int kRecords = 100;

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    for (int i = 0; i < kRecords; ++i) {
      std::vector<std::uint8_t> payload(8);
      pack_le_u64(payload.data(), static_cast<std::uint64_t>(i));
      log.append(payload);
    }
  }

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);
    TEST_ASSERT(records.size() == kRecords,
                "expected 100 records after clean reopen");

    for (int i = 0; i < kRecords; ++i) {
      std::uint64_t val = 0;
      unpack_le_u64(records[i].payload.data(), &val);
      TEST_ASSERT(val == static_cast<std::uint64_t>(i), "payload value mismatch");
    }
  }

  cleanup(dir);
  std::cout << "  recovery_normal: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 2: Crash during write — partial record at EOF
// ---------------------------------------------------------------
// Write 10 records, then manually append a partial record (just the
// length field, no payload or CRC). Recovery must truncate the partial.

bool test_partial_write() {
  const std::string dir = make_temp_dir();
  constexpr int kGoodRecords = 10;

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    for (int i = 0; i < kGoodRecords; ++i) {
      std::vector<std::uint8_t> p = {'r', static_cast<std::uint8_t>(i)};
      log.append(p);
    }
  }

  // Simulate a torn write: append only the length field of a record.
  {
    const std::string seg_path = dir + "/00000000.seg";
    const int fd = ::open(seg_path.c_str(), O_WRONLY | O_APPEND);
    TEST_ASSERT(fd >= 0, "open segment for partial write injection");

    std::uint8_t partial[4];
    pack_le_u32(partial, 42);  // length=42, but no payload or CRC follows
    const ssize_t w = ::write(fd, partial, sizeof(partial));
    TEST_ASSERT(w == sizeof(partial), "write partial record");
    ::close(fd);
  }

  // Reopen: recovery must remove the partial record.
  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);
    TEST_ASSERT(records.size() == kGoodRecords,
                "expected 10 records after partial-write recovery");
  }

  cleanup(dir);
  std::cout << "  partial_write: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 3: Garbage tail
// ---------------------------------------------------------------
// Write valid records, then append random garbage bytes.
// Recovery must trim everything after the last valid record.

bool test_garbage_tail() {
  const std::string dir = make_temp_dir();
  constexpr int kGoodRecords = 5;

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    for (int i = 0; i < kGoodRecords; ++i) {
      std::vector<std::uint8_t> p = {'d', static_cast<std::uint8_t>(i)};
      log.append(p);
    }
  }

  // Append 128 bytes of random garbage.
  {
    const std::string seg_path = dir + "/00000000.seg";
    const int fd = ::open(seg_path.c_str(), O_WRONLY | O_APPEND);
    TEST_ASSERT(fd >= 0, "open segment for garbage injection");

    std::mt19937 rng(12345);
    std::uint8_t garbage[128];
    for (auto& b : garbage) {
      b = static_cast<std::uint8_t>(rng() & 0xFF);
    }
    const ssize_t w = ::write(fd, garbage, sizeof(garbage));
    TEST_ASSERT(w == sizeof(garbage), "write garbage tail");
    ::close(fd);
  }

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);
    TEST_ASSERT(records.size() == kGoodRecords,
                "expected 5 records after garbage-tail recovery");

    for (int i = 0; i < kGoodRecords; ++i) {
      TEST_ASSERT(records[i].payload.size() == 2, "payload size");
      TEST_ASSERT(records[i].payload[0] == 'd', "payload tag byte");
      TEST_ASSERT(records[i].payload[1] == static_cast<std::uint8_t>(i),
                  "payload index byte");
    }
  }

  cleanup(dir);
  std::cout << "  garbage_tail: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 4: CRC corruption
// ---------------------------------------------------------------
// Write 20 records, then flip a bit in the payload of record #10.
// Recovery must truncate at record #10, keeping only records 0–9.

bool test_crc_corruption() {
  const std::string dir = make_temp_dir();
  constexpr int kTotalRecords = 20;
  constexpr int kCorruptAt = 10;
  constexpr int kPayloadSize = 16;

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    for (int i = 0; i < kTotalRecords; ++i) {
      std::vector<std::uint8_t> p(kPayloadSize, static_cast<std::uint8_t>(i));
      log.append(p);
    }
  }

  // Each record is 4 (length) + 16 (payload) + 4 (CRC) = 24 bytes.
  // Record #10 starts at byte 10 * 24 = 240.
  // Corrupt a byte inside its payload area (offset 240 + 4 = 244).
  {
    const std::string seg_path = dir + "/00000000.seg";
    const int fd = ::open(seg_path.c_str(), O_RDWR);
    TEST_ASSERT(fd >= 0, "open segment for CRC corruption");

    const off_t corrupt_pos =
        static_cast<off_t>(kCorruptAt) * 24 + 4;  // inside payload
    std::uint8_t byte = 0;
    TEST_ASSERT(::pread(fd, &byte, 1, corrupt_pos) == 1, "read target byte");
    byte ^= 0xFF;  // flip all bits
    TEST_ASSERT(::pwrite(fd, &byte, 1, corrupt_pos) == 1, "write corrupted byte");
    ::close(fd);
  }

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);
    TEST_ASSERT(records.size() == static_cast<std::size_t>(kCorruptAt),
                "expected 10 records after CRC corruption recovery");

    for (int i = 0; i < kCorruptAt; ++i) {
      TEST_ASSERT(records[i].payload.size() == kPayloadSize, "payload size intact");
      for (auto& b : records[i].payload) {
        TEST_ASSERT(b == static_cast<std::uint8_t>(i), "payload content intact");
      }
    }
  }

  cleanup(dir);
  std::cout << "  crc_corruption: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 5: Consumer offset store — normal recovery
// ---------------------------------------------------------------
// Commit several offsets, close, reopen, verify all are recovered.

bool test_offset_store_recovery() {
  const std::string path = make_temp_file();

  {
    OffsetStore store(path);
    store.commit("group-a", 0, 100);
    store.commit("group-a", 1, 200);
    store.commit("group-b", 0, 50);
    store.commit("group-a", 0, 150);  // overwrite: group-a/0 moves forward
  }

  {
    OffsetStore store(path);
    std::uint64_t offset = 0;

    TEST_ASSERT(store.get("group-a", 0, offset) && offset == 150,
                "group-a/0 should be 150 (latest commit wins)");
    TEST_ASSERT(store.get("group-a", 1, offset) && offset == 200,
                "group-a/1 should be 200");
    TEST_ASSERT(store.get("group-b", 0, offset) && offset == 50,
                "group-b/0 should be 50");
    TEST_ASSERT(!store.get("group-c", 0, offset),
                "group-c should not exist");
  }

  ::unlink(path.c_str());
  std::cout << "  offset_store_recovery: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 6: Offset store — corrupt tail recovery
// ---------------------------------------------------------------
// Commit offsets, append garbage, reopen. Valid entries must survive.

bool test_offset_store_corrupt_tail() {
  const std::string path = make_temp_file();

  {
    OffsetStore store(path);
    store.commit("g1", 0, 10);
    store.commit("g1", 1, 20);
  }

  // Append garbage to the metadata log.
  {
    const int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
    TEST_ASSERT(fd >= 0, "open offset store for garbage injection");

    std::uint8_t junk[32];
    std::memset(junk, 0xDE, sizeof(junk));
    const ssize_t w = ::write(fd, junk, sizeof(junk));
    TEST_ASSERT(w == sizeof(junk), "write garbage to offset store");
    ::close(fd);
  }

  {
    OffsetStore store(path);
    std::uint64_t offset = 0;

    TEST_ASSERT(store.get("g1", 0, offset) && offset == 10,
                "g1/0 survives corrupt tail");
    TEST_ASSERT(store.get("g1", 1, offset) && offset == 20,
                "g1/1 survives corrupt tail");
  }

  ::unlink(path.c_str());
  std::cout << "  offset_store_corrupt_tail: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 7: Multi-segment read
// ---------------------------------------------------------------
// Use tiny max_segment_bytes to force segment rolling.
// Verify records are readable across segment boundaries.

bool test_multi_segment() {
  const std::string dir = make_temp_dir();
  constexpr int kRecords = 20;
  constexpr int kPayloadSize = 16;

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.max_segment_bytes = 100;  // force rolling (each record = 24 bytes)
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    for (int i = 0; i < kRecords; ++i) {
      std::vector<std::uint8_t> p(kPayloadSize, static_cast<std::uint8_t>(i));
      log.append(p);
    }
  }

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.max_segment_bytes = 100;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);
    TEST_ASSERT(records.size() == kRecords,
                "expected 20 records across multiple segments");

    for (int i = 0; i < kRecords; ++i) {
      TEST_ASSERT(records[i].payload.size() == kPayloadSize, "payload size");
      for (auto& b : records[i].payload) {
        TEST_ASSERT(b == static_cast<std::uint8_t>(i), "payload content");
      }
    }
  }

  cleanup(dir);
  std::cout << "  multi_segment: OK\n";
  return true;
}

}  // namespace

int main() {
  std::cout << "=== recovery_tests ===\n";
  int failures = 0;

  if (!test_recovery_normal()) ++failures;
  if (!test_partial_write()) ++failures;
  if (!test_garbage_tail()) ++failures;
  if (!test_crc_corruption()) ++failures;
  if (!test_offset_store_recovery()) ++failures;
  if (!test_offset_store_corrupt_tail()) ++failures;
  if (!test_multi_segment()) ++failures;

  if (failures > 0) {
    std::cerr << "\n" << failures << " test(s) FAILED.\n";
    return 1;
  }

  std::cout << "\nAll recovery tests passed.\n";
  return 0;
}
