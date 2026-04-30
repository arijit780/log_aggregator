// Durability Tests — week 3-4
//
// Exercises the durability modes:
//   1. SYNC mode: data survives clean reopen (fsync per write)
//   2. ASYNC mode: data loss window (crash before background fsync)
//   3. ASYNC mode: data survives when given time to flush
//   4. fsync hook injection for testing
//   5. Crash during write (fork + SIGKILL) — recovery preserves a valid prefix
//
// The key invariant across all tests: whatever records survive a crash
// must form a valid, contiguous prefix of what was written.

#include "log_storage/io/byte_io.hpp"
#include "log_storage/storage/log.hpp"
#include "log_storage/storage/record.hpp"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::string make_temp_dir() {
  char tmpl[] = "/tmp/dur_test_XXXXXX";
  if (::mkdtemp(tmpl) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
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
// Test 1: SYNC mode — data is on disk after append returns
// ---------------------------------------------------------------
// Write a record in sync mode, close, reopen. Must be readable.

bool test_sync_durability() {
  const std::string dir = make_temp_dir();

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    log.append(std::vector<std::uint8_t>{'s', 'y', 'n', 'c'});

    TEST_ASSERT(log.fsync_count() >= 1,
                "sync mode must call fsync at least once per append");
  }

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 1024);
    TEST_ASSERT(records.size() == 1, "expected 1 record after sync reopen");
    TEST_ASSERT(records[0].payload ==
                    std::vector<std::uint8_t>({'s', 'y', 'n', 'c'}),
                "payload must match what was written");
  }

  cleanup(dir);
  std::cout << "  sync_durability: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 2: ASYNC mode — data loss window
// ---------------------------------------------------------------
// In async mode with a very long flush interval, if the process
// crashes immediately after writing, data may be lost.
//
// We use fork + _exit(0) to simulate a crash before the background
// thread has a chance to fsync. The key invariant: whatever records
// DO survive must be valid (CRC-checked) and contiguous.

bool test_async_data_loss_window() {
  const std::string dir = make_temp_dir();

  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }

  if (pid == 0) {
    // Child: write records in async mode, then _exit immediately.
    // _exit() skips destructors, simulating a crash.
    try {
      LogConfig cfg;
      cfg.directory = dir;
      cfg.durability.mode = SyncMode::Async;
      cfg.durability.async_interval_ms = 60000;  // effectively never
      Log log(cfg);

      for (int i = 0; i < 50; ++i) {
        std::vector<std::uint8_t> p(8);
        pack_le_u64(p.data(), static_cast<std::uint64_t>(i));
        log.append(p);
      }
      _exit(0);
    } catch (...) {
      _exit(2);
    }
  }

  int status = 0;
  ::waitpid(pid, &status, 0);

  // Read back whatever survived.
  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);

    // Can't assert exact count — async means some may be lost.
    // But every record must be valid and form a contiguous prefix.
    for (std::size_t i = 0; i < records.size(); ++i) {
      std::uint64_t val = 0;
      unpack_le_u64(records[i].payload.data(), &val);
      TEST_ASSERT(val == static_cast<std::uint64_t>(i),
                  "surviving records must be a contiguous prefix");
    }

    std::cout << "    (async crash: " << records.size()
              << "/50 records survived)\n";
  }

  cleanup(dir);
  std::cout << "  async_data_loss_window: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 3: ASYNC mode — eventual flush
// ---------------------------------------------------------------
// With a short flush interval, data should be durable after sleeping
// long enough for the background thread to run.

bool test_async_eventual_flush() {
  const std::string dir = make_temp_dir();

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Async;
    cfg.durability.async_interval_ms = 10;  // fast flush
    Log log(cfg);

    for (int i = 0; i < 20; ++i) {
      log.append(std::vector<std::uint8_t>{static_cast<std::uint8_t>(i)});
    }

    // Give the background thread time to fsync.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    TEST_ASSERT(log.fsync_count() >= 1,
                "async mode should have fsynced at least once");
  }

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 1024);
    TEST_ASSERT(records.size() == 20,
                "expected all 20 records after async flush + clean reopen");
  }

  cleanup(dir);
  std::cout << "  async_eventual_flush: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 4: fsync hook
// ---------------------------------------------------------------
// Inject a custom fsync via the hook. Verify it gets called.

bool test_fsync_hook() {
  const std::string dir = make_temp_dir();

  std::atomic<int> hook_calls{0};

  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    cfg.durability.fsync_hook = [&hook_calls](int fd) {
      hook_calls.fetch_add(1, std::memory_order_relaxed);
      if (::fsync(fd) != 0) {
        throw std::runtime_error("hook fsync failed");
      }
    };
    Log log(cfg);

    log.append(std::vector<std::uint8_t>{'a'});
    log.append(std::vector<std::uint8_t>{'b'});
  }

  TEST_ASSERT(hook_calls.load() >= 2,
              "sync mode should call fsync hook at least once per append");

  cleanup(dir);
  std::cout << "  fsync_hook: OK\n";
  return true;
}

// ---------------------------------------------------------------
// Test 5: Crash during write (fork + SIGKILL)
// ---------------------------------------------------------------
// Write baseline records, then fork a child that writes rapidly.
// Kill the child mid-stream. Recovery must produce a valid prefix
// containing at least the baseline records.

bool test_crash_during_write() {
  const std::string dir = make_temp_dir();
  constexpr int kBaselineRecords = 5;

  // Write baseline records (these must survive).
  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    for (int i = 0; i < kBaselineRecords; ++i) {
      std::vector<std::uint8_t> p(8);
      pack_le_u64(p.data(), static_cast<std::uint64_t>(i));
      log.append(p);
    }
  }

  // Fork a child that writes more records, then gets SIGKILL'd.
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }

  if (pid == 0) {
    try {
      LogConfig cfg;
      cfg.directory = dir;
      cfg.durability.mode = SyncMode::Sync;
      Log log(cfg);

      for (std::uint64_t i = kBaselineRecords; i < 100000; ++i) {
        std::vector<std::uint8_t> p(8);
        pack_le_u64(p.data(), i);
        log.append(p);
      }
    } catch (...) {
    }
    _exit(0);
  }

  // Let the child write for a bit, then kill it.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ::kill(pid, SIGKILL);
  int st = 0;
  ::waitpid(pid, &st, 0);

  // Recover and verify.
  {
    LogConfig cfg;
    cfg.directory = dir;
    cfg.durability.mode = SyncMode::Sync;
    Log log(cfg);

    const auto records = log.read(0, 64 * 1024 * 1024);
    TEST_ASSERT(records.size() >= static_cast<std::size_t>(kBaselineRecords),
                "must recover at least the baseline records");

    // Every surviving record must have the correct sequential value.
    for (std::size_t i = 0; i < records.size(); ++i) {
      std::uint64_t val = 0;
      unpack_le_u64(records[i].payload.data(), &val);
      TEST_ASSERT(val == static_cast<std::uint64_t>(i),
                  "surviving records must be a contiguous prefix");
    }

    std::cout << "    (crash: " << records.size() << " records survived)\n";
  }

  cleanup(dir);
  std::cout << "  crash_during_write: OK\n";
  return true;
}

}  // namespace

int main() {
  std::cout << "=== durability_tests ===\n";
  int failures = 0;

  if (!test_sync_durability()) ++failures;
  if (!test_async_data_loss_window()) ++failures;
  if (!test_async_eventual_flush()) ++failures;
  if (!test_fsync_hook()) ++failures;
  if (!test_crash_during_write()) ++failures;

  if (failures > 0) {
    std::cerr << "\n" << failures << " test(s) FAILED.\n";
    return 1;
  }

  std::cout << "\nAll durability tests passed.\n";
  return 0;
}
