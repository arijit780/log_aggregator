#pragma once

// Durability — explicit fsync policy, chosen at startup.
//
// Two modes, no mixing:
//
//   SYNC:  write → fsync → return
//          Every append blocks until data is on stable storage.
//          Highest durability, lowest throughput.
//
//   ASYNC: write → return → fsync later (background thread)
//          Append returns after write(2). A background thread periodically
//          calls fsync(2). If the process crashes between write and fsync,
//          those records are LOST. This is the explicit trade-off.
//
// The mode is set via DurabilityConfig at construction time.
// There is no per-call mode selection — that would break the "no mixing" rule.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

namespace log_storage {

/// Which durability policy applies for the lifetime of a `DurabilityGuard`.
enum class SyncMode {
  Sync,   ///< Every `after_write` flushes to disk before returning.
  Async,  ///< `after_write` returns after marking dirty; background `fsync` later.
};

/// Configuration for `DurabilityGuard`: mode, async timing, optional test hook.
struct DurabilityConfig {
  SyncMode mode = SyncMode::Sync;

  /// Async only: upper bound on delay before a background flush (wall-clock sleep granularity).
  std::uint32_t async_interval_ms = 50;

  /// If set, called instead of `::fsync(fd)` (e.g. to count or simulate fsync in tests).
  std::function<void(int fd)> fsync_hook;
};

/// Owns the fsync policy for one open file descriptor (typically one active segment).
///
/// Call `after_write()` immediately after each successful `write` to that fd.
/// The caller must serialize writes to the fd (e.g. one mutex for the log).
class DurabilityGuard {
 public:
  /// Construct guard for `fd` using `config.mode`.
  ///
  /// Async mode starts a background thread that wakes on interval or when dirty.
  /// Sync mode does not start a thread.
  DurabilityGuard(int fd, DurabilityConfig config);

  /// Destroys the guard: equivalent to `shutdown()` (final flush in async).
  ~DurabilityGuard();

  DurabilityGuard(const DurabilityGuard&) = delete;
  DurabilityGuard& operator=(const DurabilityGuard&) = delete;

  /// Invoke after each successful `write(2)` to the guarded fd.
  ///
  /// **Sync:** Blocks until `fsync` completes (or hook returns). Data durable when it returns
  /// (modulo OS/fs guarantees during `fsync` itself).
  ///
  /// **Async:** Returns quickly after setting dirty; data may be lost until background `fsync`.
  void after_write();

  /// Stop the async worker (if any), join it, and perform a final `fsync` if dirty.
  /// Idempotent; safe to call from destructor.
  void shutdown();

  /// Number of completed `fsync` operations (or hook invocations) for metrics/tests.
  std::uint64_t fsync_count() const {
    return fsync_count_.load(std::memory_order_relaxed);
  }

 private:
  void do_fsync();
  void async_worker();

  int fd_;
  DurabilityConfig config_;

  std::atomic<bool> stop_{false};
  std::atomic<bool> dirty_{false};
  std::atomic<std::uint64_t> fsync_count_{0};

  std::mutex mu_;
  std::condition_variable cv_;
  std::thread worker_;
};

}  // namespace log_storage
