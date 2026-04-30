#include "log_storage/storage/durability.hpp"

#include <unistd.h>

#include <stdexcept>

namespace log_storage {

/// Start optional background thread for Async mode; Sync mode does no threading.
DurabilityGuard::DurabilityGuard(int fd, DurabilityConfig config)
    : fd_(fd), config_(std::move(config)) {
  // Only start the background thread in Async mode.
  // In Sync mode, fsync is called inline by the writing thread.
  if (config_.mode == SyncMode::Async) {
    worker_ = std::thread(&DurabilityGuard::async_worker, this);
  }
}

/// Ensure shutdown runs exactly once for clean teardown.
DurabilityGuard::~DurabilityGuard() {
  shutdown();
}

/// Invoke real or hooked fsync and bump diagnostic counter.
void DurabilityGuard::do_fsync() {
  if (config_.fsync_hook) {
    config_.fsync_hook(fd_);
  } else {
    if (::fsync(fd_) != 0) {
      throw std::runtime_error("DurabilityGuard: fsync failed");
    }
  }
  fsync_count_.fetch_add(1, std::memory_order_relaxed);
}

/// Branch on Sync vs Async: inline flush vs mark dirty and wake worker.
void DurabilityGuard::after_write() {
  if (config_.mode == SyncMode::Sync) {
    // SYNC path: block until fsync completes. Caller cannot proceed
    // until the data is on stable storage.
    do_fsync();
    return;
  }

  // ASYNC path: mark dirty and wake the background thread.
  //
  // CRASH WINDOW: the write is in page cache but NOT on disk.
  // If the process dies before the background thread runs fsync,
  // this write is lost. The data loss window is bounded by
  // config_.async_interval_ms.
  dirty_.store(true, std::memory_order_release);
  cv_.notify_one();
}

/// Signal worker to exit, join it, flush any remaining dirty pages once.
void DurabilityGuard::shutdown() {
  // Idempotent: only the first call does work.
  bool was_running = !stop_.exchange(true, std::memory_order_acq_rel);
  if (!was_running) {
    return;
  }

  // Wake the background thread so it can exit.
  cv_.notify_all();

  if (worker_.joinable()) {
    worker_.join();
  }

  // Final fsync: flush any remaining dirty data to disk.
  // This ensures clean shutdown doesn't lose data.
  if (dirty_.load(std::memory_order_acquire)) {
    do_fsync();
    dirty_.store(false, std::memory_order_release);
  }
}

/// Background loop: sleep until dirty or stop, then fsync when dirty cleared with exchange.
void DurabilityGuard::async_worker() {
  const auto interval =
      std::chrono::milliseconds(config_.async_interval_ms);

  while (!stop_.load(std::memory_order_acquire)) {
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait_for(lk, interval, [this] {
        return stop_.load(std::memory_order_acquire) ||
               dirty_.load(std::memory_order_acquire);
      });
    }

    // fsync if there's dirty data, even if we're stopping
    // (shutdown() handles the final case, but clearing here avoids a race).
    if (dirty_.exchange(false, std::memory_order_acq_rel)) {
      do_fsync();
    }
  }
}

}  // namespace log_storage
