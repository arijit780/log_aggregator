#pragma once

#include "log_storage/durability/durability_mode.hpp"
#include "log_storage/format/irecord_codec.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

namespace log_storage {

class DurabilityManager {
 public:
  struct Config {
    DurabilityMode mode = DurabilityMode::Sync;
    std::size_t sync_batch_max_records = 16;
    double sync_batch_interval_ms = 10.0;
    std::size_t async_flush_max_records = 32;
    double async_flush_interval_ms = 50.0;
    std::function<void(int)> fsync_hook;
    IRecordCodec const* record_codec = nullptr;
  };

  DurabilityManager(int fd, std::uint64_t next_offset, Config config);
  DurabilityManager(const DurabilityManager&) = delete;
  DurabilityManager& operator=(const DurabilityManager&) = delete;
  DurabilityManager(DurabilityManager&&) = delete;
  DurabilityManager& operator=(DurabilityManager&&) = delete;
  ~DurabilityManager();

  std::uint64_t append(const void* payload, std::size_t len);

  void shutdown();

  std::uint64_t fsync_count() const { return fsync_count_.load(std::memory_order_relaxed); }

  std::uint64_t next_offset() const {
    std::lock_guard<std::mutex> lk(mu_);
    return next_offset_;
  }

 private:
  void do_fsync();
  void sync_commit_loop();
  void async_flush_loop();
  IRecordCodec const& codec() const;

  int fd_;
  Config config_;
  IRecordCodec const* codec_ptr_ = nullptr;
  std::uint64_t next_offset_ = 0;
  std::atomic<bool> stop_{false};
  std::atomic<std::uint64_t> fsync_count_{0};

  mutable std::mutex mu_;
  std::condition_variable sync_cv_;
  std::condition_variable async_cv_;
  std::deque<std::unique_ptr<std::promise<void>>> sync_pending_;

  std::size_t writes_since_fsync_ = 0;

  std::thread worker_;

  std::chrono::milliseconds sync_interval_ms_{10};
  std::chrono::milliseconds async_interval_ms_{50};
};

}  // namespace log_storage
