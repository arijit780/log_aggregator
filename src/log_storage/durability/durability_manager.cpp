#include "log_storage/durability/durability_manager.hpp"

#include "log_storage/format/v1_binary_codec.hpp"
#include "log_storage/io/byte_io.hpp"

#include <unistd.h>

#include <cmath>
#include <stdexcept>

namespace log_storage {

namespace {

std::chrono::milliseconds ms_from_double(double ms) {
  if (ms < 1.0) {
    return std::chrono::milliseconds(1);
  }
  return std::chrono::milliseconds(static_cast<std::int64_t>(std::lround(ms)));
}

}  // namespace

// DurabilityManager is the "fsync policy engine" behind LogWriter.
//
// It supports mode-per-call semantics:
// - None: just write the record (no fsync policy)
// - Sync: append() blocks until an fsync batch covers the write (group commit via promises)
// - Async: append() returns after write(); a background worker periodically fsyncs dirty data
//
// All writes are still serialized under a mutex so logical offsets remain contiguous.
IRecordCodec const& DurabilityManager::codec() const {
  return codec_ptr_ != nullptr ? *codec_ptr_ : default_v1_binary_codec();
}

DurabilityManager::DurabilityManager(int fd, std::uint64_t next_offset, Config config)
    : fd_(fd),
      config_(std::move(config)),
      codec_ptr_(config_.record_codec),
      next_offset_(next_offset),
      sync_interval_ms_(ms_from_double(config_.sync_batch_interval_ms)),
      async_interval_ms_(ms_from_double(config_.async_flush_interval_ms)) {
  if (config_.sync_batch_max_records < 1u) {
    throw std::invalid_argument("sync_batch_max_records must be >= 1");
  }
  if (config_.async_flush_max_records < 1u) {
    throw std::invalid_argument("async_flush_max_records must be >= 1");
  }

  worker_ = std::thread(&DurabilityManager::commit_loop, this);
}

DurabilityManager::~DurabilityManager() { shutdown(); }

void DurabilityManager::do_fsync() {
  if (config_.fsync_hook) {
    config_.fsync_hook(fd_);
  } else {
    if (::fsync(fd_) != 0) {
      throw std::runtime_error("DurabilityManager: fsync failed");
    }
  }
  fsync_count_.fetch_add(1, std::memory_order_relaxed);
}

void DurabilityManager::shutdown() {
  bool expected = false;
  if (!stop_.compare_exchange_strong(expected, true)) {
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    cv_.notify_all();
  }
  if (worker_.joinable()) {
    worker_.join();
  }

  bool need_fsync = false;
  std::vector<std::unique_ptr<std::promise<void>>> tail;
  {
    std::lock_guard<std::mutex> lk(mu_);
    need_fsync = writes_since_fsync_ > 0 || !sync_pending_.empty();
    writes_since_fsync_ = 0;
    while (!sync_pending_.empty()) {
      tail.push_back(std::move(sync_pending_.front()));
      sync_pending_.pop_front();
    }
  }
  if (need_fsync) {
    do_fsync();
  }
  for (auto& p : tail) {
    p->set_value();
  }
}

std::uint64_t DurabilityManager::append(const void* payload, std::size_t len, DurabilityMode mode) {
  if (len > codec().max_payload_bytes()) {
    throw std::runtime_error("DurabilityManager: payload too large for codec");
  }

  if (mode == DurabilityMode::None) {
    std::lock_guard<std::mutex> lk(mu_);
    const std::uint64_t assigned = next_offset_;
    std::vector<std::uint8_t> record_bytes;
    codec().encode(assigned, payload, len, record_bytes);
    if (!write_all(fd_, record_bytes.data(), record_bytes.size())) {
      throw std::runtime_error("DurabilityManager: write failed");
    }
    next_offset_ = assigned + 1;
    return assigned;
  }

  if (mode == DurabilityMode::Sync) {
    // Sync mode is defined as "append returns only after the bytes are made durable".
    // We still batch fsync to avoid 1 fsync per record; each append enqueues a promise
    // that is fulfilled only after the worker performs an fsync covering that write.
    auto prom = std::make_unique<std::promise<void>>();
    std::future<void> fut = prom->get_future();
    std::uint64_t assigned = 0;
    {
      std::unique_lock<std::mutex> lk(mu_);
      assigned = next_offset_;
      std::vector<std::uint8_t> record_bytes;
      codec().encode(assigned, payload, len, record_bytes);
      if (!write_all(fd_, record_bytes.data(), record_bytes.size())) {
        throw std::runtime_error("DurabilityManager: write failed");
      }
      next_offset_ = assigned + 1;
      sync_pending_.push_back(std::move(prom));
      cv_.notify_one();
    }
    fut.wait();
    return assigned;
  }

  // Async mode trades durability latency for throughput:
  // append() returns after write(); fsync is performed periodically or when enough records accumulate.
  std::uint64_t assigned = 0;
  {
    std::lock_guard<std::mutex> lk(mu_);
    assigned = next_offset_;
    std::vector<std::uint8_t> record_bytes;
    codec().encode(assigned, payload, len, record_bytes);
    if (!write_all(fd_, record_bytes.data(), record_bytes.size())) {
      throw std::runtime_error("DurabilityManager: write failed");
    }
    next_offset_ = assigned + 1;
    ++writes_since_fsync_;
    if (writes_since_fsync_ >= config_.async_flush_max_records) {
      cv_.notify_one();
    }
  }
  return assigned;
}

void DurabilityManager::commit_loop() {
  // Single worker loop that services both:
  // - Sync waiters (promises that must be fulfilled only after an fsync), and
  // - Async flush requests (dirty writes that should be fsync'd periodically / by threshold).
  while (!stop_.load(std::memory_order_acquire)) {
    std::vector<std::unique_ptr<std::promise<void>>> batch;
    bool need_fsync = false;
    {
      std::unique_lock<std::mutex> lk(mu_);
      const auto wait_for = sync_interval_ms_ < async_interval_ms_ ? sync_interval_ms_ : async_interval_ms_;
      cv_.wait_for(lk, wait_for, [&] {
        return stop_.load(std::memory_order_acquire) || !sync_pending_.empty() ||
               writes_since_fsync_ >= config_.async_flush_max_records || writes_since_fsync_ > 0;
      });
      if (stop_.load(std::memory_order_acquire)) {
        break;
      }
      while (!sync_pending_.empty() && batch.size() < config_.sync_batch_max_records) {
        batch.push_back(std::move(sync_pending_.front()));
        sync_pending_.pop_front();
      }
      if (!batch.empty()) {
        need_fsync = true;
      }
      // Time-based async flush: if we woke up (timeout or notify) and there's dirty data, flush it.
      if (writes_since_fsync_ > 0) {
        need_fsync = true;
        writes_since_fsync_ = 0;
      }
    }
    if (!need_fsync) {
      continue;
    }
    do_fsync();
    for (auto& p : batch) {
      p->set_value();
    }
  }
}

}  // namespace log_storage
