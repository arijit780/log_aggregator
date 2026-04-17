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

  if (config_.mode == DurabilityMode::Sync) {
    worker_ = std::thread(&DurabilityManager::sync_commit_loop, this);
  } else {
    worker_ = std::thread(&DurabilityManager::async_flush_loop, this);
  }
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
    sync_cv_.notify_all();
    async_cv_.notify_all();
  }
  if (worker_.joinable()) {
    worker_.join();
  }

  if (config_.mode == DurabilityMode::Sync) {
    std::vector<std::unique_ptr<std::promise<void>>> tail;
    {
      std::lock_guard<std::mutex> lk(mu_);
      while (!sync_pending_.empty()) {
        tail.push_back(std::move(sync_pending_.front()));
        sync_pending_.pop_front();
      }
    }
    if (!tail.empty()) {
      do_fsync();
      for (auto& p : tail) {
        p->set_value();
      }
    }
  } else {
    bool need = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (writes_since_fsync_ > 0) {
        writes_since_fsync_ = 0;
        need = true;
      }
    }
    if (need) {
      do_fsync();
    }
  }
}

std::uint64_t DurabilityManager::append(const void* payload, std::size_t len) {
  if (len > codec().max_payload_bytes()) {
    throw std::runtime_error("DurabilityManager: payload too large for codec");
  }

  if (config_.mode == DurabilityMode::Sync) {
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
      sync_cv_.notify_one();
    }
    fut.wait();
    return assigned;
  }

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
      async_cv_.notify_one();
    }
  }
  return assigned;
}

void DurabilityManager::sync_commit_loop() {
  while (!stop_.load(std::memory_order_acquire)) {
    std::vector<std::unique_ptr<std::promise<void>>> batch;
    {
      std::unique_lock<std::mutex> lk(mu_);
      sync_cv_.wait_for(lk, sync_interval_ms_, [&] {
        return stop_.load(std::memory_order_acquire) || !sync_pending_.empty();
      });
      if (stop_.load(std::memory_order_acquire)) {
        break;
      }
      while (!sync_pending_.empty() && batch.size() < config_.sync_batch_max_records) {
        batch.push_back(std::move(sync_pending_.front()));
        sync_pending_.pop_front();
      }
    }
    if (batch.empty()) {
      continue;
    }
    do_fsync();
    for (auto& p : batch) {
      p->set_value();
    }
  }
}

void DurabilityManager::async_flush_loop() {
  while (!stop_.load(std::memory_order_acquire)) {
    bool dirty = false;
    {
      std::unique_lock<std::mutex> lk(mu_);
      async_cv_.wait_for(lk, async_interval_ms_, [&] {
        return stop_.load(std::memory_order_acquire) || writes_since_fsync_ > 0;
      });
      if (stop_.load(std::memory_order_acquire)) {
        break;
      }
      if (writes_since_fsync_ > 0) {
        dirty = true;
        writes_since_fsync_ = 0;
      }
    }
    if (dirty) {
      do_fsync();
    }
  }
}

}  // namespace log_storage
