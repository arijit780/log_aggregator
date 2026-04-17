#pragma once

#include "log_storage/durability/durability_manager.hpp"
#include "log_storage/durability/durability_mode.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace log_storage {

class DurableLogWriter {
 public:
  explicit DurableLogWriter(std::string path, DurabilityManager::Config config);
  DurableLogWriter(const DurableLogWriter&) = delete;
  DurableLogWriter& operator=(const DurableLogWriter&) = delete;
  DurableLogWriter(DurableLogWriter&& other) noexcept;
  DurableLogWriter& operator=(DurableLogWriter&& other) noexcept;
  ~DurableLogWriter();

  std::uint64_t append(const void* payload, std::size_t len);
  std::uint64_t append(const std::vector<std::uint8_t>& payload);

  std::uint64_t next_offset() const;
  std::uint64_t fsync_count() const;

 private:
  std::string path_;
  int fd_ = -1;
  std::unique_ptr<DurabilityManager> dm_;
};

}  // namespace log_storage
