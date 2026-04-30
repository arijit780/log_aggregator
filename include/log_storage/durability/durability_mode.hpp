#pragma once

namespace log_storage {

enum class DurabilityMode {
  None,
  Sync,
  Async,
};
}  // namespace log_storage
