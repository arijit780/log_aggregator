#pragma once

namespace log_storage {

enum class RecordDecodeStatus {
  CleanEof,
  Invalid,
  Ok,
};

}  // namespace log_storage
