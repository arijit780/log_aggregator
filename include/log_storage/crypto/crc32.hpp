#pragma once

#include <cstddef>
#include <cstdint>

namespace log_storage {

std::uint32_t crc32_compute(const void* data, std::size_t len);

}  // namespace log_storage
