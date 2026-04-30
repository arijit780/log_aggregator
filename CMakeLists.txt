cmake_minimum_required(VERSION 3.16)
project(log_storage LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Core library — weeks 1-2 (single-file log) + weeks 3-4 (segment-based log)
# ---------------------------------------------------------------------------
add_library(log_storage
  # Week 1-2: single-file log with pluggable codec
  src/log_storage/crypto/crc32.cpp
  src/log_storage/io/byte_io.cpp
  src/log_storage/format/v1_binary_codec.cpp
  src/log_storage/recovery/recovery_manager.cpp
  src/log_storage/writer/log_writer.cpp
  src/log_storage/writer/durable_log_writer.cpp
  src/log_storage/reader/log_reader.cpp
  src/log_storage/durability/durability_manager.cpp

  # Week 3-4: segment-based log with [length][payload][crc32] format
  src/log_storage/storage/record.cpp
  src/log_storage/storage/segment.cpp
  src/log_storage/storage/recovery.cpp
  src/log_storage/storage/durability.cpp
  src/log_storage/storage/log.cpp
  src/log_storage/consumer/offset_store.cpp
)
target_include_directories(log_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(log_storage PUBLIC Threads::Threads)

# ---------------------------------------------------------------------------
# Week 1-2 tests
# ---------------------------------------------------------------------------
add_executable(crash_simulation tests/crash_simulation.cpp)
target_link_libraries(crash_simulation PRIVATE log_storage)

add_executable(durability_test tests/durability_test.cpp)
target_link_libraries(durability_test PRIVATE log_storage)

# ---------------------------------------------------------------------------
# Week 3-4 tests
# ---------------------------------------------------------------------------
add_executable(recovery_tests tests/recovery_tests.cpp)
target_link_libraries(recovery_tests PRIVATE log_storage)

add_executable(durability_tests tests/durability_tests.cpp)
target_link_libraries(durability_tests PRIVATE log_storage)

# ---------------------------------------------------------------------------
# CTest registration
# ---------------------------------------------------------------------------
enable_testing()
add_test(NAME crash_simulation COMMAND crash_simulation)
add_test(NAME durability_test COMMAND durability_test)
add_test(NAME recovery_tests COMMAND recovery_tests)
add_test(NAME durability_tests COMMAND durability_tests)
