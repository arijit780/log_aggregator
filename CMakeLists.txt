cmake_minimum_required(VERSION 3.16)
project(log_storage LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

add_library(log_storage
  src/log_storage/crypto/crc32.cpp
  src/log_storage/io/byte_io.cpp
  src/log_storage/format/v1_binary_codec.cpp
  src/log_storage/recovery/recovery_manager.cpp
  src/log_storage/writer/log_writer.cpp
  src/log_storage/writer/durable_log_writer.cpp
  src/log_storage/reader/log_reader.cpp
  src/log_storage/durability/durability_manager.cpp
)
target_include_directories(log_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(log_storage PUBLIC Threads::Threads)

add_executable(crash_simulation tests/crash_simulation.cpp)
target_link_libraries(crash_simulation PRIVATE log_storage)

add_executable(durability_test tests/durability_test.cpp)
target_link_libraries(durability_test PRIVATE log_storage)

enable_testing()
add_test(NAME crash_simulation COMMAND crash_simulation)
add_test(NAME durability_test COMMAND durability_test)
