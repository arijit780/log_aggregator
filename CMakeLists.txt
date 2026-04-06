cmake_minimum_required(VERSION 3.16)
project(log_storage LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(log_storage
  src/crc32.cpp
  src/io_util.cpp
  src/record_layout.cpp
  src/recovery_manager.cpp
  src/log_writer.cpp
  src/log_reader.cpp
)
target_include_directories(log_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(crash_simulation tests/crash_simulation.cpp)
target_link_libraries(crash_simulation PRIVATE log_storage)

enable_testing()
add_test(NAME crash_simulation COMMAND crash_simulation)
