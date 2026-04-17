# log_aggregator — dense codebase map (@ this file to load context cheaply)

**Stack:** C++17, POSIX I/O + `fsync`, `std::thread` / condition variables, namespace `log_storage`. **Include:** `-Iinclude` → `#include "log_storage/…"`. **IDE:** `.clangd` + `compile_flags.txt`. **CMake:** `log_storage` (Threads::Threads), `crash_simulation`, `durability_test` (`CMAKE_EXPORT_COMPILE_COMMANDS ON`).

## Layout (extensibility)
| Layer | Path | Plug in |
|-------|------|---------|
| **Format** | `include/log_storage/format/` | `IRecordCodec` — implement `encode` + `try_decode_one_record`; default `V1BinaryCodec` + `default_v1_binary_codec()`. |
| **I/O** | `include/log_storage/io/` | `byte_io.hpp` — `read_exact`, `write_all`, LE pack/unpack. |
| **Crypto** | `include/log_storage/crypto/` | `crc32.hpp` — used by codecs. |
| **Recovery** | `include/log_storage/recovery/` | `RecoveryManager::recover(fd)` or `recover(fd, codec)` — must use same codec as writer. |
| **Durability** | `include/log_storage/durability/` | `DurabilityMode`, `DurabilityManager` + `Config` (`record_codec` optional ptr). |
| **Writer / Reader** | `include/log_storage/writer/`, `reader/` | `LogWriter`, `DurableLogWriter`, `LogReader` — optional `IRecordCodec const*` ctor arg (`optional_codec`); default V1. |
| **Umbrella** | `include/log_storage/log_storage.hpp` | Single include for public API. |
| **How to extend** | `docs/EXTENSION.md` | New formats, backends, durability policies. |

## Non-goals (scope)
Full broker, transactions, replication, consumer groups. No Python runtime in-tree.

## On-disk record — V1 (`format/v1_constants.hpp` + `V1BinaryCodec`)
`[MAGIC u32][OFFSET u64][LENGTH u64][PAYLOAD ×LENGTH][CRC u32]` (LE scalars). CRC over `OFFSET‖LENGTH‖PAYLOAD` bytes. **`kMaxPayloadBytes`** = 16MiB.

**Integrity:** MAGIC + OFFSET chain + CRC (CRC alone insufficient — see design notes in `docs/EXTENSION.md` / git history).

## Decode path (`IRecordCodec::try_decode_one_record`)
Same flow as before: MAGIC → OFFSET/LENGTH → payload → CRC; `CleanEof` | `Invalid` | `Ok`. First failure **Invalid**; recovery truncates at last good end.

## Recovery (`recovery/recovery_manager.cpp`)
`recover(fd)` delegates to **`recover(fd, default_v1_binary_codec())`**. Loop uses **`codec.try_decode_one_record`**; then **`ftruncate`**, `lseek` END.

## Writer (`writer/log_writer.cpp`)
`open` → **`recover(fd_, codec())`** → append uses **`codec().encode`** + **`write_all`**. Ctor param **`optional_codec`** (must not shadow method `codec()` — use distinct name).

## Durability (`durability/durability_manager.cpp`)
**Sync:** write under mutex → `promise` queue → worker batch `fsync` → `set_value`. **Async:** return after write; background `fsync`. **`Config::fsync_hook`** for tests (call real `fsync` inside).

## Reader (`reader/log_reader.cpp`)
Uses injected **`codec().try_decode_one_record`**; no truncate.

## File inventory
| Path | Role |
|------|------|
| `format/irecord_codec.hpp` | Pluggable encode/decode interface |
| `format/record_decode_status.hpp` | `RecordDecodeStatus` |
| `format/v1_constants.hpp` | V1 MAGIC, sizes, caps |
| `format/v1_binary_codec.hpp` / `src/log_storage/format/v1_binary_codec.cpp` | Default codec |
| `io/byte_io.hpp` / `src/log_storage/io/byte_io.cpp` | POSIX byte I/O + LE |
| `crypto/crc32.hpp` / `src/log_storage/crypto/crc32.cpp` | CRC32 |
| `recovery/recovery_manager.hpp` / `src/log_storage/recovery/recovery_manager.cpp` | Scan + truncate |
| `writer/log_writer.hpp` / `src/log_storage/writer/log_writer.cpp` | Optimistic append |
| `writer/durable_log_writer.hpp` / `src/log_storage/writer/durable_log_writer.cpp` | Recovery + durability |
| `durability/durability_mode.hpp`, `durability/durability_manager.hpp` / `src/.../durability_manager.cpp` | SYNC/ASYNC |
| `reader/log_reader.hpp` / `src/log_storage/reader/log_reader.cpp` | Sequential read |
| `log_storage.hpp` | Umbrella include |
| `tests/crash_simulation.cpp`, `tests/durability_test.cpp` | Tests |
| `CMakeLists.txt` | `find_package(Threads)`, `src/log_storage/**` sources |

## Build / run
```bash
cmake -S . -B build && cmake --build build && ./build/crash_simulation && ./build/durability_test
```

## Invariants (must hold)
Strict prefix after recovery; deterministic scan; no skipping damaged records; writer never bumps offset before successful `write_all`. **Sync:** no `set_value` until after batch `fsync`.
