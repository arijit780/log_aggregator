# log_aggregator — dense codebase map (@ this file to load context cheaply)

**Stack:** C++17, POSIX I/O + `fsync`, `std::thread` / condition variables, namespace `log_storage`. **Include:** `-Iinclude` → `#include "log_storage/…"`. **IDE:** `.clangd` + `compile_flags.txt`. **CMake:** `log_storage` (Threads::Threads), `crash_simulation`, `durability_test`, `recovery_tests`, `durability_tests` (`CMAKE_EXPORT_COMPILE_COMMANDS ON`).

## Layout

### Week 1-2: Single-file log with pluggable codec

| Layer | Path | Plug in |
|-------|------|---------|
| **Format** | `include/log_storage/format/` | `IRecordCodec` — implement `encode` + `try_decode_one_record`; default `V1BinaryCodec` + `default_v1_binary_codec()`. |
| **I/O** | `include/log_storage/io/` | `byte_io.hpp` — `read_exact`, `write_all`, LE pack/unpack. |
| **Crypto** | `include/log_storage/crypto/` | `crc32.hpp` — used by codecs and segment record format. |
| **Recovery** | `include/log_storage/recovery/` | `RecoveryManager::recover(fd)` or `recover(fd, codec)`. |
| **Durability** | `include/log_storage/durability/` | `DurabilityMode`, `DurabilityManager` + `Config`. |
| **Writer / Reader** | `include/log_storage/writer/`, `reader/` | `LogWriter`, `LogReader` — `LogWriter::append(payload, DurabilityMode)` (per-call). |
| **Umbrella** | `include/log_storage/log_storage.hpp` | Single include for public API. |

### Week 3-4: Segment-based log with consumer offset tracking

| Layer | Path | Role |
|-------|------|------|
| **Record** | `storage/record.hpp/.cpp` | Wire format `[uint32 length][payload][uint32 crc32]`. `encode_record`, `decode_record`. |
| **Segment** | `storage/segment.hpp/.cpp` | Single segment file. `segment_open`, `segment_create`, `segment_append`, `segment_read_at`, `segment_fsync`, `segment_truncate`, `segment_close`. |
| **Recovery** | `storage/recovery.hpp/.cpp` | `recover_segment(seg)` — scan, validate CRC, truncate at first invalid. Single function. |
| **Durability** | `storage/durability.hpp/.cpp` | `SyncMode::Sync` / `SyncMode::Async`. `DurabilityGuard` — config-level choice, no mixing. |
| **Log** | `storage/log.hpp/.cpp` | `Log` class — segment-based. `append(payload) → offset`, `read(offset, max_bytes) → vector<Record>`. |
| **Consumer** | `consumer/offset_store.hpp/.cpp` | `OffsetStore` — `(group, partition) → offset` in separate append-only metadata log. Rebuild from disk on recovery. |

## On-disk formats

### V1 (week 1-2): `[MAGIC u32][OFFSET u64][LENGTH u64][PAYLOAD ×LENGTH][CRC u32]`
CRC over `OFFSET‖LENGTH‖PAYLOAD`. Strict offset chain.

### Segment (week 3-4): `[uint32 length][payload bytes][uint32 crc32]`
CRC over `length_bytes‖payload`. No magic, no logical offset in the record. Offset tracking is by byte position.

## Recovery (week 3-4)
`recover_segment(seg)`: read full segment → decode records in linear scan → on first `Truncated` or `Corrupt` → `ftruncate`. Idempotent.

## Durability (week 3-4)
**Sync:** `after_write()` calls `fsync` inline. **Async:** background thread fsyncs periodically (`async_interval_ms`). `DurabilityConfig::fsync_hook` for testing.

## Consumer offset store (week 3-4)
Metadata log uses same `[length][payload][crc32]` framing. Payload: `[uint32 group_len][group_bytes][uint32 partition][uint64 offset]`. Last-writer-wins. `commit()` always fsyncs. At-least-once semantics.

## Invariants (must hold)
- After recovery, every segment is a strict prefix of valid records.
- No skipping damaged records.
- Writer never bumps offset before successful `write_all`.
- Sync: fsync completes before `append()` returns.
- Async: explicit data-loss window bounded by `async_interval_ms`.

## Build / run
```bash
cmake -S . -B build && cmake --build build
./build/crash_simulation && ./build/durability_test        # week 1-2
./build/recovery_tests && ./build/durability_tests         # week 3-4
```

## File inventory
| Path | Role |
|------|------|
| `format/irecord_codec.hpp` | Pluggable encode/decode interface (week 1-2) |
| `format/v1_binary_codec.hpp/.cpp` | Default codec (week 1-2) |
| `format/v1_constants.hpp` | V1 MAGIC, sizes, caps |
| `format/record_decode_status.hpp` | `RecordDecodeStatus` (week 1-2) |
| `io/byte_io.hpp/.cpp` | POSIX byte I/O + LE (shared) |
| `crypto/crc32.hpp/.cpp` | CRC32 (shared) |
| `recovery/recovery_manager.hpp/.cpp` | Single-file scan + truncate (week 1-2) |
| `writer/log_writer.hpp/.cpp` | Single writer API (week 1-2) |
| `writer/durable_log_writer.hpp/.cpp` | Back-compat durable writer (week 1-2) |
| `durability/durability_mode.hpp` | `DurabilityMode` enum (week 1-2) |
| `durability/durability_manager.hpp/.cpp` | SYNC/ASYNC manager (week 1-2) |
| `reader/log_reader.hpp/.cpp` | Sequential read (week 1-2) |
| `storage/record.hpp/.cpp` | `[length][payload][crc32]` format (week 3-4) |
| `storage/segment.hpp/.cpp` | Segment file ops (week 3-4) |
| `storage/recovery.hpp/.cpp` | Segment recovery (week 3-4) |
| `storage/durability.hpp/.cpp` | `SyncMode` + `DurabilityGuard` (week 3-4) |
| `storage/log.hpp/.cpp` | Segment-based `Log` class (week 3-4) |
| `consumer/offset_store.hpp/.cpp` | `(group,partition)→offset` tracking (week 3-4) |
| `log_storage.hpp` | Umbrella include |
| `tests/crash_simulation.cpp` | Crash test (week 1-2) |
| `tests/durability_test.cpp` | Durability test (week 1-2) |
| `tests/recovery_tests.cpp` | Recovery + CRC + garbage + partial write tests (week 3-4) |
| `tests/durability_tests.cpp` | Sync/Async + crash + hook tests (week 3-4) |
