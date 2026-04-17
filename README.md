# log_aggregator

C++17 **crash-safe append-only log** storage: one file, monotonic offsets, **recovery by scanning and truncating** so the durable state is always a strict prefix of valid records. This is a storage layer (not a full Kafka-like broker).

## Build

Requires CMake 3.16+ and a C++17 toolchain.

```bash
cmake -S . -B build
cmake --build build
```

## Test

```bash
./build/crash_simulation
./build/durability_test
```

## Library overview

| Piece | Purpose |
|--------|--------|
| `LogWriter` | Open (with recovery), `append(payload) → offset` (no durability modes) |
| `DurableLogWriter` | Same + **SYNC** (ACK after `fsync`, group commit) or **ASYNC** (ACK after `write`, batched background `fsync`) |
| `DurabilityManager` | Used by `DurableLogWriter`; `DurabilityManager::Config` selects mode and batching |
| `LogReader` | Read records in order |
| `RecoveryManager` | Validate from byte 0, stop at first bad record, `ftruncate` tail |

**Module layout:** `include/log_storage/format/` (pluggable **`IRecordCodec`**, default **`V1BinaryCodec`**), `io/`, `crypto/`, `recovery/`, `durability/`, `writer/`, `reader/`. Sources mirror under `src/log_storage/`. Umbrella: **`#include "log_storage/log_storage.hpp"`**.

## More detail

- **How to extend (new format / backend):** [`docs/EXTENSION.md`](docs/EXTENSION.md)  
- **Architecture diagrams (Mermaid):** [`docs/architecture.md`](docs/architecture.md)  
- **Dense codebase map:** **`AGENTS.md`**

For editor/clangd: `.clangd` and `compile_flags.txt` add `-I include`.
