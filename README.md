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
```

## Library overview

| Piece | Purpose |
|--------|--------|
| `LogWriter` | Open (with recovery), `append(payload) → offset` |
| `LogReader` | Read records in order |
| `RecoveryManager` | Validate from byte 0, stop at first bad record, `ftruncate` tail |

Headers live under `include/log_storage/`, sources under `src/`.

## Python (Week 2 — durability / ACK)

Same on-disk record layout as the C++ library. From repo root:

```bash
PYTHONPATH=. python3 -m unittest tests.test_week2_durability -v
```

See `wal_py/durability_manager.py` (SYNC vs ASYNC, group commit) and `wal_py/durable_wal.py`.

## More detail

- **Architecture diagrams (Mermaid):** [`docs/architecture.md`](docs/architecture.md)  
- **Dense codebase map:** **`AGENTS.md`**

For editor/clangd: `.clangd` and `compile_flags.txt` add `-I include`.
