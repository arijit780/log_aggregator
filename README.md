# log_aggregator

C++17 **crash-safe append-only log** storage: segment-based files, CRC integrity, explicit durability modes, and consumer offset tracking. Recovery by scanning and truncating — the durable state is always a strict prefix of valid records.

## Build

Requires CMake 3.16+ and a C++17 toolchain.

```bash
cmake -S . -B build
cmake --build build
```

## Test

```bash
# Week 1-2: single-file log
./build/crash_simulation
./build/durability_test

# Week 3-4: segment-based log
./build/recovery_tests
./build/durability_tests
```

## Architecture

### Week 1-2 — Single-file log

| Component | Purpose |
|-----------|---------|
| `LogWriter` | Open (with recovery), `append(payload, DurabilityMode) → offset` — per-call mode |
| `DurabilityManager` | Batching `fsync` + group commit (Sync/Async/None) |
| `LogReader` | Sequential record reader |
| `RecoveryManager` | Validate from byte 0, stop at first bad record, `ftruncate` |

**Record format (V1):** `[MAGIC u32][OFFSET u64][LENGTH u64][PAYLOAD][CRC u32]`

### Week 3-4 — Segment-based log

| Component | Purpose |
|-----------|---------|
| `Log` | Segment-based: `append(payload) → byte_offset`, `read(offset, max_bytes) → vector<Record>` |
| `DurabilityGuard` | Config-level `Sync` or `Async` — no mixing |
| `recover_segment()` | Single function: scan, validate CRC, truncate at first invalid |
| `OffsetStore` | Consumer offset tracking: `(group, partition) → offset` in metadata log |

**Record format:** `[uint32 length][payload bytes][uint32 crc32]`

### Durability modes

| Mode | Behavior | Trade-off |
|------|----------|-----------|
| **Sync** | `write → fsync → return` | Highest durability, lowest throughput |
| **Async** | `write → return → background fsync` | Higher throughput, bounded data-loss window |

### Consumer offset store

- Key: `(group_id, partition) → byte_offset`
- Stored in a separate append-only metadata log (same `[length][payload][crc32]` framing)
- Recovery rebuilds state by replaying the metadata log (last-writer-wins)
- Commits are always fsynced (at-least-once delivery semantics)

## Module layout

```
include/log_storage/
  storage/          # Week 3-4: record, segment, recovery, durability, log
  consumer/         # Consumer offset tracking
  format/           # Week 1-2: pluggable codec (IRecordCodec, V1BinaryCodec)
  io/               # POSIX byte I/O helpers (shared)
  crypto/           # CRC32 (shared)
  writer/, reader/  # Week 1-2: LogWriter, LogReader
  durability/       # Week 1-2: DurabilityManager
  recovery/         # Week 1-2: RecoveryManager

src/log_storage/    # Implementation mirrors include/ layout

tests/
  crash_simulation.cpp   # Week 1-2
  durability_test.cpp    # Week 1-2
  recovery_tests.cpp     # Week 3-4
  durability_tests.cpp   # Week 3-4
```

## More detail

- **Architecture diagrams (Mermaid):** [`docs/architecture.md`](docs/architecture.md)
- **Dense codebase map:** **`AGENTS.md`**

For editor/clangd: `.clangd` and `compile_flags.txt` add `-I include`.
