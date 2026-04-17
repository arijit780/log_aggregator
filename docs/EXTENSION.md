# Extending the log (formats, storage, durability)

## Layout (repository)

| Area | Purpose |
|------|---------|
| `include/log_storage/format/` | **Pluggable framing**: `IRecordCodec`, decode status, V1 constants + `V1BinaryCodec`. |
| `include/log_storage/io/` | Raw POSIX byte I/O helpers (`read_exact`, `write_all`, LE pack). |
| `include/log_storage/crypto/` | Hashing used by codecs (CRC32 today). |
| `include/log_storage/recovery/` | Authoritative scan + truncate; takes **`IRecordCodec const&`**. |
| `include/log_storage/durability/` | SYNC/ASYNC `fsync` policy (orthogonal to record shape). |
| `include/log_storage/writer/` | `LogWriter`, `DurableLogWriter` (inject codec / durability config). |
| `include/log_storage/reader/` | `LogReader` (inject same codec used to write the file). |

## Adding a new record format (e.g. V2)

1. Implement **`IRecordCodec`** in e.g. `include/log_storage/format/v2_foo_codec.hpp` (+ `.cpp`).
2. Ensure **`encode`** and **`try_decode_one_record`** agree on byte layout and failure semantics (stop at first invalid record; no resync).
3. Pass a **stable** instance to **`LogWriter(path, &my_codec)`**, **`LogReader(path, &my_codec)`**, and **`RecoveryManager::recover(fd, my_codec)`** (or `DurableLogWriter` with `DurabilityManager::Config::record_codec = &my_codec`).
4. **Never** mix codecs on the same file: readers and recovery must use the codec that produced the bytes.

## Swapping durability policy

Today **`DurabilityManager::Config`** selects **Sync** vs **Async** batching. A different policy (e.g. group size tied to bytes instead of record count) would add a new class behind a small interface in `durability/` and construct it from `DurableLogWriter` — the record codec stays independent.

## Swapping storage backend

The codebase assumes a **single POSIX file** and `int` fd. A future **segment store** would introduce `ISegmentSink` (open, append bytes, fsync, path) and adapt `LogWriter` / recovery to iterate segments; keep **`IRecordCodec`** per segment or global per deployment.
