# log_aggregator — dense codebase map (@ this file to load context cheaply)

**Stack:** C++17, POSIX `open/read/write/lseek/ftruncate`, namespace `log_storage`. **Include:** `-Iinclude` → `#include "log_storage/…"`. **IDE:** `.clangd` + `compile_flags.txt`. **CMake:** lib `log_storage` + exe `crash_simulation` (`CMAKE_EXPORT_COMPILE_COMMANDS ON`).

## Non-goals (not in repo)
Txn/WAL/commit, consumers, replication, partitions, `fsync` policy — storage + recovery only.

## On-disk record (little-endian)
`[MAGIC u32][OFFSET u64][LENGTH u64][PAYLOAD ×LENGTH][CRC u32]`  
- `MAGIC = 0x0A15E10D` (`kRecordMagic`, note LE on disk).  
- `OFFSET` = 0,1,2… logical id; must equal scanner’s `expected` or **Invalid**.  
- `LENGTH` ≤ `kMaxPayloadBytes` (=16MiB) or **Invalid**.  
- **CRC32** (`crc32.cpp`, poly IEEE Ethernet style): over bytes = `pack_le_64(OFFSET)‖pack_le_64(LENGTH)‖PAYLOAD` (same layout as `encode_record` builds for CRC body).  
- Overhead: `kRecordOverhead = 4+8+8+4` + payload.

**Integrity:** MAGIC = record boundary; OFFSET = global chain (no skip/resync); CRC = bytes for that triple. CRC-only bad: wrong start byte can yield wrong triple that still “checks” — need MAGIC+OFFSET. Long rationale: `record_format.hpp` comments.

## Single decode path (`record_layout.cpp`)
`try_decode_one_record(fd, expected_offset, end_file_offset, payload_out|null)` → `CleanEof` | `Invalid` | `Ok`.  
Flow: read 4 MAGIC (`read` once; `0` bytes ⇒ `CleanEof`; `<4` ⇒ `Invalid`); MAGIC match; `read_exact` OFFSET, LENGTH; `rec_off==expected`; LENGTH bound; `read_exact` payload; `read_exact` CRC; recompute CRC vs file. On `Ok`, `end_file_offset` = `lseek(SEEK_CUR)`.  
Failure modes (torn header/payload/CRC, garbage tail, misalign): first failed check **Invalid**; caller uses last good `end_pos` and truncates.

## Recovery (`recovery_manager.cpp`)
`RecoveryManager::recover(fd)`: `lseek` 0; `expected=0`, `last_valid=0`; loop `try_decode_one_record(..., nullptr)`; on `Ok` set `last_valid=end_pos`, `expected++`; on `CleanEof`/`Invalid` break; **`ftruncate(fd, last_valid)`**; `lseek` END; return `{next_offset=expected, truncated_bytes=last_valid}`. Throws on `lseek`/`ftruncate` error.

## Writer (`log_writer.cpp` / `.hpp`)
Ctor: `O_RDWR|O_CREAT`, then **`recover(fd)`**, `next_offset_=result.next_offset`.  
`append(payload,len)`: reject `len>kMaxPayloadBytes`; `assigned=next_offset_`; `encode_record` → **`write_all`** full buffer; **then** `++next_offset_`. Optimistic: successful return ≠ durable until OS/storage says so; reopen may truncate suffix.

## Reader (`log_reader.cpp` / `.hpp`)
`O_RDONLY`, `lseek` 0, `expected_offset_=0`.  
`read_next(payload)`: `try_decode_one_record(..., &payload)`; `CleanEof`→false; `Invalid`→throw; else `++expected_offset_` (after decode, matches internal counter with record’s OFFSET) → true.

## IO / utils
`io_util.cpp`: `read_exact`, `write_all`, `pack_le_u32/u64`, `unpack_le_u32/u64`.

## File inventory
| Path | Role |
|------|------|
| `include/log_storage/record_format.hpp` | constants, size caps, long design comments |
| `include/log_storage/record_layout.hpp` | `encode_record`, `try_decode_one_record`, `RecordDecodeStatus` |
| `src/record_layout.cpp` | encode/decode impl |
| `include/log_storage/recovery_manager.hpp` | `RecoveryManager::recover` |
| `src/recovery_manager.cpp` | scan + truncate |
| `include/log_storage/log_writer.hpp` / `src/log_writer.cpp` | append + open recovery |
| `include/log_storage/log_reader.hpp` / `src/log_reader.cpp` | sequential read |
| `include/log_storage/io_util.hpp` / `src/io_util.cpp` | I/O + LE |
| `include/log_storage/crc32.hpp` / `src/crc32.cpp` | CRC32 |
| `tests/crash_simulation.cpp` | corrupt tail test; fork child appends, parent SIGKILL, reopen + verify contiguous offsets |
| `CMakeLists.txt` | targets + export compile DB |

## Build / run
```bash
cmake -S . -B build && cmake --build build && ./build/crash_simulation
```
Or one-shot: `c++ -std=c++17 -Wall -Wextra -Werror -I include src/crc32.cpp src/io_util.cpp src/record_layout.cpp src/recovery_manager.cpp src/log_writer.cpp src/log_reader.cpp tests/crash_simulation.cpp -o crash_simulation`

## Invariants (must hold)
Strict prefix after recovery; deterministic scan (same file → same prefix); no skipping damaged records; writer never bumps offset before successful `write_all`.
