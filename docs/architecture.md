# Architecture (study guide)

Mermaid diagrams below render in GitHub, VS Code Markdown preview, and many other viewers.

## Suggested reading order

### Week 1-2 (single-file log)
1. `include/log_storage/format/irecord_codec.hpp` — pluggable framing contract
2. `include/log_storage/format/v1_constants.hpp` + `format/v1_binary_codec.hpp` — default on-disk layout
3. `src/log_storage/recovery/recovery_manager.cpp` — scan, stop at first invalid, `ftruncate`
4. `src/log_storage/writer/log_writer.cpp` — open → recover → append
5. `src/log_storage/durability/durability_manager.cpp` + `writer/durable_log_writer.cpp` — SYNC/ASYNC

### Week 3-4 (segment-based log)
1. `include/log_storage/storage/record.hpp` — wire format `[length][payload][crc32]`
2. `src/log_storage/storage/segment.cpp` — file-level operations
3. `src/log_storage/storage/recovery.cpp` — scan + truncate (single function)
4. `src/log_storage/storage/durability.cpp` — Sync / Async guard
5. `src/log_storage/storage/log.cpp` — segment management + append/read
6. `src/log_storage/consumer/offset_store.cpp` — `(group, partition) → offset`

## Week 1-2 components

```mermaid
flowchart TB
  subgraph app["App / tests"]
    T[tests/crash_simulation.cpp]
    DT[tests/durability_test.cpp]
  end

  subgraph public_api["Public API"]
    W[LogWriter]
    DM[DurabilityManager]
    R[LogReader]
    M[RecoveryManager]
  end

  subgraph record["Format layer"]
    IC[IRecordCodec]
    V1[V1BinaryCodec]
  end

  subgraph sys["I/O and hashing"]
    IO[io/byte_io]
    C[crypto/crc32]
  end

  subgraph disk["OS"]
    FS[(Log file)]
  end

  T --> W
  T --> R
  DT --> W
  W --> DM
  W --> M
  W --> IC
  R --> IC
  M --> IC
  V1 -.-> IC
  W --> IO
  R --> IO
  M --> IO
  V1 --> IO
  V1 --> C
  W --> FS
  R --> FS
  M --> FS
  DM --> IC
  DM --> IO
  DM --> FS
```

## Week 3-4 components

```mermaid
flowchart TB
  subgraph tests["Tests"]
    RT[tests/recovery_tests.cpp]
    DTT[tests/durability_tests.cpp]
  end

  subgraph api["Segment-based API"]
    LOG[Log]
    DG[DurabilityGuard]
    REC[recover_segment]
  end

  subgraph record["Record layer"]
    ENC[encode_record]
    DEC[decode_record]
  end

  subgraph consumer["Consumer"]
    OS[OffsetStore]
  end

  subgraph shared["Shared utilities"]
    IO[io/byte_io]
    CRC[crypto/crc32]
  end

  subgraph disk["OS"]
    SEG[(Segment files)]
    META[(Metadata log)]
  end

  RT --> LOG
  RT --> OS
  DTT --> LOG
  LOG --> DG
  LOG --> REC
  LOG --> ENC
  LOG --> DEC
  REC --> DEC
  OS --> ENC
  OS --> DEC
  ENC --> CRC
  ENC --> IO
  DEC --> CRC
  DEC --> IO
  LOG --> SEG
  OS --> META
  DG --> SEG
  REC --> SEG
```

## Week 3-4 on-disk record format

```mermaid
flowchart LR
  LEN["uint32 length<br/>(LE)"] --> PL["payload<br/>(length bytes)"] --> CRC["uint32 crc32<br/>(LE)"]
```

CRC covers `length_bytes ‖ payload_bytes` — protects both the length value and payload content.

## Segment-based log lifecycle

```mermaid
sequenceDiagram
  participant App
  participant Log
  participant Segment
  participant Recovery
  participant DurabilityGuard
  participant Disk

  App->>Log: Log(config)
  Log->>Disk: scan dir for .seg files
  Log->>Segment: segment_open(last)
  Log->>Recovery: recover_segment(seg)
  Recovery->>Segment: segment_read_at(0, all)
  Recovery->>Recovery: decode records in loop
  Recovery->>Segment: segment_truncate(valid_pos)
  Recovery-->>Log: RecoveryResult
  Log->>DurabilityGuard: new(fd, config)

  App->>Log: append(payload)
  Log->>Log: encode_record(payload)
  Log->>Segment: segment_append(wire)
  Log->>DurabilityGuard: after_write()
  Note over DurabilityGuard: Sync: fsync now<br/>Async: mark dirty
  DurabilityGuard-->>Log: return
  Log-->>App: byte_offset
```

## Consumer offset store lifecycle

```mermaid
sequenceDiagram
  participant App
  participant OffsetStore
  participant Disk

  App->>OffsetStore: OffsetStore(path)
  OffsetStore->>Disk: open metadata log
  OffsetStore->>OffsetStore: recover(): scan all records
  Note over OffsetStore: Rebuild (group,partition)→offset map<br/>Last-writer-wins

  App->>OffsetStore: commit("group-a", 0, 1500)
  OffsetStore->>OffsetStore: encode offset entry as record
  OffsetStore->>Disk: write + fsync
  Note over OffsetStore: Crash before fsync = commit lost<br/>(at-least-once semantics)
  OffsetStore-->>App: return

  App->>OffsetStore: get("group-a", 0)
  OffsetStore-->>App: 1500
```

## Segment rolling

```mermaid
flowchart LR
  A["active segment<br/>size > max"] --> B["shutdown DurabilityGuard<br/>(final fsync)"]
  B --> C["segment_create<br/>(new file)"]
  C --> D["new DurabilityGuard<br/>(new fd)"]
```

Crash between B and C: old segment fsynced, no new file. Safe.
Crash during write to new segment: recovery truncates partial tail. Safe.
