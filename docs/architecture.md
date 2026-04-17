# Architecture (study guide)

Mermaid diagrams below render in GitHub, VS Code Markdown preview, and many other viewers.

## Suggested reading order

1. `include/log_storage/format/irecord_codec.hpp` — pluggable framing contract  
2. `include/log_storage/format/v1_constants.hpp` + `format/v1_binary_codec.hpp` — default on-disk layout  
3. `src/log_storage/recovery/recovery_manager.cpp` — scan, stop at first invalid, `ftruncate`  
4. `src/log_storage/writer/log_writer.cpp` — open → recover → append  
5. `src/log_storage/durability/durability_manager.cpp` + `writer/durable_log_writer.cpp` — Week 2 SYNC/ASYNC  
6. `src/log_storage/reader/log_reader.cpp` — sequential read  

See **`docs/EXTENSION.md`** for adding a new codec or backend.

## Components and dependencies

```mermaid
flowchart TB
  subgraph app["App / tests"]
    T[tests/crash_simulation.cpp]
    DT[tests/durability_test.cpp]
  end

  subgraph public_api["Public API"]
    W[LogWriter]
    DW[DurableLogWriter]
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
  DT --> DW
  DW --> DM
  DW --> M
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
  DW --> FS
```

## Append vs recovery vs read

```mermaid
flowchart LR
  subgraph write["Append optimistic"]
    A1[next_offset_] --> A2[IRecordCodec::encode]
    A2 --> A3[write_all]
    A3 --> A4[++next_offset_]
  end

  subgraph recover["Recovery authoritative"]
    B1[lseek 0] --> B2[codec.try_decode loop]
    B2 --> B3[CleanEof or Invalid: stop]
    B3 --> B4[ftruncate last_valid]
    B4 --> B5[next_offset = count]
  end

  subgraph readpath["Read no truncate"]
    C1[codec.try_decode_one_record] --> C2[Ok: payload]
    C1 --> C3[Invalid: throw]
  end
```

## On-disk record (V1, strict byte order, little-endian scalars)

```mermaid
flowchart LR
  MG[MAGIC u32] --> OF[OFFSET u64] --> LN[LENGTH u64] --> PL[PAYLOAD × LENGTH] --> CR[CRC u32]
```

## Open DurableLogWriter: recovery then durability threads

```mermaid
sequenceDiagram
  participant App
  participant DurableLogWriter
  participant Recovery
  participant Codec as IRecordCodec
  participant Disk

  App->>DurableLogWriter: open path + config
  DurableLogWriter->>Disk: open O_RDWR CREAT
  DurableLogWriter->>Recovery: recover fd optional codec
  Recovery->>Disk: lseek 0
  loop each record
    Recovery->>Codec: try_decode_one_record
    Codec->>Disk: read validate
    Codec-->>Recovery: Ok / CleanEof / Invalid
  end
  Recovery->>Disk: ftruncate last_valid
  Recovery-->>DurableLogWriter: next_offset
  App->>DurableLogWriter: append payload
  DurableLogWriter->>Disk: write full record
```
