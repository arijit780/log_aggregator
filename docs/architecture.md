# Architecture (study guide)

Mermaid diagrams below render in GitHub, VS Code Markdown preview, and many other viewers.

## Suggested reading order

1. `include/log_storage/record_format.hpp` — layout, constants, design rationale  
2. `src/record_layout.cpp` — `encode_record` / `try_decode_one_record` (single validation path)  
3. `src/recovery_manager.cpp` — scan, stop at first invalid, `ftruncate`  
4. `src/log_writer.cpp` — open → recover → append  
5. `src/log_reader.cpp` — sequential read, same checks, no truncate  

## Components and dependencies

```mermaid
flowchart TB
  subgraph app["App / tests"]
    T[tests/crash_simulation.cpp]
  end

  subgraph public_api["Public API"]
    W[LogWriter]
    R[LogReader]
    M[RecoveryManager]
  end

  subgraph record["Record layer"]
    L[record_layout: encode_record, try_decode_one_record]
    F[record_format: MAGIC, sizes, caps]
  end

  subgraph sys["I/O and hashing"]
    IO[io_util]
    C[crc32]
  end

  subgraph disk["OS"]
    FS[(Log file)]
  end

  T --> W
  T --> R
  W --> M
  W --> L
  W --> IO
  R --> L
  R --> IO
  M --> L
  L --> F
  L --> IO
  L --> C
  W --> FS
  R --> FS
  M --> FS
```

## Append vs recovery vs read

```mermaid
flowchart LR
  subgraph write["Append optimistic"]
    A1[next_offset_] --> A2[encode_record]
    A2 --> A3[write_all]
    A3 --> A4[++next_offset_]
  end

  subgraph recover["Recovery authoritative"]
    B1[lseek 0] --> B2[decode loop]
    B2 --> B3[CleanEof or Invalid: stop]
    B3 --> B4[ftruncate last_valid]
    B4 --> B5[next_offset = count]
  end

  subgraph readpath["Read no truncate"]
    C1[try_decode_one_record] --> C2[Ok: payload]
    C1 --> C3[Invalid: throw]
  end
```

## On-disk record (strict byte order, little-endian scalars)

```mermaid
flowchart LR
  MG[MAGIC u32] --> OF[OFFSET u64] --> LN[LENGTH u64] --> PL[PAYLOAD × LENGTH] --> CR[CRC u32]
```

## Open writer: recovery always runs first

```mermaid
sequenceDiagram
  participant App
  participant LogWriter
  participant Recovery
  participant Decode as try_decode_one_record
  participant Disk

  App->>LogWriter: open path
  LogWriter->>Disk: open O_RDWR CREAT
  LogWriter->>Recovery: recover fd
  Recovery->>Disk: lseek 0
  loop each record
    Recovery->>Decode: expected offset
    Decode->>Disk: read validate
    Decode-->>Recovery: Ok / CleanEof / Invalid
  end
  Recovery->>Disk: ftruncate last_valid
  Recovery-->>LogWriter: next_offset
  App->>LogWriter: append payload
  LogWriter->>Disk: write full record
```
