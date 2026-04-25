<!--
  Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements.  See the NOTICE file distributed with this work for
  additional information regarding copyright ownership.  The ASF licenses this
  file to you under the Apache License, Version 2.0 (the "License"); you may
  not use this file except in compliance with the License.  You may obtain a
  copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY FOR PARTICULAR USE.
-->

# Codec Pipeline Framework — Forward Index Format V7

**Status**: Implemented (PR [#18229](https://github.com/apache/pinot/pull/18229))
**Module**: `pinot-segment-spi/codec`, `pinot-segment-local/io/codec`, `pinot-segment-local/.../FixedByteChunkForwardIndexWriterV7`, `FixedByteChunkSVForwardIndexReaderV7`
**Scope (v1)**: A single legacy-compatible compression invocation uses the existing raw
forward-index format. Specs that cannot map to one legacy `ChunkCompressionType` (any transform,
multiple compression stages, or non-default compression options) use V7 for single-value INT/LONG.
**Wire-format identifier**: version 7 plus the explicit codec-pipeline header magic

---

## 1. Goals and non-goals

### Goals
1. Replace the closed `FieldConfig.CompressionCodec` enum with an extensible **codec pipeline DSL** so new codecs (or new combinations) can be added without churning the public enum.
2. Introduce a **multi-stage pipeline** that lets users compose a transform (e.g. DELTA, DELTADELTA) with a compression codec (e.g. LZ4, ZSTD) — covering use cases the single-codec enum cannot express cleanly.
3. Embed the **canonical codec spec** in V7 pipeline segment headers so readers can decode without out-of-band configuration; V7 segments are self-describing.
4. Preserve full **backward compatibility**: legacy `compressionCodec` continues to work; `codecSpec` is opt-in per column.
5. Provide a clear **migration path** from the legacy enum to the DSL.

### Non-goals (v1)
- Variable-width or multi-value V7 pipeline storage. Only single-stage compression specs that map to a legacy `ChunkCompressionType` use the existing raw writer formats; all V7-requiring specs are fixed-byte SV INT/LONG only.
- Streaming / dictionary-encoded indexes. `codecSpec` applies to **raw** forward indexes only.
- A SQL-visible codec function. The DSL lives in table config (`fieldConfigList[].indexes.forward.codecSpec`).
- Plugin-registered codecs. v1 ships with 8 built-in codecs; plugin registration is a v2 follow-up.

---

## 2. DSL grammar

```
spec        ::= invocation
              | "CODEC" "(" invocation ("," invocation)* ")"
invocation  ::= name
              | name "(" arg ("," arg)* ")"
name        ::= [A-Za-z_] [A-Za-z0-9_]*       (* ASCII only, locale-stable *)
arg         ::= [0-9]+                          (* unsigned integer; signs rejected *)
```

- Case-insensitive lookup (`zstd(3)` ≡ `ZSTD(3)`).
- The keyword `CODEC` is **permanently reserved** and may not be used as a codec name.
- Whitespace is allowed between tokens; not allowed inside identifiers or numbers.
- A pipeline must contain at least one stage.
- To bound parse work for table configs and untrusted segment headers, a spec is limited to 64 KiB,
  32 stages, 128 characters per identifier, 16 arguments per stage, and 32 digits per argument.
- A pipeline is a chain of the form **N typed-layout-preserving transforms → at most one packing transform → N compressions**:
  - **Typed-layout-preserving transforms** (`DELTA`, `DELTADELTA`) map a column-typed value array to a same-width value array (header-less passthrough; element type comes from the column context, value count from the buffer length). Any number may be chained, so `CODEC(DELTA, DELTADELTA, LZ4)` is valid.
  - **Packing transforms** (`T64`, `GORILLA`) emit a bit-packed, self-framed byte stream that is no longer a typed value array, so a packing transform must be the **last** transform — only compression stages may follow it (e.g. `CODEC(DELTA, T64, LZ4)`).
  - **Compression** stages (`LZ4`, `ZSTD`, `SNAPPY`, `GZIP`) are byte→byte; any number may follow the transforms (e.g. `CODEC(DELTA, LZ4, ZSTD(3))`).
  - The validator enforces this by tracking a "typed-value domain": a `TRANSFORM` may only appear while still in the typed domain (it cannot follow a packing transform or a compression stage). It runs at table-config validation time so bad configs do not reach ZooKeeper.
- **Evaluation order:** stages run **left-to-right on encode** and **right-to-left on decode**. For example, `CODEC(DELTA, T64, LZ4).encode(x) = LZ4.encode(T64.encode(DELTA.encode(x)))` and `decode(y) = DELTA.decode(T64.decode(LZ4.decode(y)))`.

### Examples

| DSL                                  | Stages                          | Notes                                   |
|--------------------------------------|---------------------------------|-----------------------------------------|
| `LZ4`                                | LZ4 (compression)               | Single-stage compression                |
| `ZSTD(3)`                            | ZSTD level 3 (compression)      | Compression with level argument         |
| `SNAPPY`                             | Snappy (compression)            |                                         |
| `GZIP`                               | GZIP / DEFLATE (compression)    | Slower than LZ4/ZSTD                    |
| `DELTA`                              | DELTA (transform)               | INT/LONG only, no compression           |
| `DELTADELTA`                         | DELTADELTA (transform)          | Second-order delta; good for timestamps |
| `CODEC(DELTA, LZ4)`                  | DELTA → LZ4                     | Common timestamp pipeline               |
| `CODEC(DELTA, ZSTD(3))`              | DELTA → ZSTD level 3            | Better ratio than LZ4 at higher CPU     |
| `CODEC(DELTADELTA, LZ4)`             | DELTADELTA → LZ4                | Best for monotonic timestamps           |
| `T64`                                | T64 bit-pack (transform)        | Frame-of-reference bit-packing on 64-value blocks; INT/LONG only |
| `CODEC(T64, LZ4)`                    | T64 → LZ4                       | Bit-pack then byte-compress             |
| `GORILLA`                            | Gorilla XOR (transform)         | XOR-delta bit-stream; INT/LONG only     |
| `CODEC(GORILLA, ZSTD(3))`            | Gorilla → ZSTD level 3          | XOR + entropy coding                    |
| `CODEC(DELTA, DELTADELTA, LZ4)`      | DELTA → DELTADELTA → LZ4        | Chained typed-layout transforms + compression |
| `CODEC(DELTA, T64, LZ4)`             | DELTA → T64 bit-pack → LZ4      | Delta then frame-of-reference pack then compress |
| `CODEC(DELTA, LZ4, ZSTD(3))`         | DELTA → LZ4 → ZSTD level 3      | Chained compressions after a transform  |

---

## 3. Architecture

```
                           pinot-segment-spi/codec/         (interfaces + AST)
                           +------------------------------+
                           | CodecKind   (TRANSFORM,       |
                           |              COMPRESSION)     |
                           | CodecOptions  (marker)        |
                           | CodecContext  (DataType ctx)  |
                           | CodecInvocation (name, args)  |
                           | CodecPipeline   (List<Inv>)   |
                           | CodecSpecParser (recursive    |
                           |   descent → CodecPipeline)    |
                           | CodecDefinition  (parseOpts,  |
                           |   canonicalize, kind)         |
                           | ChunkCodecHandler             |
                           |   extends CodecDefinition:    |
                           |     encode(opts, ctx, src)    |
                           |     decode(opts, ctx, src)    |
                           |     decodeInto(opts, ctx,     |
                           |                src, dst)      |
                           |     maxEncodedSize(opts, n)   |
                           |     requiresDirectDstBuffer() |
                           +-------------|----------------+
                                         |
                                         v
        pinot-segment-local/io/codec/   (concrete handlers + runtime)
        +------------------------------------------------------------+
        | DeltaCodecDefinition           (TRANSFORM, INT/LONG)       |
        | DeltaDeltaCodecDefinition      (TRANSFORM, INT/LONG)       |
        |   extends BaseDeltaCodecDefinition  (shared scaffold)      |
        | T64CodecDefinition             (packing TRANSFORM, INT/LONG)|
        | GorillaCodecDefinition         (packing TRANSFORM, INT/LONG)|
        | Lz4CodecDefinition             (COMPRESSION, lazy native)  |
        | ZstdCodecDefinition            (COMPRESSION, level 1..22)  |
        | SnappyCodecDefinition          (COMPRESSION)               |
        | GzipCodecDefinition            (COMPRESSION, direct        |
        |                                 ByteBuffer I/O)            |
        |                                                            |
        | CodecRegistry                  (immutable DEFAULT;         |
        |                                 mutable @VisibleForTesting)|
        | CodecPipelineValidator         (structural rules:          |
        |                                 N typed-layout transforms, |
        |                                 ≤1 packing transform,      |
        |                                 N compressions)            |
        | CodecPipelineExecutor          (binds pipeline → handlers; |
        |                                 thread-safe;                |
        |                                 encode/decode;              |
        |                                 getCanonicalSpec)           |
        | CodecBufferUtils               (toDirectBuffer helpers)    |
        +------------|-----------------------------------------------+
                     |
                     v
        +-----------------------------------------------------------+
        | FixedByteChunkForwardIndexWriterV7                         |
        |   - writes self-describing V7 segments (header embeds     |
        |     canonicalSpec)                                         |
        | FixedByteChunkSVForwardIndexReaderV7                       |
        |   - dispatched by factory on version=7 + format magic    |
        |   - reads canonicalSpec from header → builds executor     |
        |   - validates header bounds, monotonic chunk offsets      |
        | ForwardIndexCreatorFactory                                 |
        |   - one legacy-compatible compression → existing writers  |
        |   - every other codecSpec → V7 fixed-byte writer           |
        +-----------------------------------------------------------+
```

**Module placement rationale**: SPI types are pure interfaces + AST + parser (no JNI, no runtime state). Concrete codec handlers and registry/executor live in `pinot-segment-local` because they reference native libraries (LZ4/ZSTD/Snappy) and PinotDataBuffer — both of which are local-module dependencies.

---

## 4. On-disk format (V7)

The codec-pipeline V7 file is **self-describing**: every reader can determine the codec spec from the
file header alone. There is no out-of-band configuration required at read time. Version 7 was already
a valid legacy fixed-byte writer version, so the version field alone is not a format discriminator.
Pipeline V7 adds the explicit `0xC0DEC0DE` magic immediately after the version. The reader factory
uses only that stable marker to select the pipeline reader, whose constructor then validates every
remaining field; other fixed-byte versions `>= 4`, including legacy V7, continue to use the legacy
reader.

```
┌─────────────────────────────────────────────────────────────────────┐
│ FILE HEADER                                                         │
├─────────────────────────────────────────────────────────────────────┤
│ Offset  Field             Size    Notes                             │
│ ─────── ────────────────  ─────   ─────────────────────────────────  │
│   0     version           int(4)  = 7                                │
│   4     formatMagic       int(4)  = 0xC0DEC0DE                       │
│   8     numChunks         int(4)  ≥ 0                                │
│  12     numDocsPerChunk   int(4)  power of 2, ≥ 1                    │
│  16     sizeOfEntry       int(4)  4 (INT) or 8 (LONG)                │
│  20     totalDocs         int(4)  ≥ 0                                │
│  24     codecSpecLength   int(4)  > 0                                │
│  28     dataHeaderStart   int(4)  = 32 + codecSpecLength             │
│  32     codecSpec[]       byte[]  UTF-8 canonical DSL                │
│  X      chunkOffsets[]    long[8] absolute file offsets, monotonic   │
├─────────────────────────────────────────────────────────────────────┤
│ DATA SECTION (one entry per chunk)                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Y     encodedSize        int(4)                                    │
│  Y+4   decodedSize        int(4)                                    │
│  Y+8   payload            byte[encodedSize]  (codec output)         │
└─────────────────────────────────────────────────────────────────────┘

X = 32 + codecSpecLength
Y = chunkOffsets[i]
```

### Reader-side validation (corruption defense)

| Check                                                             | Failure                              |
|-------------------------------------------------------------------|--------------------------------------|
| `version == 7` and `formatMagic == 0xC0DEC0DE`                    | `IllegalArgumentException`           |
| `numChunks ≥ 0`                                                   | `IllegalArgumentException`           |
| `numDocsPerChunk` is a power of two                               | `IllegalArgumentException`           |
| `sizeOfEntry == storedType.size()`                                | `IllegalArgumentException`           |
| `totalDocs ≥ 0`                                                   | `IllegalArgumentException`           |
| `0 < codecSpecLength ≤ 64 KiB` and `dataHeaderStart == 32 + length` | `IllegalArgumentException`         |
| `dataHeaderStart` and chunk-offset table fit in buffer            | `IllegalArgumentException`           |
| `chunkOffsets[]` strictly monotonic and leave room for a header   | `IllegalArgumentException`           |
| Per-chunk `encodedSize ≥ 0` and fits before next chunk offset     | `IllegalStateException` ("corrupt")  |
| `decodedSize` exactly matches rows in that chunk × entry size     | `IllegalStateException` ("corrupt")  |
| Decoded chunks stay within 64 MiB, each encoded/intermediate bound within 128 MiB, and cumulative stage-output bounds within 256 MiB | `IOException` / `IllegalArgumentException` |
| Each codec consumes a complete, valid frame (including GZIP checksum/trailer) | `IOException` ("corrupt")       |

---

## 5. Codec catalog (built-in)

| Codec        | `CodecKind`  | Args         | Wire format                                                          | Notes                                                                    |
|--------------|--------------|--------------|----------------------------------------------------------------------|--------------------------------------------------------------------------|
| `DELTA`      | TRANSFORM    | none         | `[first:N][delta_i:N for i=1..count-1]` (header-less passthrough; type from column ctx, count from length) | Typed-layout-preserving (chainable); two's-complement wrap intentional, symmetric on decode (locked by tests) |
| `DELTADELTA` | TRANSFORM    | none         | `[first:N][firstDelta:N][dod_i:N for i=2..]` (header-less passthrough) | Typed-layout-preserving (chainable); same wrap semantics                   |
| `T64`        | TRANSFORM    | none         | `[flag:1B][count:4B]` + per-64-value block `[baseline:N][bitWidth:1B][packed:ceil(bitWidth*64/8)B]` | Frame-of-reference + bit-packing on fixed 64-value blocks                |
| `GORILLA`    | TRANSFORM    | none         | `[flag:1B][count:4B][first:N][bit-stream]`                            | XOR-delta with MSB-first bit-stream, reusing previous leading/width window when it fits |
| `LZ4`        | COMPRESSION  | none         | LZ4 length-prefixed                                                   | `LZ4Factory.fastestInstance()` lazy init via inner holder class           |
| `ZSTD`       | COMPRESSION  | `level` (int) | Zstd frame with embedded decompressedSize                             | Levels 1–22; default 3                                                    |
| `SNAPPY`     | COMPRESSION  | none         | xerial Snappy                                                         | JNI requires direct buffers (handled internally)                          |
| `GZIP`       | COMPRESSION  | none         | DEFLATE payload + 4-byte decompressed-size footer                     | ThreadLocal `Deflater`/`Inflater`; direct `ByteBuffer` input and output      |

**Frozen on-disk names**: All codec `NAME` constants (DELTA, DELTADELTA, T64, GORILLA, LZ4, ZSTD, SNAPPY, GZIP) and the keyword `CODEC` are part of the on-disk format contract and must never be changed.

**Rolling upgrade considerations**:

1. **The `codecSpec` property requires upgraded config consumers.** Pre-1.6 controllers, servers,
   minions, or external segment builders can reject the unknown nested JSON property before segment-format
   compatibility is relevant. Upgrade every component that validates or consumes the table config before
   enabling any `codecSpec`, including a legacy-format spec such as `LZ4` or `ZSTD(3)`.

2. **V7 segments are unreadable by pre-V7 servers.** Servers built before this change use
   `>= VERSION 4` to dispatch version 7 to the legacy fixed-byte reader. That reader encounters the
   negative pipeline magic where it expects `numChunks` and rejects or fails the segment load.
   **DO NOT enable a V7-requiring `codecSpec` on any column until every server in your fleet has
   been upgraded** to a build that includes V7 support. This includes transforms, compression
   chains, and compression options such as `ZSTD(5)`.

3. **Adding a new codec to `CodecRegistry.DEFAULT` is also a rolling-upgrade-sensitive change.** A server that does not know a given codec name cannot read segments encoded with it (lookup throws `IllegalArgumentException`). Operators must ensure every server in the fleet runs a build that registers the codec **before** enabling that codec on any column in table config.

4. **Rollback is one-way at the codec level.** Once a column has been written with a V7 segment, downgrading the server fleet requires (a) reverting the `codecSpec` config to a legacy `compressionCodec`, (b) reloading the segments via the new servers to convert them back to a legacy raw format (this is supported — see §7.3), and (c) only then downgrading servers. Skipping step (b) leaves V7 segments on disk that downgraded servers cannot read.

Future work (§12) covers automating these constraints via a controller-side gate.

---

## 6. Configuration

### Table config

`codecSpec` is configured under the modern `indexes.forward` block (it is **not** a top-level
`FieldConfig` field — that pattern is reserved for legacy settings like `compressionCodec`). A
top-level legacy `compressionCodec` and an `indexes.forward.codecSpec` are mutually exclusive:

```jsonc
{
  "fieldConfigList": [
    {
      "name": "ts",
      "encodingType": "RAW",
      "indexes": { "forward": { "codecSpec": "CODEC(DELTADELTA,LZ4)" } }
    },
    {
      "name": "userId",
      "encodingType": "RAW",
      "indexes": { "forward": { "codecSpec": "ZSTD(3)" } }
    },
    {
      "name": "eventName",
      "encodingType": "DICTIONARY"
    }
  ]
}
```

### Validation

`ForwardIndexType.validateCodecSpec` runs at table-config validation time (after `FieldIndexConfigsUtil` has resolved `noDictionaryColumns` / `noDictionaryConfig` overrides into the effective `ForwardIndexConfig`) and rejects:

- Non-RAW encoding type
- Spec parse failures (unknown codec, syntax error, unsigned-only argument violation)
- Structural pipeline errors (a transform after a packing transform or compression stage, or a second packing
  transform); multiple byte-compression stages are allowed after all transforms
- Specs that require the V7 codec-pipeline writer (any transform, or compression-only with non-default arguments like `ZSTD(5)`) on multi-value columns
- Specs that require the V7 codec-pipeline writer on non-INT/LONG stored types

Compression-only specs whose arguments map to a legacy `ChunkCompressionType` (`LZ4`, `SNAPPY`, `GZIP`, `ZSTD`/`ZSTD(3)`) use the existing raw forward-index writers and support any SV/MV + fixed/var-byte column.

### Builder API (programmatic)

`codecSpec` is set on the forward-index config, which is carried in `FieldConfig.indexes.forward`:

```java
// Build the indexes.forward JSON block with the codecSpec.
ObjectNode forward = JsonUtils.newObjectNode();
forward.put("codecSpec", "CODEC(DELTADELTA,LZ4)");
ObjectNode indexes = JsonUtils.newObjectNode();
indexes.set("forward", forward);

FieldConfig fc = new FieldConfig.Builder("ts")
    .withEncodingType(EncodingType.RAW)
    .withIndexes(indexes)
    .build();

// Equivalently, configure ForwardIndexConfig directly when building FieldIndexConfigs in code:
ForwardIndexConfig fwd = new ForwardIndexConfig.Builder(EncodingType.RAW)
    .withCodecSpec("CODEC(DELTADELTA,LZ4)")
    .build();
```

A top-level legacy `compressionCodec` cannot coexist with an `indexes.forward.codecSpec`; the
table-config validator rejects that combination.

### Choosing a codec

| Workload                                              | Recommended spec                |
|-------------------------------------------------------|----------------------------------|
| Monotonic timestamps (epoch ms / s)                   | `CODEC(DELTADELTA,LZ4)`          |
| Time-ordered counters                                  | `CODEC(DELTA,LZ4)`               |
| Approximately uniform random IDs                      | `LZ4` or `ZSTD(3)`               |
| Cold fixed-byte INT/LONG data with trend              | `CODEC(DELTA,ZSTD(8))`           |
| Compatibility with old SNAPPY/GZIP segments           | `SNAPPY` or `GZIP`               |
| Fastest decode, low compression ratio                 | `LZ4`                            |

---

## 7. User manual

### 7.1 Enabling `codecSpec` on a new column

1. Before enabling **any** `codecSpec`, upgrade every controller and tenant server, plus any minion or external segment builder that validates or consumes the table config, to a Pinot version that understands the field (≥ 1.6). Older components can reject the unknown JSON property even when the selected codec uses legacy segment bytes.
2. For a V7-requiring spec (a transform, multiple compression stages, or non-default options such as `ZSTD(5)`), also verify every server that can load the table's segments has a V7 reader.
3. Add `codecSpec` under the column's `indexes.forward` block. Do **not** also set a top-level `compressionCodec`; the table config validator will reject mutual presence.
4. Rebuild affected segments (push offline data or wait for realtime → committed transitions).
5. The single-stage legacy-compatible specs `LZ4`, `SNAPPY`, `GZIP`, and `ZSTD`/`ZSTD(3)` use existing raw forward-index formats. Every other valid spec uses V7 and embeds the canonical spec.

### 7.2 Migrating from the legacy `compressionCodec`

Two approaches:

**(a) Manual** — Edit table config; replace `compressionCodec` with the equivalent `codecSpec`:

| Legacy `compressionCodec` | Equivalent `codecSpec`        | Semantic note                                                |
|---------------------------|--------------------------------|---------------------------------------------------------------|
| `LZ4`                     | `LZ4`                          | Drop-in equivalent                                            |
| `ZSTANDARD`               | `ZSTD(3)`                      | Drop-in equivalent                                            |
| `SNAPPY`                  | `SNAPPY`                       | Drop-in equivalent                                            |
| `GZIP`                    | `GZIP`                         | Drop-in equivalent                                            |
| `DELTA`                   | `CODEC(DELTA,LZ4)`             | **Adds LZ4 byte compression**; not a byte-for-byte equivalent |
| `DELTADELTA`              | `CODEC(DELTADELTA,LZ4)`        | **Adds LZ4 byte compression**; not a byte-for-byte equivalent |
| `PASS_THROUGH`            | (no migration; keep legacy)    | No codec to apply                                             |
| `MV_ENTRY_DICT`, CLP family | (no migration; not in scope) |                                                                |

**(b) Programmatic** — Use the migration helper:

```java
// Schema-aware migration (preferred): compression-only specs migrate for all
// supported raw forward-index shapes; transform specs migrate only for SV INT/LONG.
TableConfig migrated = CompressionCodecMigrator.migrateTableConfig(tableConfig, schema);

// Type-agnostic (use only when no schema is available)
TableConfig migrated = CompressionCodecMigrator.migrateTableConfig(tableConfig);
```

`CompressionCodecMigrator` emits a `WARN` log when migrating `DELTA`/`DELTADELTA` because the new spec adds LZ4 byte compression. The log notes that existing segments will be rewritten on next reload.

### 7.3 Rolling back from `codecSpec`

If a table needs to be downgraded to a Pinot version that does **not** understand `codecSpec`, first replace affected configs with legacy `compressionCodec` values. Tables with V7-requiring specs also need their V7 segments rewritten before downgrade:

1. In table config, replace `codecSpec` with an equivalent legacy `compressionCodec` (e.g. `codecSpec="LZ4"` → `compressionCodec=LZ4`).
2. For V7-requiring specs, trigger a segment reload. `ForwardIndexHandler.shouldRewriteRawForwardIndex` detects the legacy revert and rewrites V7 segments to the legacy format.
3. Once all segments are rewritten, the cluster can be downgraded.

The helper `ForwardIndexHandler.isLegacyRevertTargetForFixedByteSv` identifies legitimate revert targets:
`PASS_THROUGH`, `SNAPPY`, `ZSTANDARD`, `LZ4`, `GZIP`, `DELTA`, `DELTADELTA` (CLP family is excluded — not applicable to fixed-byte SV).

### 7.4 Mixed-version cluster safety

There are two independent compatibility boundaries:

- **Table config:** `codecSpec` is a new JSON property. Before enabling any spec, upgrade every controller and tenant server, plus segment-building minions or clients that validate or consume the config, to a version that understands it. This applies even to `LZ4` or `ZSTD(3)`, whose segment bytes remain legacy-compatible.
- **Segment bytes:** V7 pipeline segments are **forward-only** and can only be read by Pinot ≥ 1.6. Before enabling a V7-requiring spec, verify with `kubectl get pods` (or equivalent) that every server able to load the table's segments is running a V7-capable binary.

If a V7-requiring `codecSpec` is enabled before all servers are upgraded, older servers dispatch
version 7 to the legacy fixed-byte reader, encounter the negative pipeline magic where they expect
`numChunks`, and fail the segment load while parsing the incompatible header.

### 7.5 Common errors and remediation

| Error message                                                                              | Remediation                                                  |
|--------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `Conflicting forward-index config for column: <col> — FieldConfig.compressionCodec=... but indexes.forward.codecSpec is also set` | Remove the legacy top-level `compressionCodec`; keep only `indexes.forward.codecSpec` |
| `codecSpec '...' requires the V7 codec-pipeline writer ... only supports single-value columns. Column 'X' is multi-value` | Use one legacy-compatible compression invocation or legacy `compressionCodec` |
| `codecSpec '...' requires the V7 codec-pipeline writer ... only supports INT and LONG columns. Column 'X' has type: ...`   | Use one legacy-compatible compression invocation or legacy `compressionCodec` |
| `Unknown codec 'XYZ'. Known codecs: [DELTA, DELTADELTA, T64, GORILLA, ZSTD, ZSTANDARD, LZ4, SNAPPY, GZIP]` | Fix typo in DSL; `ZSTANDARD` is an alias for `ZSTD`          |
| `Transform stage '<name>' must operate on column values ...`                                | Put typed-layout-preserving transforms first, an optional T64/GORILLA packing transform next, and compression stages last |
| `Leading sign is not allowed in codec argument at position N in: ...`                       | Use unsigned integer (e.g. `ZSTD(3)`, not `ZSTD(+3)`)        |
| `LZ4: decompressed length N is out of range [0, 1073741824]. Segment may be corrupt.`       | Segment file is corrupt; re-download from deep storage       |

---

## 8. Threading and concurrency

- **`CodecRegistry.DEFAULT`** is built in a `static {}` block and wrapped in `Collections.unmodifiableMap`. Safe for concurrent reads. The mutable `CodecRegistry()` constructor is `@VisibleForTesting`.
- **`CodecPipelineExecutor`** is immutable after construction. `encode(src)` and `decode(src, dst)` are thread-safe.
- **`FixedByteChunkSVForwardIndexReaderV7`** is immutable after construction and may be shared across threads. Each `ChunkReaderContext` is single-threaded — the returned chunk buffer is the context's reusable scratch and must not be retained across `getInt`/`getLong` calls.
- **`FixedByteChunkForwardIndexWriterV7`** is `@NotThreadSafe`.
- **`GzipCodecDefinition`** reuses `ThreadLocal<Deflater>` and `ThreadLocal<Inflater>` instances, resets them after every operation so they do not retain caller buffers, and reads from and writes to direct `ByteBuffer` instances without heap staging arrays.
- **`Lz4CodecDefinition`** wraps `LZ4Factory.fastestInstance()` in a private inner holder class so a missing native library only fails when LZ4 is actually used (not at registry class init).

---

## 9. Performance characteristics

- **Read hot path**: `getInt(int)`/`getLong(int)` use absolute `ByteBuffer.getXxx(int)` indexing — no per-row position mutation, no allocation. The reader returns the context's reusable scratch buffer directly (no per-chunk `duplicate()`).
- **Chunk-cache**: `ChunkReaderContext` caches the last-decoded chunk; sequential reads stay in cache and pay the decode cost only on chunk transitions. `setChunkId(-1)` is set **before** decompress so a thrown decoder leaves the cache invalidated rather than appearing valid with partial data.
- **Single-stage pipelines** decompress directly into the context buffer (no intermediate allocation).
  Multi-stage decode sizes each scratch buffer from the validated outer decoded size and codec bounds;
  every reverse stage uses bounded `decodeInto`. Intermediate direct buffers are deterministically
  cleaned after encode/decode rather than retained until garbage collection.
- **Resource bounds**: writer and reader both preflight the full-chunk composed size bound. A
  pipeline/chunk-size combination is rejected before file creation or context allocation if any
  stage can exceed the 128 MiB encoded/intermediate ceiling or the pipeline can exceed the
  256 MiB cumulative-work ceiling.
- **GZIP** uses the JDK 11+ direct `ByteBuffer` APIs for both deflate and inflate, avoiding whole-chunk heap staging and copy-back. Thread-local `Deflater`/`Inflater` instances amortize native setup cost.
- **Header parse cost**: O(numChunks) once per reader open (monotonicity scan) — bounded and amortized over the segment lifetime.

---

## 10. Backward compatibility

| Concern                                                            | Status                                                                                |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| Existing tables with `compressionCodec` keep working               | ✅ Legacy path unchanged                                                              |
| `FieldConfig` constructor signature for plugins                    | ✅ Unchanged; `codecSpec` lives in the nested `indexes.forward` config                |
| Existing `ForwardIndexConfig` builder and JSON construction paths   | ✅ Preserved; `codecSpec` is an additive builder/JSON property                         |
| `ForwardIndexReader.getCodecSpec()` SPI addition                    | ✅ `default` method returning `null`; existing implementations don't break             |
| Arbitrary version tags emitted by the existing legacy fixed-byte writer (including 5, 6, 7, and 10) | ✅ Structurally distinguished from pipeline V7 and dispatched to the legacy reader |
| Other old V1–V6 segments readable on new servers                    | ✅ Existing reader dispatch remains intact                                            |
| Any `codecSpec` consumed by pre-1.6 components                       | ❌ New JSON property — upgrade config consumers before enabling it                     |
| V7 pipeline segments readable on old servers                         | ❌ Forward-only — upgrade the fleet before enabling any V7-requiring spec               |
| `CompressionCodec` enum unchanged                                   | ✅ No values removed or renamed; legacy field and getters remain supported            |
| Mutual exclusion of `compressionCodec` and `codecSpec`              | ✅ Old JSON remains valid; a column may set only one of the two paths                  |

---

## 11. Test coverage

| Test class                                              | What it locks in                                                    |
|---------------------------------------------------------|---------------------------------------------------------------------|
| `CodecSpecParserTest`                                    | DSL grammar, ASCII identifiers and digits, signed-argument rejection, reserved keyword, and resource limits |
| `CodecInvocationTest`, `CodecPipelineTest`               | Immutable AST construction, normalization, and direct-construction limits |
| `CodecPipelineValidatorTest`                             | Typed-layout-transform chaining, optional packing transform, compression chaining, invalid ordering and type checks |
| `CodecPipelineForwardIndexTest`                          | Write/read round-trips for all 8 built-ins and representative chains across INT/LONG; boundaries; partial last chunks; V7 dispatch |
| `T64CodecDefinitionTest`, `GorillaCodecDefinitionTest`   | Codec-specific round-trips, boundary values, and corrupt-input handling |
| `CompressionCodecCorruptInputTest`                       | Bounded multi-stage decompression, complete-frame validation, and corrupt/truncated input rejection |
| `FixedByteChunkSVForwardIndexReaderV7CorruptionTest`     | Magic-only dispatch, truncated/oversized headers, overflow-safe offsets, per-chunk extents, exact sizes, and resource caps |
| `ForwardIndexReaderFactoryBackwardCompatTest`            | Legacy fixed-byte writer versions 4, 5, 6, 7, and 10 across fixed types and codecs remain readable |
| `CompressionCodecMigratorTest`                           | `toCodecSpec`, `isMigratable`, and schema-aware migration            |
| `CompressionCodecMigrationRoundtripTest`                 | Migrated configs, including DELTA/DELTADELTA, produce identical decoded values |
| `ForwardIndexConfigTest`                                 | JSON round-trip, canonicalization, builder copy, and mutual exclusion |
| `FieldConfigTest` (pinot-spi)                            | Legacy `compressionCodec` JSON compatibility; `codecSpec` remains under `indexes.forward` |
| `ForwardIndexHandlerTest`                                | Legacy↔V7 transitions, transform reload value parity, and the PASS_THROUGH revert path |
| `TableConfigUtilsTest`                                   | Table-config validation and effective forward-index config resolution |
| `CodecPipelineIntegrationTest`                           | All configured codec specs in both query engines (SSE+MSE), including point, aggregate, cross-codec, and dictionary-coexistence assertions |

---

## 12. Future work

- **Plugin codec registration**: expose `CodecRegistry.setDefault(...)` or service-loader so external modules can register codecs without forking.
- **Variable-width / MV V7 support**: extend the V7 writer/reader to STRING, BYTES, and multi-value columns. Single-stage specs that map to a legacy compression codec already route to existing raw forward-index writers.
- **Transform chaining**: `DELTA`/`DELTADELTA` are header-less typed-layout-preserving transforms and chain freely; `T64`/`GORILLA` are packing transforms (bit-packed output) and so must be the last transform. A follow-up could give the packing transforms a typed-passthrough output form so chains like `CODEC(T64, GORILLA)` become expressible — though the practical value is limited.
- **JMH benchmark**: add `pinot-perf` benchmarks comparing V7 (LZ4 / ZSTD / DELTA+LZ4) against legacy `FixedBytePower2ChunkSVForwardIndexReader` for read throughput and segment build cost.
- **Controller-side capability gate**: refuse `codecSpec` table-config updates until every relevant config consumer and segment reader/builder advertises support (mixed-version safety).
- **Sunset of legacy `compressionCodec`**: once `codecSpec` covers all column types, deprecate `getCompressionCodec()` and the enum constants in 2.0 per `CompressionCodecMigrator`'s class Javadoc.

---

## 13. References

- Pull request: [#18229](https://github.com/apache/pinot/pull/18229)
- Source packages: `pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/codec/`, `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/io/codec/`
- Reader: `pinot-segment-local/.../FixedByteChunkSVForwardIndexReaderV7.java`
- Writer: `pinot-segment-local/.../FixedByteChunkForwardIndexWriterV7.java`
- Migrator: `pinot-segment-local/.../utils/CompressionCodecMigrator.java`
- Handler: `pinot-segment-local/.../ForwardIndexHandler.java` (`shouldRewriteRawForwardIndex`)
- Integration test: `pinot-integration-tests/.../custom/CodecPipelineIntegrationTest.java`
