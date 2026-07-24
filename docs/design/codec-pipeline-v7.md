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
**Scope (v1)**: Compression-only `codecSpec` values for existing raw forward-index compression paths;
transform `codecSpec` values for single-value INT and LONG raw forward indexes using V7
**Wire-format version**: 7 (frozen on-disk identifier)

---

## 1. Goals and non-goals

### Goals
1. Replace the closed `FieldConfig.CompressionCodec` enum with an extensible **codec pipeline DSL** so new codecs (or new combinations) can be added without churning the public enum.
2. Introduce a **multi-stage pipeline** that lets users compose a transform (e.g. DELTA, DELTADELTA) with a compression codec (e.g. LZ4, ZSTD) — covering use cases the single-codec enum cannot express cleanly.
3. Embed the **canonical codec spec** in V7 transform segment headers so readers can decode without out-of-band configuration; transform segments are self-describing.
4. Preserve full **backward compatibility**: legacy `compressionCodec` continues to work; `codecSpec` is opt-in per column.
5. Provide a clear **migration path** from the legacy enum to the DSL.

### Non-goals (v1)
- Variable-width or multi-value V7 transform storage. Compression-only `codecSpec` values use the existing raw writer formats, while the V7 transform writer is fixed-byte SV INT/LONG only.
- Streaming / dictionary-encoded indexes. `codecSpec` applies to **raw** forward indexes only.
- A SQL-visible codec function. The DSL lives in table config (`fieldConfigList[].indexes.forward.codecSpec`).
- Plugin-registered codecs. v1 ships with 6 built-in codecs; plugin registration is a v2 follow-up.

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
- A pipeline is a chain of the form **N value-preserving transforms → at most one packing transform → N compressions**:
  - **Value-preserving transforms** (`DELTA`, `DELTADELTA`) map a column-typed value array to a same-width value array (header-less passthrough; element type comes from the column context, value count from the buffer length). Any number may be chained, so `CODEC(DELTA, DELTADELTA, LZ4)` is valid.
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
| `CODEC(DELTA, DELTADELTA, LZ4)`      | DELTA → DELTADELTA → LZ4        | Chained value transforms + compression  |
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
        | Lz4CodecDefinition             (COMPRESSION, lazy native)  |
        | ZstdCodecDefinition            (COMPRESSION, level 1..22)  |
        | SnappyCodecDefinition          (COMPRESSION)               |
        | GzipCodecDefinition            (COMPRESSION, ThreadLocal   |
        |                                 staging buffers)           |
        |                                                            |
        | CodecRegistry                  (immutable DEFAULT;         |
        |                                 mutable @VisibleForTesting)|
        | CodecPipelineValidator         (structural rules:          |
        |                                 N TRANSFORMs in order,     |
        |                                 ≤1 COMPRESSION at end)     |
        | CodecPipelineExecutor          (binds pipeline → handlers; |
        |                                 thread-safe;                |
        |                                 compress/decompress;        |
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
        |   - dispatched by ForwardIndexReaderFactory on version=7  |
        |   - reads canonicalSpec from header → builds executor     |
        |   - validates header bounds, monotonic chunk offsets      |
        | ForwardIndexCreatorFactory                                 |
        |   - compression-only codecSpec → existing raw writers      |
        |   - transform codecSpec → V7 fixed-byte writer             |
        +-----------------------------------------------------------+
```

**Module placement rationale**: SPI types are pure interfaces + AST + parser (no JNI, no runtime state). Concrete codec handlers and registry/executor live in `pinot-segment-local` because they reference native libraries (LZ4/ZSTD/Snappy) and PinotDataBuffer — both of which are local-module dependencies.

---

## 4. On-disk format (V7)

The V7 file is **self-describing**: every reader can determine the codec spec from the file header alone. There is no out-of-band configuration required at read time.

```
┌─────────────────────────────────────────────────────────────────────┐
│ FILE HEADER                                                         │
├─────────────────────────────────────────────────────────────────────┤
│ Offset  Field             Size    Notes                             │
│ ─────── ────────────────  ─────   ─────────────────────────────────  │
│   0     version           int(4)  = 7 (frozen identifier)           │
│   4     numChunks         int(4)  ≥ 0                                │
│   8     numDocsPerChunk   int(4)  power of 2, ≥ 1                    │
│  12     sizeOfEntry       int(4)  4 (INT) or 8 (LONG)                │
│  16     totalDocs         int(4)  ≥ 0                                │
│  20     codecSpecLength   int(4)  > 0                                │
│  24     dataHeaderStart   int(4)  = 28 + codecSpecLength             │
│  28     codecSpec[]       byte[]  UTF-8 canonical DSL                │
│  X      chunkOffsets[]    long[8] absolute file offsets, monotonic   │
├─────────────────────────────────────────────────────────────────────┤
│ DATA SECTION (one entry per chunk)                                  │
├─────────────────────────────────────────────────────────────────────┤
│  Y     compressedSize     int(4)                                    │
│  Y+4   uncompressedSize   int(4)                                    │
│  Y+8   payload            byte[compressedSize]  (codec output)      │
└─────────────────────────────────────────────────────────────────────┘

X = 28 + codecSpecLength
Y = chunkOffsets[i]
```

### Reader-side validation (corruption defense)

| Check                                                             | Failure                              |
|-------------------------------------------------------------------|--------------------------------------|
| `version == 7`                                                    | `IllegalArgumentException`           |
| `numChunks ≥ 0`                                                   | `IllegalArgumentException`           |
| `numDocsPerChunk` is a power of two                               | `IllegalArgumentException`           |
| `sizeOfEntry == storedType.size()`                                | `IllegalArgumentException`           |
| `totalDocs ≥ 0`                                                   | `IllegalArgumentException`           |
| `codecSpecLength > 0`                                             | `IllegalArgumentException`           |
| `dataHeaderStart` and chunk-offset table fit in buffer            | `IllegalArgumentException`           |
| `chunkOffsets[]` strictly monotonic, all in data section          | `IllegalArgumentException`           |
| Per-chunk `compressedSize ≥ 0` and fits in remaining buffer       | `IllegalStateException` ("corrupt")  |
| Decompressed size matches `uncompressedSize`                      | `IllegalStateException` ("corrupt")  |
| `decompressedSize ≤ MAX_REASONABLE_DECOMPRESSED_SIZE` (1 GiB)     | `IOException` ("DoS guard")          |

---

## 5. Codec catalog (built-in)

| Codec        | `CodecKind`  | Args         | Wire format                                                          | Notes                                                                    |
|--------------|--------------|--------------|----------------------------------------------------------------------|--------------------------------------------------------------------------|
| `DELTA`      | TRANSFORM    | none         | `[first:N][delta_i:N for i=1..count-1]` (header-less passthrough; type from column ctx, count from length) | Value-preserving (chainable); two's-complement wrap intentional, symmetric on decode (locked by tests) |
| `DELTADELTA` | TRANSFORM    | none         | `[first:N][firstDelta:N][dod_i:N for i=2..]` (header-less passthrough) | Value-preserving (chainable); same wrap semantics                          |
| `T64`        | TRANSFORM    | none         | `[flag:1B][count:4B]` + per-64-value block `[baseline:N][bitWidth:1B][packed:ceil(bitWidth*64/8)B]` | Frame-of-reference + bit-packing on fixed 64-value blocks                |
| `GORILLA`    | TRANSFORM    | none         | `[flag:1B][count:4B][first:N][bit-stream]`                            | XOR-delta with MSB-first bit-stream, reusing previous leading/width window when it fits |
| `LZ4`        | COMPRESSION  | none         | LZ4 length-prefixed                                                   | `LZ4Factory.fastestInstance()` lazy init via inner holder class           |
| `ZSTD`       | COMPRESSION  | `level` (int) | Zstd frame with embedded decompressedSize                             | Levels 1–22; default 3                                                    |
| `SNAPPY`     | COMPRESSION  | none         | xerial Snappy                                                         | JNI requires direct buffers (handled internally)                          |
| `GZIP`       | COMPRESSION  | none         | DEFLATE payload + 4-byte uncompressedSize footer                      | ThreadLocal `Deflater`/`Inflater` + staging buffers; capped at 16 MiB     |

**Frozen on-disk names**: All codec `NAME` constants (DELTA, DELTADELTA, T64, GORILLA, LZ4, ZSTD, SNAPPY, GZIP) and the keyword `CODEC` are part of the on-disk format contract and must never be changed.

**Rolling upgrade considerations**:

1. **V7 segments are unreadable by pre-V7 servers.** The V7 format reuses the SV fixed-byte forward-index file family but bumps the version byte to 7. Servers built before this change use `>= VERSION 4` to dispatch to the legacy `FixedBytePower2ChunkSVForwardIndexReader`, which then misinterprets V7's `codecSpecLength` field as the legacy `compressionType` int. The likely outcome is `IllegalArgumentException` deep inside chunk decode; the silent-wrong-result case is also possible. **DO NOT enable `codecSpec` on any column until every server in your fleet has been upgraded** to a build that includes V7 support.

2. **Adding a new codec to `CodecRegistry.DEFAULT` is also a rolling-upgrade-sensitive change.** A server that does not know a given codec name cannot read segments encoded with it (lookup throws `IllegalArgumentException`). Operators must ensure every server in the fleet runs a build that registers the codec **before** enabling that codec on any column in table config.

3. **Rollback is one-way at the codec level.** Once a column has been written with a V7 segment, downgrading the server fleet requires (a) reverting the `codecSpec` config to a legacy `compressionCodec`, (b) reloading the segments via the new servers to convert them back to a legacy raw format (this is supported — see §7.3), and (c) only then downgrading servers. Skipping step (b) leaves V7 segments on disk that downgraded servers cannot read.

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
- Structural pipeline errors (>1 TRANSFORM, >1 COMPRESSION, COMPRESSION not last)
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

1. For transform specs (`DELTA` / `DELTADELTA`), verify all servers in the tenant are upgraded to a Pinot version that ships V7 readers (≥ 1.6).
2. Add `codecSpec` under the column's `indexes.forward` block. Do **not** also set a top-level `compressionCodec`; the table config validator will reject mutual presence.
3. Rebuild affected segments (push offline data or wait for realtime → committed transitions).
4. Compression-only specs are written with the existing raw forward-index formats. Transform specs are written with V7 and embed the canonical spec.

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

If a table needs to be downgraded to a Pinot version that does **not** understand `codecSpec`, first replace affected configs with legacy `compressionCodec` values. Tables with transform specs also need their V7 segments rewritten before downgrade:

1. In table config, replace `codecSpec` with an equivalent legacy `compressionCodec` (e.g. `codecSpec="LZ4"` → `compressionCodec=LZ4`).
2. For transform specs, trigger a segment reload. `ForwardIndexHandler.shouldRewriteRawForwardIndex` detects the legacy revert and rewrites V7 segments to the legacy format.
3. Once all segments are rewritten, the cluster can be downgraded.

The helper `ForwardIndexHandler.isLegacyRevertTargetForFixedByteSv` identifies legitimate revert targets:
`PASS_THROUGH`, `SNAPPY`, `ZSTANDARD`, `LZ4`, `GZIP`, `DELTA`, `DELTADELTA` (CLP family is excluded — not applicable to fixed-byte SV).

### 7.4 Mixed-version cluster safety

V7 transform segments are **forward-only**: they can only be read by Pinot ≥ 1.6. To safely roll out transform specs:

1. Upgrade all servers in the tenant to a V7-capable version.
2. Verify with `kubectl get pods` (or equivalent) that no server is running an older binary.
3. Only then enable transform `codecSpec` values on any column.

If a transform `codecSpec` is enabled before all servers are upgraded, older servers will fail segment loads with `UnsupportedOperationException("Unsupported fixed-byte SV forward index version: 7")`.

### 7.5 Common errors and remediation

| Error message                                                                              | Remediation                                                  |
|--------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `Conflicting forward-index config for column: <col> — FieldConfig.compressionCodec=... but indexes.forward.codecSpec is also set` | Remove the legacy top-level `compressionCodec`; keep only `indexes.forward.codecSpec` |
| `codecSpec '...' requires the V7 codec-pipeline writer ... only supports single-value columns. Column 'X' is multi-value` | Use a compression-only spec or legacy `compressionCodec`     |
| `codecSpec '...' requires the V7 codec-pipeline writer ... only supports INT and LONG columns. Column 'X' has type: ...`   | Use a compression-only spec or legacy `compressionCodec`     |
| `Compression-only codecSpec for column 'X' must be one of LZ4, SNAPPY, GZIP, ZSTD, or ZSTD(3)` | Use a legacy-compatible compression-only spec or add a transform |
| `Unknown codec 'XYZ'. Known codecs: [DELTA, DELTADELTA, ZSTD, LZ4, SNAPPY, GZIP]`            | Fix typo in DSL                                              |
| `pipeline must contain exactly one COMPRESSION stage as the last stage`                     | Re-order stages; e.g. `CODEC(DELTA,ZSTD(3))` not `CODEC(ZSTD(3),DELTA)` |
| `Leading sign is not allowed in codec argument at position N in: ...`                       | Use unsigned integer (e.g. `ZSTD(3)`, not `ZSTD(+3)`)        |
| `LZ4: decompressed length N is out of range [0, 1073741824]. Segment may be corrupt.`       | Segment file is corrupt; re-download from deep storage       |

---

## 8. Threading and concurrency

- **`CodecRegistry.DEFAULT`** is built in a `static {}` block and wrapped in `Collections.unmodifiableMap`. Safe for concurrent reads. The mutable `CodecRegistry()` constructor is `@VisibleForTesting`.
- **`CodecPipelineExecutor`** is immutable after construction. `compress(src)` and `decompress(src, dst)` are thread-safe.
- **`FixedByteChunkSVForwardIndexReaderV7`** is immutable after construction and may be shared across threads. Each `ChunkReaderContext` is single-threaded — the returned chunk buffer is the context's reusable scratch and must not be retained across `getInt`/`getLong` calls.
- **`FixedByteChunkForwardIndexWriterV7`** is `@NotThreadSafe`.
- **`GzipCodecDefinition`** uses `ThreadLocal<Deflater>`, `ThreadLocal<Inflater>`, and three `ThreadLocal<byte[]>` staging buffers. Buffers grow on demand and are capped at 16 MiB; outliers above the cap are allocated one-shot (not retained) to prevent unbounded heap pinning.
- **`Lz4CodecDefinition`** wraps `LZ4Factory.fastestInstance()` in a private inner holder class so a missing native library only fails when LZ4 is actually used (not at registry class init).

---

## 9. Performance characteristics

- **Read hot path**: `getInt(int)`/`getLong(int)` use absolute `ByteBuffer.getXxx(int)` indexing — no per-row position mutation, no allocation. The reader returns the context's reusable scratch buffer directly (no per-chunk `duplicate()`).
- **Chunk-cache**: `ChunkReaderContext` caches the last-decoded chunk; sequential reads stay in cache and pay the decode cost only on chunk transitions. `setChunkId(-1)` is set **before** decompress so a thrown decoder leaves the cache invalidated rather than appearing valid with partial data.
- **Single-stage pipelines** decompress directly into the context buffer (no intermediate allocation). Multi-stage pipelines allocate one direct buffer per non-terminal stage; the terminal stage writes into the context via `decodeInto`.
- **GZIP** writes use `Deflater.setInput(byte[], 0, len)` with a thread-local source buffer — no per-call heap allocation. **GZIP** reads inflate into a thread-local staging buffer; the caller copies the exact `decompressedSize` bytes into the destination.
- **Header parse cost**: O(numChunks) once per reader open (monotonicity scan) — bounded and amortized over the segment lifetime.

---

## 10. Backward compatibility

| Concern                                                            | Status                                                                                |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| Existing tables with `compressionCodec` keep working               | ✅ Legacy path unchanged                                                              |
| `FieldConfig` constructor signature for plugins                    | ✅ Deprecated 9-arg shim retained as `@Deprecated(forRemoval=true)`                   |
| `ForwardIndexConfig` constructor signature                          | ✅ Deprecated 9-arg shim retained                                                     |
| `ForwardIndexReader.getCodecSpec()` SPI addition                    | ✅ `default` method returning `null`; existing implementations don't break             |
| Old V1–V6 segments readable on new servers                          | ✅ `ForwardIndexReaderFactory` continues to dispatch by version                       |
| V7 transform segments readable on old servers                        | ❌ Forward-only — operators must upgrade fleet before enabling transform specs         |
| `CompressionCodec` enum unchanged                                   | ✅ No values removed / renamed; legacy field/getter deprecated for new configs        |
| Mutual exclusion of `compressionCodec` and `codecSpec` (old JSON)   | ✅ Old JSON without `codecSpec` deserializes to `codecSpec=null`                      |

---

## 11. Test coverage

| Test class                                              | Cases | What it locks in                                                    |
|---------------------------------------------------------|-------|---------------------------------------------------------------------|
| `CodecSpecParserTest`                                    | 16    | DSL grammar, ASCII identifier, sign rejection, reserved keyword     |
| `CodecPipelineValidatorTest`                             | 13    | Structural rules (≤1 transform, compression last)                   |
| `CodecPipelineForwardIndexTest`                          | 57    | Write/read round-trip across all 6 codecs × INT/LONG; boundaries (INT_MIN/MAX, LONG_MIN/MAX); partial last chunk; V7 dispatch |
| `ForwardIndexReaderFactoryBackwardCompatTest`            | 7     | Legacy V4 segments still readable through new factory               |
| `CompressionCodecMigratorTest`                           | 27    | toCodecSpec, isMigratable, schema-aware migration                   |
| `CompressionCodecMigrationRoundtripTest`                 | 20    | Migrated configs produce identical decoded values                   |
| `ForwardIndexConfigTest`                                 | 13    | JSON round-trip; Builder copy / mutual exclusion                    |
| `FieldConfigTest` (pinot-spi)                            | 5     | JSON round-trip with/without `codecSpec`; both-fields rejection     |
| `ForwardIndexHandlerTest`                                | 35    | Legacy↔V7 transitions including PASS_THROUGH revert path            |
| `CodecPipelineIntegrationTest`                           | end-to-end | 7 codec specs × 2 query engines (SSE+MSE) × point/aggregate/cross-codec/dict-coexistence assertions |

---

## 12. Future work

- **Plugin codec registration**: expose `CodecRegistry.setDefault(...)` or service-loader so external modules can register codecs without forking.
- **Variable-width / MV transform support**: extend the V7 writer/reader to STRING, BYTES, and multi-value columns. Compression-only specs already route to the existing raw forward-index writers for supported legacy compression codecs.
- **Chainable packing transforms**: `DELTA`/`DELTADELTA` are header-less value-preserving transforms and chain freely; `T64`/`GORILLA` are packing transforms (bit-packed output) and so must be the last transform. A follow-up could give the packing transforms a typed-passthrough output form so chains like `CODEC(T64, GORILLA)` become expressible — though the practical value is limited.
- **JMH benchmark**: add `pinot-perf` benchmarks comparing V7 (LZ4 / ZSTD / DELTA+LZ4) against legacy `FixedBytePower2ChunkSVForwardIndexReader` for read throughput and segment build cost.
- **Controller-side version gate**: refuse `codecSpec` table-config updates if any tenant server reports a pre-1.6 version (mixed-version safety).
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
