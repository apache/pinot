<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# SPARSE_MAP DataType — Implementation Summary

## Overview

`SPARSE_MAP` is a new native Pinot datatype designed for columns where each document carries a
map of key-value pairs but most keys are absent in any given document (i.e., the map is sparse).

Unlike the existing `MAP` type (which stores the entire map as a JSON blob), `SPARSE_MAP` stores
each key's values in a dedicated column-like structure, enabling:

- **O(1) per-key value retrieval** via presence bitmap rank operations.
- **O(1) key-presence check** via `ImmutableRoaringBitmap`.
- **Fast EQ/IN filtered queries** (e.g., `WHERE col['country'] = 'US'`) using per-key inverted
  indexes without full document scanning.
- **Range / BETWEEN filtered queries** (e.g., `WHERE col['clicks'] >= 5`) via expression-based
  scan over the per-key forward index.
- **Direct group-by and aggregation** on individual keys.

---

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| FieldType | `SPARSE_MAP` (`SparseMapFieldSpec`) | Dedicated `FieldSpec` subclass (analogous to `ComplexFieldSpec`). Avoids polluting `DimensionFieldSpec` with sparse-map-specific fields. |
| Stored type | `SPARSE_MAP` (self-referential) | No reuse of `MAP`'s blob forward index. `SparseMapIndex` is the sole storage. |
| Forward index | **None** | Eliminated; full map reconstruction is done from the `SparseMapIndex`. |
| Dictionary | **None** | Values are stored raw (typed) inside the index binary format. |
| Null semantics | Key absence = `NULL` | The `presenceBitmap` implicitly represents NULL for absent keys. **Null values in ingested maps are treated as absent** (key not recorded); previously they were coerced to type zero-defaults, incorrectly marking the key present. Reading an absent key via the forward index still returns the zero-default (0, 0.0, `""`). There is no distinction between "key absent" and "key present with null value". |
| Multi-value keys | **Not supported** (initially) | Single-value per key per document only. |
| Undeclared keys | Stored as `sparseMapDefaultValueType` (default: `STRING`) | Any key not in `sparseMapKeyTypes` is stored as the configurable default type. |
| Query syntax | Reuse `col['key']` (`ItemTransformFunction`) | No new parser changes needed. |
| Config format | `fieldConfigList` (new format only) | Legacy `tableIndexConfig.sparseMapIndexConfigs` is not supported; this is a new feature with no backward compatibility requirement. |

---

## Schema Definition

A `SPARSE_MAP` column is declared as a `SparseMapFieldSpec` and serialised under a dedicated
`sparseMapFieldSpecs` array in the schema JSON (analogous to `complexFieldSpecs`):

```json
{
  "sparseMapFieldSpecs": [
    {
      "name": "userMetrics",
      "fieldType": "SPARSE_MAP",
      "keyTypes": {
        "clicks": "LONG",
        "spend":  "DOUBLE",
        "country": "STRING"
      },
      "defaultValueType": "STRING"
    }
  ]
}
```

- `keyTypes` — required; maps key names to their declared `DataType`.
- `defaultValueType` — optional (default: `STRING`); type used for any key not in `keyTypes`.

---

## Index Configuration (Table Config)

Use the `fieldConfigList` format — the only supported configuration path:

```json
{
  "tableIndexConfig": {
    "loadMode": "MMAP"
  },
  "fieldConfigList": [
    {
      "name": "metrics",
      "indexes": {
        "sparse_map": {
          "enabled": true,
          "enableInvertedIndexForAll": true,
          "maxKeys": 100
        }
      }
    }
  ]
}
```

Per-key inverted index control allows fine-grained selection of which keys get inverted indexes:

```json
{
  "fieldConfigList": [
    {
      "name": "riskError",
      "indexes": {
        "sparse_map": {
          "enabled": true,
          "enableInvertedIndexForAll": false,
          "invertedIndexKeys": ["code", "title"],
          "maxKeys": 100
        }
      }
    }
  ]
}
```

Configuration fields:

- `enabled` — whether to build the index (default: `true`).
- `enableInvertedIndexForAll` — if `true`, inverted indexes are built for **all** keys and
  `invertedIndexKeys` is ignored. If `false`, only the keys listed in `invertedIndexKeys` get
  inverted indexes.
- `invertedIndexKeys` — explicit set of key names that should have inverted indexes. Only
  consulted when `enableInvertedIndexForAll` is `false`.
- `indexedKeys` — optional set of key names to store in the index. If `null`, all keys from the
  ingested map are stored (subject to `maxKeys`). If specified, only these keys are stored; all
  other keys in the ingested map are silently dropped.
- `maxKeys` — maximum distinct keys to track (default: `1000`). Keys beyond this limit are
  silently dropped with a WARN log.

Keys that have inverted indexes also get a **dictId forward index** and a **value dictionary**
automatically. These enable dictionary-based GROUP BY operations (see below).

> **Note:** The legacy `tableIndexConfig.sparseMapIndexConfigs` path was removed. Only the
> `fieldConfigList.indexes.sparse_map` path is supported.

---

## Binary Index Format (On-Disk)

File extension: `.sparsemap.idx`

All multi-byte integers and offsets are written and read in **BIG_ENDIAN** order throughout.

The index file has four sections: header, key dictionary, key metadata, per-key data, and a
value dictionary. The header stores offsets to each section for O(1) random access.

```
[Header — 64 bytes, BIG_ENDIAN]
  MAGIC:            4 bytes  0x53504D58 ("SPMX")
  VERSION:          4 bytes  2
  NUM_KEYS:         4 bytes  K
  NUM_DOCS:         4 bytes  D
  keyDictOffset:    8 bytes  file offset of key-dictionary section
  keyMetaOffset:    8 bytes  file offset of key-metadata section
  perKeyDataOffset: 8 bytes  file offset of per-key data section
  valueDictOffset:  8 bytes  file offset of value-dictionary section
  padding:         16 bytes  zeros

[Key dictionary]
  NUM_KEYS:       4 bytes  (int, same as header)
  For each key k in [0..K):
    KEY_NAME_LEN: 4 bytes  (int)
    KEY_NAME:     variable  UTF-8 bytes

[Key metadata — 69 bytes per key, BIG_ENDIAN]
  For each key k:
    VALUE_TYPE:        1 byte   DataType ordinal (bounds-checked on read)
    NUM_DOCS_WITH_KEY: 4 bytes  N_k
    presenceOffset:    8 bytes  offset of presence bitmap within per-key data
    presenceLen:       8 bytes  byte length of presence bitmap
    fwdOffset:         8 bytes  offset of forward index within per-key data
    fwdLen:            8 bytes  byte length of forward index
    invOffset:         8 bytes  offset of inverted index within per-key data
    invLen:            8 bytes  byte length of inverted index
    dictIdFwdOffset:   8 bytes  offset of dictId forward index within per-key data
    dictIdFwdLen:      8 bytes  byte length of dictId forward index

[Per-key data]
  For each key k:
    PRESENCE_BITMAP:   presenceLen bytes  serialized ImmutableRoaringBitmap (N_k bits set)
    FORWARD_INDEX:     fwdLen bytes       typed values in document order (N_k entries)
    INVERTED_INDEX:    invLen bytes       (present only if inverted index enabled for this key)
      NUM_DISTINCT_VALUES: 4 bytes
      For each distinct value v:
        VALUE_STR_LEN: 4 bytes
        VALUE_STR:     variable
        BITMAP_SIZE:   4 bytes
        BITMAP:        variable  docs with key=v
    DICTID_FWD_INDEX:  dictIdFwdLen bytes (present only if inverted index enabled)
      Bit-packed array of dictIds, one per document (numDocs entries, not N_k).
      Documents without the key get the default value's dictId.
      Bit width = ceil(log2(numDistinctValues)).

[Value dictionary section]
  For each key with inverted index enabled:
    NUM_DISTINCT_VALUES: 4 bytes  total distinct values (including default)
    NUM_BITS_PER_VALUE:  4 bytes  bits used per dictId in the bit-packed array
    For each distinct value v (sorted):
      VALUE_STR_LEN: 4 bytes
      VALUE_STR:     variable  UTF-8 bytes
```

The reader validates `MAGIC` and `VERSION` on open, and bounds-checks each key's
`VALUE_TYPE` ordinal against `DataType.values()` before use.

### Forward Index Formats

**Fixed-width types** (INT, LONG, FLOAT, DOUBLE): values are stored consecutively in document
order as fixed-size big-endian primitives. The position of value for `docId` is computed as
`presenceBitmap.rank(docId) - 1` (0-based index into the values array). The reader also
cross-validates that the forward index byte length matches the expected size based on the
presence bitmap cardinality.

**Variable-width types** (STRING, BYTES): the forward index stores:
```
  NUM_VALUES:  4 bytes (int, equals N_k)
  OFFSETS:     (N_k + 1) × 4 bytes  — start offsets into the data region
  DATA:        variable bytes        — concatenated UTF-8 strings or raw bytes
```
The value at ordinal `i` spans from `OFFSETS[i]` to `OFFSETS[i+1]`.

### DictId Forward Index (Bit-Packed)

For keys with inverted indexes enabled, a bit-packed dictId forward index is built. Unlike
the regular forward index (which stores values only for documents that have the key), the
dictId forward index stores one dictId per **every document** in the segment. Documents without
the key are assigned the default value's dictId (0 for numeric types, empty string for STRING).

This enables O(1) dictionary-based GROUP BY: the query engine reads the dictId directly from
the bit-packed array for any document without needing a presence bitmap rank operation. The
`SparseMapKeyForwardIndexReader.readDictIds()` method provides the fast path.

---

## File & Class Inventory

### New Files

| File | Module | Purpose |
|---|---|---|
| `SparseMapIndexConfig.java` | `pinot-spi` | Index configuration (enabled, enableInvertedIndexForAll, invertedIndexKeys, indexedKeys, maxKeys). Per-key inverted index control: `enableInvertedIndexForAll: true` → all keys get inverted indexes; `false` + `invertedIndexKeys` → only listed keys; `false` + no list → no inverted indexes. |
| `SparseMapFieldSpec.java` | `pinot-spi` | Dedicated `FieldSpec` subclass for `SPARSE_MAP` columns; holds `keyTypes` and `defaultValueType`. Analogous to `ComplexFieldSpec`. |
| `SparseMapIndexCreator.java` | `pinot-segment-spi` | SPI interface: `add(Map<String,Object>)`, `seal()`. |
| `SparseMapIndexReader.java` | `pinot-segment-spi` | SPI interface: `getKeys()`, `getPresenceBitmap()`, typed getters, `getDocsWithKeyValue()`, `getMap()`. |
| `SparseMapIndexPlugin.java` | `pinot-segment-local` | `@AutoService`-registered plugin that exposes `SparseMapIndexType`. |
| `SparseMapIndexType.java` | `pinot-segment-local` | `AbstractIndexType` implementation; wires creator, reader, handler, and mutable index. New format only — no legacy deserializer or cleanup hooks. Uses `FieldType.SPARSE_MAP` check and casts to `SparseMapFieldSpec`. |
| `OnHeapSparseMapIndexCreator.java` | `pinot-segment-local` | On-heap index builder; accumulates per-doc maps in memory, serialises binary format on `seal()`. Casts `FieldSpec` to `SparseMapFieldSpec` to access `keyTypes` and `defaultValueType`. |
| `ImmutableSparseMapIndexReader.java` | `pinot-segment-local` | Memory-mapped reader; parses binary format, provides O(1) typed value access via bitmap rank. |
| `MutableSparseMapIndexImpl.java` | `pinot-segment-local` | In-memory mutable index for real-time segments; uses `MutableRoaringBitmap` + `List<Object>` per key; thread-safe via `ReentrantReadWriteLock`. Casts to `SparseMapFieldSpec` to access key types. |
| `SparseMapDataSource.java` | `pinot-segment-local` | `MapDataSource` implementation wrapping `SparseMapIndexReader`; used by the query engine. Exposes per-key `DataSource` via `getKeyDataSource(key)`. |
| `SparseMapKeyForwardIndexReader.java` | `pinot-segment-local` | Per-key `ForwardIndexReader` backed by `SparseMapIndexReader`. Enables the query engine to read typed key values for projection, aggregation, and range evaluation. Returns zero-default for absent keys. Supports dictionary-encoded access via `readDictIds()` when a `SparseMapKeyDictionary` and `FixedBitIntReaderWriter` are available — the fast path for GROUP BY. |
| `SparseMapKeyDictionary.java` | `pinot-segment-local` | In-memory sorted `Dictionary` implementation backed by distinct values from the inverted index. Provides O(1) string→dictId lookup via `Object2IntOpenHashMap`. Used by `readDictIds()` for dictionary-based GROUP BY. Thread-safe (immutable after construction). |
| `SparseMapIndexHandler.java` | `pinot-segment-local` | `BaseIndexHandler` for segment reload: removes disabled indexes; warns on add (re-ingest required). |
| `SparseMapColumnPreIndexStatsCollector.java` | `pinot-segment-local` | Minimal stats collector (tracks doc count only; no min/max/cardinality). |
| `SparseMapFilterOperator.java` | `pinot-core` | Filter operator using `SparseMapIndexReader.getDocsWithKeyValue()` for EQ/NOT_EQ/IN/NOT_IN predicates via inverted index. Range predicates fall back to `ExpressionFilterOperator`. NOT_EQ/NOT_IN exclude absent-key documents (SQL NULL semantics). |
| `SparseMapFilterOperatorTest.java` | `pinot-core` | Tests NOT_EQ/NOT_IN absence-exclusion semantics. |

### Modified Files

| File | Module | Change |
|---|---|---|
| `FieldSpec.java` | `pinot-spi` | Added `SPARSE_MAP` to `DataType` enum and `FieldType` enum; `getStoredType()`, `getDefaultNullValue()`, `compare()`. Added `@JsonSubTypes` entry for `SparseMapFieldSpec`. |
| `DimensionFieldSpec.java` | `pinot-spi` | Removed `sparseMapKeyTypes` and `sparseMapDefaultValueType` fields (moved to `SparseMapFieldSpec`). Removed `SPARSE_MAP` from allowed dimension data types. |
| `Schema.java` | `pinot-spi` | Added `setSparseMapFieldSpecs()` deserialization setter and `sparseMapFieldSpecs` serialization (mirrors `complexFieldSpecs`). Updated `addField()` switch with `case SPARSE_MAP` that validates `keyTypes` and `defaultValueType`. `hasSparseMapColumn()` now checks `FieldType.SPARSE_MAP`. |
| `CommonConstants.java` | `pinot-spi` | Added `NullValuePlaceHolder.SPARSE_MAP = Collections.emptyMap()`. |
| `IndexingConfig.java` | `pinot-spi` | Removed `sparseMapIndexConfigs` field (legacy format dropped; config now lives in `fieldConfigList`). |
| `DataSchema.java` | `pinot-common` | Added `SPARSE_MAP` to `ColumnDataType` enum, mapped to `SqlTypeName.MAP`. |
| `PinotDataType.java` | `pinot-common` | Added `SPARSE_MAP` to `PinotDataType` enum with map-specific convert/byte handling. |
| `V1Constants.java` | `pinot-segment-spi` | Added `SPARSE_MAP_INDEX_FILE_EXTENSION = ".sparsemap.idx"` and `SPARSE_MAP_COLUMNS = "segment.sparsemap.column.names"` for segment metadata persistence. |
| `StandardIndexes.java` | `pinot-segment-spi` | Added `SPARSE_MAP_ID = "sparse_map"` and `sparseMap()` static accessor. |
| `SegmentGeneratorConfig.java` | `pinot-segment-spi` | Added `getSparseMapColumnNames()` method to return columns with `FieldType.SPARSE_MAP`. |
| `ColumnMetadataImpl.java` | `pinot-segment-spi` | Added `case SPARSE_MAP` to reconstruct `SparseMapFieldSpec` (with `keyTypes` and `defaultValueType`) from segment metadata properties. |
| `SegmentMetadataImpl.java` | `pinot-segment-spi` | Added `addPhysicalColumns()` call for `SPARSE_MAP_COLUMNS` so SPARSE_MAP columns are discovered during segment metadata init (required for V1→V3 conversion). |
| `ForwardIndexType.java` | `pinot-segment-local` | Return `null` from `createMutableIndex()` for `SPARSE_MAP` columns (no forward index). |
| `StatsCollectorUtil.java` | `pinot-segment-local` | Route `SPARSE_MAP` columns to `SparseMapColumnPreIndexStatsCollector`; exclude from no-dict optimisation path. |
| `BaseSegmentCreator.java` | `pinot-segment-local` | Skip dictionary creation for `SPARSE_MAP` columns. Persist `SPARSE_MAP_COLUMNS` list to segment metadata properties. Cast to `SparseMapFieldSpec` (instead of `DimensionFieldSpec`) for metadata persistence. |
| `MutableSegmentImpl.java` | `pinot-segment-local` | Allow missing forward index for `SPARSE_MAP`; return `SparseMapDataSource`; treat as no-dictionary; skip min/max update. |
| `ImmutableSegmentImpl.java` | `pinot-segment-local` | Explicitly create `SparseMapDataSource` (with the `SparseMapIndexReader` from the index container) for `SPARSE_MAP` columns instead of falling through to `ImmutableDataSource`. |
| `SparseMapDataSource.java` | `pinot-segment-local` | Changed `_keyDataSourceCache` from `HashMap` to `ConcurrentHashMap` to prevent data race under concurrent query threads on immutable segments. Updated exception message to reference `SparseMapFieldSpec`. |
| `DataTypeTransformerUtils.java` | `pinot-segment-local` | Skip standardisation for `SPARSE_MAP` (same treatment as `MAP` and `JSON`). |
| `TableConfigUtils.java` | `pinot-segment-local` | Reject `SPARSE_MAP` columns in star-tree index config. |
| `ColumnMinMaxValueGenerator.java` | `pinot-segment-local` | Skip `SPARSE_MAP` columns — they have no forward index and no meaningful min/max values. |
| `DataFetcher.java` | `pinot-core` | Skip forward-index validation for `MapDataSource` columns (including `SPARSE_MAP`); per-key data sources are resolved lazily through `ProjectionBlock`. |
| `MapFilterOperator.java` | `pinot-core` | Detect `SparseMapDataSource` and delegate to `SparseMapFilterOperator` for EQ/NOT_EQ/IN/NOT_IN predicates; fall back to `ExpressionFilterOperator` for range and other predicate types. |
| `SelectionOperatorUtils.java` | `pinot-core` | Handle `SPARSE_MAP` alongside `MAP` in DataTable set/get column operations. |

---

## Query Support

| Query Pattern | Mechanism | Status |
|---|---|---|
| `SELECT col['key']` | `ItemTransformFunction` → `SparseMapDataSource.getKeyDataSource()` → `SparseMapKeyForwardIndexReader` | Supported |
| `WHERE col['key'] = 'v'` | `SparseMapFilterOperator` via inverted index | Supported |
| `WHERE col['key'] IN (...)` | `SparseMapFilterOperator` via inverted index | Supported |
| `WHERE col['key'] != 'v'` | `SparseMapFilterOperator` via inverted index. **Docs where the key is absent are excluded** (SQL NULL semantics: NULL != X is unknown, not true). | Supported |
| `WHERE col['key'] NOT IN (...)` | `SparseMapFilterOperator` via inverted index. **Docs where the key is absent are excluded** (same absence-exclusion as NOT_EQ). | Supported |
| `WHERE col['key'] >= n` | `ExpressionFilterOperator` (full scan via forward index) | Supported |
| `WHERE col['key'] BETWEEN a AND b` | `ExpressionFilterOperator` (full scan via forward index) | Supported |
| `SUM/AVG/COUNT(col['key'])` | Expression aggregation via `SparseMapKeyForwardIndexReader` | Supported |
| `GROUP BY col['key']` | Expression group-by via `SparseMapKeyForwardIndexReader` | Supported |
| `SELECT *` | Requires direct projection of the SPARSE_MAP column value | **Not supported** — use `col['key']` syntax |

Absent keys return the type zero-default (0, 0.0, `""`) from the forward index reader. Use the
inverted-index-backed `NOT_EQ` predicate to exclude absent keys from result sets — absent-key
docs are already excluded from `NOT_EQ` / `NOT_IN` results by design (SQL NULL semantics).

---

## Test Coverage

| Test Class | Module | What it covers |
|---|---|---|
| `SparseMapDataTypeTest` | `pinot-spi` | DataType properties, null placeholder, `SparseMapFieldSpec` key types, Schema validation (accept/reject), `hasSparseMapColumn()`, default null value, JSON round-trip (verifies `sparseMapFieldSpecs` array and `fieldType: "SPARSE_MAP"`). **11 tests.** |
| `SparseMapIndexTest` | `pinot-segment-local` | Basic int/string keys, `getMap()`, absent key, inverted index lookup, mutable index CRUD; magic-byte corruption detection, full byte-order round-trip for INT/LONG/FLOAT/DOUBLE; null-value-treated-as-absent (immutable + mutable); maxKeys excess-drop behavior (immutable + mutable); concurrent add+read under `ReentrantReadWriteLock`; maxKeys exact retention; 500-doc sparse index. **14 tests.** |
| `SparseMapIndexEndToEndTest` | `pinot-segment-local` | Full user-metrics scenario (create→seal→read), undeclared key default type, mutable→immutable round-trip, key-union across simulated segment merge. **4 tests.** |
| `SparseMapSegmentCreationTest` | `pinot-segment-local` | Full segment creation pipeline (`SegmentIndexCreationDriverImpl`); verifies `.sparsemap.idx` appears in the V3 `index_map`. **1 test.** |
| `SparseMapFilterOperatorTest` | `pinot-core` | NOT_EQ excludes absent-key docs; NOT_IN excludes absent-key docs; NOT_EQ returns empty when all docs lack the key. **3 tests.** |
| `SparseMapClusterIntegrationTest` | `pinot-integration-tests` | Full embedded cluster (ZK + controller + broker + server); covers COUNT, regular-column projection/filter, GROUP BY, EQ/IN/NOT_EQ/RANGE/BETWEEN filters on SPARSE_MAP keys, key projection, multiple key projection, SUM aggregation, GROUP BY SPARSE_MAP key, combined projection + range filter. **19 tests.** |

Total: **52 tests**, all passing.

---

## Key Algorithmic Details

### O(1) Value Lookup

Values for key `k` are stored in a dense array (one entry per document that has key `k`).
The position of document `d`'s value is:

```
ordinal = presenceBitmap[k].rank(d) - 1   // 0-based
value   = values[k][ordinal]
```

`rank(d)` returns the count of set bits ≤ `d`, which equals the 1-based rank of `d` in the
bitmap. Subtracting 1 gives the 0-based array index.

### O(1) Key Presence Check

```
hasKey(d, k) = presenceBitmap[k].contains(d)
```

### Fast Filter via Inverted Index (EQ/IN/NOT_EQ/NOT_IN)

When `enableInvertedIndex: true`, the index stores a `TreeMap<String, ImmutableRoaringBitmap>` per
key mapping serialised value strings to the set of documents with that value:

```
docsWithKeyValue(k, v) = invertedIndex[k][toString(v)]
```

`SparseMapFilterOperator` combines these bitmaps for EQ, NOT_EQ, IN, NOT_IN predicates without
scanning any document values.

### Range Filter via Expression Evaluation

For predicates not supported by the inverted index (RANGE, BETWEEN, REGEXP, etc.),
`MapFilterOperator` falls back to `ExpressionFilterOperator`. This evaluates `col['key']`
for every document using `SparseMapKeyForwardIndexReader`, which performs a bitmap-rank lookup
per document. Documents without the key return the zero-default and typically fail the predicate.

### Dictionary-Based GROUP BY

When a key has an inverted index enabled, the index builder automatically creates:

1. **A value dictionary** (`SparseMapKeyDictionary`) — a sorted array of all distinct string
   representations of the key's values, including the type-appropriate default value.
2. **A bit-packed dictId forward index** — one dictId per document in the segment (not just per
   document that has the key), stored as `ceil(log2(numDistinctValues))` bits per entry.

At query time, `SparseMapKeyForwardIndexReader.readDictIds()` reads dictIds from the bit-packed
array via `FixedBitIntReaderWriter.readInt(docId)` — an O(1) operation per document. This
enables the standard Pinot dictionary-based GROUP BY path, avoiding the slower per-document
value extraction through bitmap-rank + forward index read.

For mutable (real-time) segments, `readDictIds()` falls back to `getString() + dictionary.indexOf()`
since the bit-packed dictId forward index is only built during segment seal.

### Full Map Reconstruction

```
getMap(d) = { k → values[k][rank(d, k) - 1] : k ∈ keys, presenceBitmap[k].contains(d) }
```

---

### maxKeys Enforcement and Logging

When the `maxKeys` limit is reached, keys beyond the limit are silently dropped from the index.
Both the immutable creator and the mutable index emit `WARN`-level log messages:

- **`OnHeapSparseMapIndexCreator`**: logs once per unique dropped key name with the count of
  distinct dropped keys.
- **`MutableSparseMapIndexImpl`**: logs on the first drop and every 1000th subsequent drop,
  including the column name, limit, and total drop count.

---

## Segment Merge Behavior

When segments are merged (e.g., via the Minion merge task), the `SPARSE_MAP` index is rebuilt
from scratch by re-ingesting all documents through `OnHeapSparseMapIndexCreator`. The merged
index contains the **union of all keys** observed across the merged segments, subject to the
`maxKeys` limit configured for the merged segment. No explicit merge handler logic is required;
key-set union happens naturally as documents from all segments are added in sequence.

---

## Build Commands

```bash
# Install only the changed modules (fastest iterative loop)
./mvnw install -T 1C \
  -pl pinot-spi,pinot-common,pinot-segment-spi,pinot-segment-local,pinot-core,pinot-tools \
  -DskipTests -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true -q

# Rebuild distribution and copy fresh JARs
./mvnw package -pl pinot-tools -Pbin-dist \
  -DskipTests -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true -q
cp pinot-core/target/pinot-core-*.jar          pinot-tools/target/pinot-tools-pkg/lib/
cp pinot-segment-local/target/pinot-segment-local-*.jar pinot-tools/target/pinot-tools-pkg/lib/

# Unit tests
./mvnw test -pl pinot-spi          -Dtest=SparseMapDataTypeTest          -Dcheckstyle.skip
./mvnw test -pl pinot-segment-local -Dtest=SparseMapIndexTest             -Dcheckstyle.skip
./mvnw test -pl pinot-segment-local -Dtest=SparseMapIndexEndToEndTest     -Dcheckstyle.skip
./mvnw test -pl pinot-segment-local -Dtest=SparseMapSegmentCreationTest   -Dcheckstyle.skip
./mvnw test -pl pinot-core          -Dtest=SparseMapFilterOperatorTest    -Dcheckstyle.skip

# Integration test (starts embedded cluster; slow)
./mvnw test -pl pinot-integration-tests \
  -Dtest=SparseMapClusterIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dcheckstyle.skip -Dlicense.skip

# Quickstart (requires distribution package step above)
pkill -9 -f "pinot" 2>/dev/null; sleep 2; rm -rf /tmp/QuickStart*
pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh QuickStart -type SPARSE_MAP
```

---

## Limitations & Future Work

- **`SELECT *` not supported**: Direct projection of the SPARSE_MAP column itself (as `SELECT *`
  would do) is not implemented. Use `col['key']` syntax to project individual keys.
- **No multi-value keys**: Each key holds a single value per document. Multi-value key support
  would require a separate count/offset array per key.
- **No off-heap creator**: Only `OnHeapSparseMapIndexCreator` exists. An off-heap variant would
  reduce GC pressure during large segment creation.
- **Re-ingest required for index addition**: `SparseMapIndexHandler` cannot add the index to an
  existing segment in-place; the segment must be re-ingested.
- **Range filter is a full scan**: Range/BETWEEN predicates use `ExpressionFilterOperator` which
  scans every document. A sorted per-key values structure would allow binary-search range pruning.
- **No segment pruning**: Segment-level min/max for individual keys is not tracked, so segments
  cannot be pruned on key-range predicates.
- **No native aggregation push-down**: `SUM(col['key'])` works via expression evaluation but does
  not use a dedicated aggregation function that could exploit the compact per-key value arrays.
- **`IS NOT NULL` on SPARSE_MAP does a full table scan**: Unlike flattened columns with inverted
  indexes that can skip null values efficiently, `IS NOT NULL` on a SPARSE_MAP key scans all
  documents through the presence bitmap (no dedicated null bitmap). This can be 3.5–12× slower
  than flattened columns at scale (see benchmark below).

---

## SPARSE_MAP vs Flattened Columns — Performance Benchmark

> **Dataset:** ~304M rows · 228 segments · 4 servers · 2 replicas · 5 runs per query · 1s delay
> between runs. Both tables consume the same Kafka topic with identical retention and indexes.

This benchmark compares SPARSE_MAP columns against individual flattened dimension columns storing
the same data (e.g., `riskError` sparse map with keys `code`, `title`, `key` vs three separate
columns `riskError_code`, `riskError_title`, `riskError_key`).

### Results Summary

| # | Query Pattern | Flat avg | Sparse avg | Winner | Speedup |
|---|---|---|---|---|---|
| Q01 | `count(*)` | 69 ms | 9 ms | **SPARSE_MAP** | 7.9× |
| Q02 | EQ filter on regular dim (`eventType`) | 24 ms | 31 ms | FLATTENED | 1.3× |
| Q03 | EQ filter on sparse/flat field (`riskError_code`) | 21 ms | 20 ms | TIE | — |
| Q04 | `IS NOT NULL` on sparse/flat field | 15 ms | 185 ms | **FLATTENED** | **12.0×** |
| Q05 | GROUP BY sparse/flat field | 211 ms | 573 ms | **FLATTENED** | 2.7× |
| Q06 | EQ filter on different sparse field | 25 ms | 15 ms | **SPARSE_MAP** | 1.7× |
| Q07 | GROUP BY different sparse field | 88 ms | 226 ms | **FLATTENED** | 2.6× |
| Q08 | Combined regular dim + sparse filter | 26 ms | 67 ms | **FLATTENED** | 2.6× |
| Q09 | Multi-sparse equality filters | 66 ms | 19 ms | **SPARSE_MAP** | 3.5× |
| Q10 | SELECT multiple sparse fields with filter | 32 ms | 72 ms | FLATTENED | 2.3× |
| Q11 | SUM + GROUP BY sparse field | 803 ms | 392 ms | **SPARSE_MAP** | 2.0× |
| Q12 | `IS NOT NULL` on deeply nested sparse field | 47 ms | 168 ms | **FLATTENED** | 3.5× |
| Q13 | GROUP BY deeply nested sparse field | 162 ms | 253 ms | FLATTENED | 1.6× |
| Q14 | Filter sparse field + GROUP BY regular dim | 63 ms | 12 ms | **SPARSE_MAP** | 5.1× |
| Q15 | Complex multi-filter + aggregation + GROUP BY | 274 ms | 149 ms | **SPARSE_MAP** | 1.8× |

**Flattened wins 8 · Sparse_map wins 6 · Ties 1**

### Key Observations

1. **`IS NOT NULL` is SPARSE_MAP's biggest weakness.** Flattened columns with inverted indexes
   efficiently skip null documents (scanning only ~12.5M matching docs). SPARSE_MAP has no null
   bitmap — `IS NOT NULL` requires scanning all 304M documents through the presence bitmap,
   resulting in 304M filter entries. This produces a **3.5–12× slowdown**.

2. **GROUP BY on sparse map keys is 1.5–2.7× slower** than flattened columns. Despite the
   dictId forward index optimization, the runtime overhead of extracting values from the sparse
   map structure adds cost compared to reading from a native per-column forward index.

3. **Equality filters (`= 'value'`) are comparable.** When both sides have inverted indexes,
   performance is within noise. Some queries slightly favor sparse map due to its compact
   index structure.

4. **Sparse_map excels at aggregations.** `SUM + GROUP BY` (Q11) is 2× faster, likely because
   co-located per-key data reduces memory access overhead compared to reading from separate
   column files.

5. **Sparse_map wins on multi-key intersection queries.** Q09 (two equality filters across
   different sparse map columns) is 3.5× faster, and Q14 (filter sparse field + group by
   regular dim) is 5.1× faster.

6. **Schema simplicity matters.** The sparse_map table has 7 map columns vs 16 extra dimension
   columns in the flattened table. This gives sparse_map an advantage on metadata-heavy
   operations like `count(*)` (Q01: 7.9× faster).

### Decision Guide: SPARSE_MAP vs Flattened Columns

| If your workload is heavy on... | Use |
|---|---|
| `IS NOT NULL` checks or null-aware filtering | **Flattened** |
| GROUP BY on the sparse fields | **Flattened** (unless aggregation-heavy) |
| Equality filters with known values | Either (comparable) |
| Aggregations (SUM, AVG) with GROUP BY | **SPARSE_MAP** |
| Multi-field intersection queries | **SPARSE_MAP** |
| Schema simplicity (many optional fields) | **SPARSE_MAP** |
| Mixed workload | Benchmark your specific queries |
