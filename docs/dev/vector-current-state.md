# Apache Pinot Vector Index: Current State Analysis

> Generated 2026-03-26 by vector-index-archaeologist.
> Target: Comprehensive map of every class, method, and file involved in vector indexing,
> to support multi-backend vector indexing abstraction (phase 1).

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Config Parsing Flow](#2-config-parsing-flow)
3. [Validation Flow](#3-validation-flow)
4. [Segment Build Flow (Immutable/Offline)](#4-segment-build-flow-immutableoffline)
5. [Segment Load Flow](#5-segment-load-flow)
6. [Query Execution Flow](#6-query-execution-flow)
7. [Distance Function Flow](#7-distance-function-flow)
8. [Query Options Plumbing](#8-query-options-plumbing)
9. [Realtime / Mutable Path](#9-realtime--mutable-path)
10. [Lucene/HNSW Coupling Points](#10-lucenehnsw-coupling-points)
11. [Refactor Insertion Points](#11-refactor-insertion-points)
12. [File Inventory](#12-file-inventory)

---

## 1. Architecture Overview

The vector index implementation follows Pinot's standard `IndexType` plugin framework:

```
SPI Layer (pinot-segment-spi)
  IndexPlugin  -->  IndexType<VectorIndexConfig, VectorIndexReader, VectorIndexCreator>
  VectorIndexConfig      -- config POJO (distance function, dimension, type, properties)
  VectorIndexCreator     -- SPI interface for building indexes
  VectorIndexReader      -- SPI interface for reading indexes (returns RoaringBitmap)

Implementation Layer (pinot-segment-local)
  VectorIndexPlugin      -- ServiceLoader entry point (@AutoService)
  VectorIndexType        -- The IndexType implementation (factory, validation, reader factory)
  HnswVectorIndexCreator -- Lucene HNSW writer (immutable segments)
  HnswVectorIndexReader  -- Lucene HNSW reader (immutable segments)
  MutableVectorIndex     -- Lucene HNSW for realtime mutable segments
  VectorIndexUtils       -- Lucene codec/config helpers, distance function mapping
  HnswCodec              -- Custom Lucene codec allowing higher dimensions
  HnswVectorsFormat      -- Custom KnnVectorsFormat wrapping Lucene99HnswVectorsFormat

Query Layer (pinot-core, pinot-common)
  VectorSimilarityPredicate   -- Predicate POJO (column, vector, topK)
  VectorSimilarityFilterOperator -- Filter operator that calls VectorIndexReader
  FilterPlanNode               -- Dispatches VECTOR_SIMILARITY predicates
```

**Current limitation**: The implementation is 100% Lucene HNSW. The `VectorIndexType.validate()` method
hard-checks `"HNSW".equals(vectorIndexType)`, and all creator/reader factories directly instantiate
Lucene-specific classes.

---

## 2. Config Parsing Flow

### Call Chain

1. **Table config JSON** contains either:
   - **Modern format**: `fieldConfigList[].indexes.vector_index = { vectorIndexType, vectorDimension, vectorDistanceFunction, version, properties }`
   - **Legacy format**: `fieldConfigList[].indexTypes = ["VECTOR"]` with `properties = { vectorIndexType: "HNSW", vectorDimension: "1536", ... }`

2. `AbstractIndexType#getConfig` (file: `pinot-segment-spi/.../index/AbstractIndexType.java`, line ~72)
   - Lazily initializes `_deserializer` via `createDeserializer()`
   - Calls `_deserializer.deserialize(tableConfig, schema)`

3. `AbstractIndexType#createDeserializer` (file: `pinot-segment-spi/.../index/AbstractIndexType.java`, line ~45)
   - Merges two deserializers with `withExclusiveAlternative`:
     - **Modern path**: `IndexConfigDeserializer.fromIndexes("vector", VectorIndexConfig.class)` -- reads `fieldConfigList[].indexes.vector_index` JSON node and deserializes with Jackson
     - **Legacy path**: `VectorIndexType#createDeserializerForLegacyConfigs` (see below)

4. `VectorIndexType#createDeserializerForLegacyConfigs` (file: `pinot-segment-local/.../index/vector/VectorIndexType.java`, line ~97)
   - `IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.VECTOR, (tableConfig, fieldConfig) -> new VectorIndexConfig(fieldConfig.getProperties()))`
   - Checks if `FieldConfig.IndexType.VECTOR` is in the field's `indexTypes` list
   - Creates `VectorIndexConfig` from the `properties` map

5. `VectorIndexConfig` constructor from `Map<String, String>` (file: `pinot-segment-spi/.../creator/VectorIndexConfig.java`, line ~58)
   - Reads `vectorIndexType`, `vectorDimension`, `vectorDistanceFunction`, `version` from properties
   - Default distance function: `COSINE`
   - Default version: `"1"`

### Key Classes

| Class | File | Purpose |
|---|---|---|
| `VectorIndexConfig` | `pinot-segment-spi/.../creator/VectorIndexConfig.java` | Config POJO with vectorIndexType, vectorDimension, vectorDistanceFunction, version, properties |
| `VectorIndexConfig.VectorDistanceFunction` | Same file, line ~140 | Enum: `COSINE, INNER_PRODUCT, EUCLIDEAN, DOT_PRODUCT` |
| `AbstractIndexType` | `pinot-segment-spi/.../index/AbstractIndexType.java` | Generic deserialization framework |
| `IndexConfigDeserializer` | `pinot-segment-spi/.../index/IndexConfigDeserializer.java` | Utility for building ColumnConfigDeserializer from modern/legacy config formats |
| `FieldConfig` | `pinot-spi/.../config/table/FieldConfig.java` | Contains `IndexType.VECTOR` enum value |

### Notes
- The config supports arbitrary `properties` map for backend-specific parameters. This is already a good extension point -- HNSW-specific params like `maxCon`, `beamWidth`, `maxDimensions` are passed through `properties`.
- The modern JSON format (`indexes.vector_index`) uses Jackson `@JsonCreator` deserialization directly.
- The legacy format uses `FieldConfig.IndexType.VECTOR` enum + `properties` map.

---

## 3. Validation Flow

### Call Chain

1. `VectorIndexType#validate` (file: `pinot-segment-local/.../index/vector/VectorIndexType.java`, line ~77)
   - Called during table config validation
   - Checks:
     - Column must be **multi-value** (NOT single-value): `!fieldSpec.isSingleValueField()`
     - Stored type must be **FLOAT**: `fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.FLOAT`
     - **HARD-CODED**: vectorIndexType must be `"HNSW"`: `"HNSW".equals(vectorIndexType)`

2. `VectorIndexType#createIndexCreator` (file: same, line ~103)
   - Additional runtime validation:
     - Data type must be FLOAT and multi-value
     - **HARD-CODED**: vectorIndexType must be `"HNSW"`

3. `VectorIndexType.ReaderFactory#createIndexReader` (file: same, line ~139)
   - Validates data type is FLOAT and not single-value
   - Error message says "HNSW Vector index" -- coupling in error messages

4. `CalciteSqlParser#validateFilter` (file: `pinot-common/.../parsers/CalciteSqlParser.java`, line ~274)
   - SQL-level validation:
     - First argument must be an identifier (column name)
     - Second argument must be a float/double array literal or ARRAYVALUECONSTRUCTOR function
     - Third argument (optional) must be an integer literal (topK)

### Key Coupling Points

- **Line 86-87 in VectorIndexType.java**: `Preconditions.checkState("HNSW".equals(vectorIndexType), ...)` -- this is the primary gate preventing any non-HNSW backend.
- **Line 108-109**: Same hard check in `createIndexCreator`.
- The inner enum `VectorIndexType.IndexType` (line ~167) only has `HNSW`.

---

## 4. Segment Build Flow (Immutable/Offline)

### Call Chain

1. `BaseSegmentCreator#initColSegmentCreationInfo` (file: `pinot-segment-local/.../creator/impl/BaseSegmentCreator.java`, line ~147)
   - Iterates all columns, calls `createColIndexCreators` for each

2. `BaseSegmentCreator#createColIndexCreators` (file: same, line ~166)
   - Calls `getIndexCreatorsByColumn` to build all index creators

3. `BaseSegmentCreator#getIndexCreatorsByColumn` (file: same, line ~256)
   - Iterates `IndexService.getInstance().getAllIndexes()`
   - For each index type, calls `tryCreateIndexCreator(creatorsByIndex, index, context, config)`
   - This is generic -- works for all index types including vector

4. `IndexType#createIndexCreator` (SPI, file: `pinot-segment-spi/.../index/IndexType.java`, line ~91)
   - Dispatches to `VectorIndexType#createIndexCreator`

5. `VectorIndexType#createIndexCreator` (file: `pinot-segment-local/.../index/vector/VectorIndexType.java`, line ~103)
   - Validates FLOAT multi-value and "HNSW" type
   - Returns `new HnswVectorIndexCreator(column, indexDir, vectorIndexConfig)`

6. `HnswVectorIndexCreator` constructor (file: `pinot-segment-local/.../creator/impl/vector/HnswVectorIndexCreator.java`, line ~56)
   - Converts distance function to Lucene: `VectorIndexUtils.toSimilarityFunction(config.getVectorDistanceFunction())`
   - Creates Lucene `FSDirectory` at `<segDir>/<column>.vector.v912.hnsw.index`
   - Creates Lucene `IndexWriter` with `VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig)`

7. `VectorIndexUtils#getIndexWriterConfig` (file: `pinot-segment-local/.../store/VectorIndexUtils.java`, line ~78)
   - Creates `IndexWriterConfig` with Lucene-specific settings:
     - `maxBufferSizeMB` from properties
     - `commit` flag from properties
     - `useCompoundFile` from properties
     - `maxCon` (HNSW max connections) from properties, default: `Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN`
     - `beamWidth` from properties, default: `Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH`
     - `maxDimensions` from properties, default: `HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS` (2048)
   - Creates custom `HnswVectorsFormat` (wraps `Lucene99HnswVectorsFormat`)
   - Creates `HnswCodec` with the format

8. Per-row indexing: `HnswVectorIndexCreator#add(float[])` (file: same, line ~85)
   - Creates `Document` with `XKnnFloatVectorField` (custom Lucene field allowing higher dimensions)
   - Stores Pinot docId as `StoredField("DocID", nextDocId++)`
   - Calls `_indexWriter.addDocument(docToIndex)`

9. `HnswVectorIndexCreator#seal()` (file: same, line ~100)
   - Calls `_indexWriter.forceMerge(1)` to create single Lucene segment

10. `HnswVectorIndexCreator#close()` (file: same, line ~110)
    - Closes `_indexWriter` (commits based on config flag)
    - Closes `_indexDirectory`

### On-disk Layout

The vector index is stored as a **Lucene index directory**:
```
<segmentDir>/<column>.vector.v912.hnsw.index/
    segments_1
    _0.si
    _0_Lucene99_0.vex    (HNSW graph)
    _0_Lucene99_0.vec    (vector data)
    _0_Lucene99_0.vemf   (vector metadata)
    _0.cfs / _0.cfe      (compound file, if enabled)
    write.lock
```

After segment load, a mapping file is also created:
```
<segmentDir>/<column>.vector.hnsw.mapping
```

### Key Classes

| Class | File | Purpose | Coupling |
|---|---|---|---|
| `HnswVectorIndexCreator` | `pinot-segment-local/.../creator/impl/vector/HnswVectorIndexCreator.java` | Lucene HNSW index writer | **TIGHT** -- directly uses Lucene IndexWriter, Document, KnnFloatVectorField |
| `XKnnFloatVectorField` | `pinot-segment-local/.../creator/impl/vector/XKnnFloatVectorField.java` | Custom Lucene field with 2048 max dims | **TIGHT** -- extends `KnnFloatVectorField` |
| `HnswCodec` | `pinot-segment-local/.../creator/impl/vector/lucene99/HnswCodec.java` | Custom Lucene codec | **TIGHT** -- extends `FilterCodec`, uses Lucene912Codec |
| `HnswVectorsFormat` | `pinot-segment-local/.../creator/impl/vector/lucene99/HnswVectorsFormat.java` | Custom KnnVectorsFormat wrapper | **TIGHT** -- extends `KnnVectorsFormat`, delegates to `Lucene99HnswVectorsFormat` |
| `VectorIndexUtils` | `pinot-segment-local/.../store/VectorIndexUtils.java` | Shared utilities | **TIGHT** -- builds Lucene IndexWriterConfig, maps distance functions to Lucene enums |

---

## 5. Segment Load Flow

### Call Chain

1. `VectorIndexType.ReaderFactory#createIndexReader` (file: `pinot-segment-local/.../index/vector/VectorIndexType.java`, line ~139)
   - Validates column is FLOAT multi-value
   - Gets `segmentDir` from `segmentReader`
   - Returns `new HnswVectorIndexReader(column, segmentDir, numDocs, indexConfig)`

2. `HnswVectorIndexReader` constructor (file: `pinot-segment-local/.../readers/vector/HnswVectorIndexReader.java`, line ~57)
   - Calls `getVectorIndexFile(indexDir)` to find the Lucene index directory

3. `HnswVectorIndexReader#getVectorIndexFile` (file: same, line ~88)
   - Delegates to `SegmentDirectoryPaths.findVectorIndexIndexFile(segmentIndexDir, column)`

4. `SegmentDirectoryPaths#findVectorIndexIndexFile` (file: `pinot-segment-spi/.../store/SegmentDirectoryPaths.java`, line ~144)
   - Looks for index files in order of precedence:
     1. `<column>.vector.v912.hnsw.index` (current version)
     2. `<column>.vector.v99.hnsw.index` (older version)
     3. `<column>.vector.hnsw.index` (oldest version)
   - Uses `findFormatFile` which checks both V1 and V3 directory layouts

5. Back in `HnswVectorIndexReader` constructor:
   - Opens `FSDirectory` on the index file
   - Opens `DirectoryReader` (Lucene `IndexReader`)
   - Creates `IndexSearcher`
   - Creates `DocIdTranslator` -- builds Lucene docId -> Pinot docId mapping

6. `DocIdTranslator` constructor (file: same, line ~145)
   - Creates or loads a memory-mapped file `<column>.vector.hnsw.mapping`
   - If mapping file does not exist (first load): iterates all Lucene docs, reads stored `DocID` field, builds mapping
   - If mapping file exists (reload/restart): memory-maps it read-only
   - Uses `PinotDataBuffer.mapFile()` with `LITTLE_ENDIAN` byte order
   - File size: `4 bytes * numDocs`

### V1-to-V3 Format Conversion

`SegmentV1V2ToV3FormatConverter#copyVectorIndexIfExists` (file: `pinot-segment-local/.../converter/SegmentV1V2ToV3FormatConverter.java`, line ~264)
- Vector index is explicitly excluded from the standard `copyIndexIfExists` path (line ~164)
- Instead, the entire Lucene index directory is copied file-by-file to the V3 subdirectory
- Copies all files in `<column>.vector.v912.hnsw.index/` directory

### Segment Store Integration

In `FilePerIndexDirectory` (file: `pinot-segment-local/.../store/FilePerIndexDirectory.java`):
- `removeIndex`: Special-cases `StandardIndexes.vector()` to call `VectorIndexUtils.cleanupVectorIndex` (line ~106)
- This is necessary because vector indexes are directories, not single files

In `SingleFileIndexDirectory` (file: `pinot-segment-local/.../store/SingleFileIndexDirectory.java`):
- `hasIndexFor`: Special-cases vector to call `VectorIndexUtils.hasVectorIndex` (line ~158)
- `removeIndex`: Special-cases vector to call `VectorIndexUtils.cleanupVectorIndex` (line ~453)
- `getColumnsWithIndex`: Special-cases vector to scan all columns (line ~475)
- Vector indexes are NOT stored inside the single-file index; they remain as separate directories

---

## 6. Query Execution Flow

### Call Chain

#### Step 1: SQL Parsing

1. **Calcite parser** parses SQL with `VECTOR_SIMILARITY(column, ARRAY[...], topK)` in the WHERE clause

2. `PinotOperatorTable` (file: `pinot-query-planner/.../sql/fun/PinotOperatorTable.java`, line ~297)
   - Registers `VECTOR_SIMILARITY` as a `PinotSqlFunction` with signature `(ARRAY, ARRAY, INTEGER)` where third arg is optional
   - Return type: `BOOLEAN`

3. `CalciteSqlParser#validateFilter` (file: `pinot-common/.../parsers/CalciteSqlParser.java`, line ~274)
   - Validates the VECTOR_SIMILARITY filter expression structure

#### Step 2: Predicate Rewriting

4. `PredicateComparisonRewriter#updateFunctionExpression` (file: `pinot-common/.../parsers/rewriter/PredicateComparisonRewriter.java`, line ~126)
   - Handles `VECTOR_SIMILARITY` case
   - Validates 2-3 operands
   - Validates second operand is float/double array literal or ARRAYVALUECONSTRUCTOR
   - Validates optional third operand is a literal (topK)

#### Step 3: Predicate Construction

5. `RequestContextUtils#getFilter` (from Thrift Expression path) (file: `pinot-common/.../request/context/RequestContextUtils.java`, line ~267)
   - `case VECTOR_SIMILARITY:`
   - Extracts `lhs` (column expression), `vectorValue` (float array), `topK` (default 10)
   - Creates `VectorSimilarityPredicate(lhs, vectorValue, topK)`

6. `RequestContextUtils#getFilter` (from ExpressionContext path) (file: same, line ~451)
   - Same logic but operating on `ExpressionContext` objects instead of Thrift

7. `RequestContextUtils#getVectorValue` (file: same, line ~478 and ~491)
   - Two overloads:
     - From `ExpressionContext`: extracts float values from function arguments (ARRAYVALUECONSTRUCTOR)
     - From `Expression` (Thrift): handles `intArrayValue`, `longArrayValue`, `floatArrayValue`, `doubleArrayValue`, and `ARRAYVALUECONSTRUCTOR` function

#### Step 4: Filter Plan Node

8. `FilterPlanNode#constructPhysicalOperator` (file: `pinot-core/.../plan/FilterPlanNode.java`, line ~341)
   - `case VECTOR_SIMILARITY:`
   - Gets `VectorIndexReader` from `dataSource.getVectorIndex()`
   - Precondition checks that vector index is not null
   - Creates `VectorSimilarityFilterOperator(vectorIndex, (VectorSimilarityPredicate) predicate, numDocs)`

#### Step 5: Filter Operator Execution

9. `VectorSimilarityFilterOperator#getTrues` (file: `pinot-core/.../operator/filter/VectorSimilarityFilterOperator.java`, line ~66)
   - Calls `_vectorIndexReader.getDocIds(_predicate.getValue(), _predicate.getTopK())`
   - Returns `BitmapDocIdSet(_matches, _numDocs)`

10. `HnswVectorIndexReader#getDocIds` (file: `pinot-segment-local/.../readers/vector/HnswVectorIndexReader.java`, line ~98)
    - Creates `KnnFloatVectorQuery(_column, searchQuery, topK)` -- Lucene KNN query
    - Executes with `_indexSearcher.search(knnFloatVectorQuery, docIDCollector)`
    - `HnswDocIdCollector` translates Lucene docIds to Pinot docIds via `DocIdTranslator`

11. `HnswDocIdCollector#collect` (file: `pinot-segment-local/.../readers/vector/HnswDocIdCollector.java`, line ~70)
    - For each Lucene hit: `_docIds.add(_docIdTranslator.getPinotDocId(context.docBase + doc))`
    - Result is a `MutableRoaringBitmap` of matching Pinot docIds

### DataSource Access

- `DataSource#getVectorIndex()` (SPI interface, file: `pinot-segment-spi/.../datasource/DataSource.java`, line ~140)
- `BaseDataSource#getVectorIndex()` (file: `pinot-segment-local/.../datasource/BaseDataSource.java`, line ~145)
  - Returns `getIndex(StandardIndexes.vector())`

### Key Classes

| Class | File | Purpose | Coupling |
|---|---|---|---|
| `VectorSimilarityPredicate` | `pinot-common/.../predicate/VectorSimilarityPredicate.java` | Predicate POJO: column, float[] vector, int topK | LOOSE -- no Lucene types |
| `VectorSimilarityFilterOperator` | `pinot-core/.../filter/VectorSimilarityFilterOperator.java` | Filter operator wrapping VectorIndexReader | LOOSE -- uses SPI VectorIndexReader |
| `HnswDocIdCollector` | `pinot-segment-local/.../readers/vector/HnswDocIdCollector.java` | Lucene Collector that translates docIds | **TIGHT** -- implements Lucene `Collector` |

---

## 7. Distance Function Flow

### Pinot Distance Function Enum

`VectorIndexConfig.VectorDistanceFunction` (file: `pinot-segment-spi/.../creator/VectorIndexConfig.java`, line ~140)
- `COSINE` -- cosine similarity (1 - cosine)
- `INNER_PRODUCT` -- maximum inner product
- `EUCLIDEAN` -- L2 squared distance
- `DOT_PRODUCT` -- raw dot product

### Mapping to Lucene

`VectorIndexUtils#toSimilarityFunction` (file: `pinot-segment-local/.../store/VectorIndexUtils.java`, line ~62)

| Pinot | Lucene `VectorSimilarityFunction` |
|---|---|
| `COSINE` | `VectorSimilarityFunction.COSINE` |
| `INNER_PRODUCT` | `VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT` |
| `EUCLIDEAN` | `VectorSimilarityFunction.EUCLIDEAN` |
| `DOT_PRODUCT` | `VectorSimilarityFunction.DOT_PRODUCT` |

### Scalar Distance Functions (runtime computation, NOT index)

`VectorFunctions` (file: `pinot-common/.../function/scalar/VectorFunctions.java`)
- These are `@ScalarFunction` annotated methods used as SQL UDFs (SELECT-clause, not WHERE-clause)
- `cosineDistance(float[], float[])` -- returns `1 - (dot / (norm1 * norm2))`
- `cosineDistance(float[], float[], double defaultValue)` -- same with default for zero-norm vectors
- `innerProduct(float[], float[])` -- dot product
- `l2Distance(float[], float[])` -- Euclidean distance (sqrt of sum of squares)
- `l1Distance(float[], float[])` -- Manhattan distance
- `euclideanDistance(float[], float[])` -- L2 squared (no sqrt)
- `dotProduct(float[], float[])` -- same as innerProduct
- `vectorDims(float[])` -- returns vector.length
- `vectorNorm(float[])` -- L2 norm

### Notes
- The scalar functions are used for **runtime distance computation** in SELECT projections, independent of the index.
- The index distance function is configured at index creation time and is baked into the Lucene index.
- There is a subtle difference: `euclideanDistance` returns L2 squared while `l2Distance` returns actual L2. The Lucene `EUCLIDEAN` also uses squared distance internally.

---

## 8. Query Options Plumbing

### Current State

**There are no vector-specific query options.** A grep for "vector" in `QueryOptionsUtils.java` returns no matches.

The only query-time parameter is `topK`, which is passed as the third argument to `VECTOR_SIMILARITY(column, vector, topK)` in SQL syntax. It flows through:

1. SQL parse -> `PredicateComparisonRewriter` -> validates literal
2. `RequestContextUtils` -> extracts as int -> `VectorSimilarityPredicate.topK`
3. `VectorSimilarityFilterOperator` -> passes to `VectorIndexReader.getDocIds(vector, topK)`

### Implications for Multi-Backend

For future backends like IVF_FLAT, query-time parameters such as `nprobe` would need to be threaded through. Possible approaches:
- Extend `VectorSimilarityPredicate` with an options map
- Use query options (`SET vectorSearchNprobe = 10`)
- Encode in the SQL function call (`VECTOR_SIMILARITY(col, vec, topK, 'nprobe=10')`)

---

## 9. Realtime / Mutable Path

### MutableVectorIndex

`MutableVectorIndex` (file: `pinot-segment-local/.../realtime/impl/vector/MutableVectorIndex.java`)

This implements **both** `VectorIndexReader` and `MutableIndex`, providing a combined writer+reader for realtime segments.

#### Creation

`VectorIndexType#createMutableIndex` (file: `pinot-segment-local/.../index/vector/VectorIndexType.java`, line ~155)
- Checks config is enabled, column is FLOAT multi-value
- Returns `new MutableVectorIndex(segmentName, column, config)`

#### Constructor (line ~71)
- Creates a **temporary directory** under system temp: `new File(FileUtils.getTempDirectory(), segmentName)`
- Opens Lucene `FSDirectory` and `IndexWriter` in the temp directory
- Uses same `VectorIndexUtils.getIndexWriterConfig()` as immutable path
- Immediately commits the empty index to make it searchable
- Reads `commitIntervalMs` (default 10s) and `commitDocs` (default 1000) from properties

#### Write Path: `add(Object[] values, int[] dictIds, int docId)` (line ~103)
- Converts Object[] to float[]
- Creates Lucene `Document` with `XKnnFloatVectorField` and stored `DocID`
- Adds document to `IndexWriter`
- Commits periodically based on time interval or doc count threshold
- Thread safety: designed for single writer

#### Read Path: `getDocIds(float[] vector, int topK)` (line ~127)
- Creates a **new** `IndexSearcher` on every query (opens a new `DirectoryReader`)
- Creates `KnnFloatVectorQuery` and searches
- Collects results via `TopDocs` (NOT the custom `HnswDocIdCollector`)
- **IMPORTANT**: Uses `scoreDoc.doc` directly as Pinot docId -- does NOT use DocIdTranslator
- This works because the mutable index uses sequential docIds that match Pinot's

#### Close (line ~142)
- Commits and closes IndexWriter
- Deletes the temporary directory

### Key Differences from Immutable Path

| Aspect | Immutable | Mutable |
|---|---|---|
| Storage location | Segment directory | System temp directory |
| IndexWriter lifecycle | Created, sealed with forceMerge(1), closed | Long-lived, periodic commits |
| DocId mapping | Requires DocIdTranslator | Direct mapping (scoreDoc.doc) |
| Search | Uses HnswDocIdCollector | Uses TopDocs |
| Thread safety | Not applicable (sealed) | Single writer, multiple readers |
| Index refresh | Not needed | Opens new DirectoryReader per query |

---

## 10. Lucene/HNSW Coupling Points

This section catalogs every place where Lucene or HNSW is assumed or referenced. Each must be evaluated for abstraction.

### HARD COUPLINGS (must change for multi-backend)

| # | Location | File | Line(s) | Nature |
|---|---|---|---|---|
| 1 | `VectorIndexType#validate` | `.../index/vector/VectorIndexType.java` | ~86 | `"HNSW".equals(vectorIndexType)` hard check |
| 2 | `VectorIndexType#createIndexCreator` | Same | ~108 | `"HNSW".equals(...)` hard check, returns `new HnswVectorIndexCreator(...)` |
| 3 | `VectorIndexType.ReaderFactory#createIndexReader` | Same | ~149 | Returns `new HnswVectorIndexReader(...)` |
| 4 | `VectorIndexType#createMutableIndex` | Same | ~164 | Returns `new MutableVectorIndex(...)` |
| 5 | `VectorIndexType.IndexType` enum | Same | ~167 | Only has `HNSW` |
| 6 | `VectorIndexUtils#toSimilarityFunction` | `.../store/VectorIndexUtils.java` | ~62 | Maps to `VectorSimilarityFunction` (Lucene type) |
| 7 | `VectorIndexUtils#getIndexWriterConfig` | Same | ~78 | Builds Lucene `IndexWriterConfig` with HNSW params |

### IMPLEMENTATION COUPLINGS (Lucene types in implementation classes)

| # | Class | Lucene Types Used |
|---|---|---|
| 8 | `HnswVectorIndexCreator` | `IndexWriter`, `Directory`, `FSDirectory`, `Document`, `StoredField`, `VectorSimilarityFunction` |
| 9 | `HnswVectorIndexReader` | `IndexReader`, `DirectoryReader`, `Directory`, `FSDirectory`, `IndexSearcher`, `KnnFloatVectorQuery`, `Collector`, `QueryParser` |
| 10 | `HnswDocIdCollector` | `Collector`, `LeafCollector`, `LeafReaderContext`, `Scorable`, `ScoreMode` |
| 11 | `XKnnFloatVectorField` | `KnnFloatVectorField`, `FieldType`, `VectorEncoding`, `VectorSimilarityFunction` |
| 12 | `HnswCodec` | `FilterCodec`, `Lucene912Codec`, `PostingsFormat`, `DocValuesFormat`, `KnnVectorsFormat`, `PointsFormat`, `PerField*Format` |
| 13 | `HnswVectorsFormat` | `KnnVectorsFormat`, `Lucene99HnswVectorsFormat`, `KnnVectorsReader`, `KnnVectorsWriter` |
| 14 | `MutableVectorIndex` | `IndexWriter`, `FSDirectory`, `DirectoryReader`, `IndexSearcher`, `KnnFloatVectorQuery`, `TopDocs`, `Document`, `StoredField` |
| 15 | `VectorIndexUtils#cleanupVectorIndex` | Uses Lucene-specific file extensions (`.vector.hnsw.index`) |

### LEAKS INTO PUBLIC/SPI APIs (most critical for abstraction)

| # | Location | Issue |
|---|---|---|
| 16 | `VectorIndexReader#getDocIds` returns `ImmutableRoaringBitmap` | NOT a Lucene leak -- this is clean |
| 17 | `VectorIndexCreator#add(float[])` | Clean SPI interface |
| 18 | `VectorIndexConfig` | Clean -- no Lucene types |
| 19 | Error messages reference "HNSW" | `.../VectorIndexType.java` lines ~86, ~109, ~144 |

**Verdict**: The SPI layer (`VectorIndexReader`, `VectorIndexCreator`, `VectorIndexConfig`) is **already clean** of Lucene types. The coupling is entirely in the `pinot-segment-local` implementation layer, concentrated in `VectorIndexType` (the factory), `VectorIndexUtils`, and the `HnswVectorIndex*` classes.

---

## 11. Refactor Insertion Points

Ranked by impact and risk.

### IP-1: VectorIndexType Factory Methods (PRIMARY)

**File**: `pinot-segment-local/.../index/vector/VectorIndexType.java`
**Methods**: `createIndexCreator`, `ReaderFactory#createIndexReader`, `createMutableIndex`, `validate`
**Risk**: LOW
**Why**: This is the single dispatch point. Currently it hard-codes `new HnswVectorIndexCreator(...)` etc. Replacing this with a registry/switch on `vectorIndexType` string is the minimal-risk change to enable multiple backends.
**What could break**: Nothing if HNSW remains the default path. All existing configs that specify `"HNSW"` continue to work.
**Approach**:
```java
// In createIndexCreator:
switch (indexConfig.getVectorIndexType()) {
  case "HNSW":
    return new HnswVectorIndexCreator(column, indexDir, indexConfig);
  case "IVF_FLAT":
    return new IvfFlatVectorIndexCreator(column, indexDir, indexConfig);
  default:
    throw new IllegalArgumentException("Unsupported vector index type: " + type);
}
```

### IP-2: VectorIndexType#validate

**File**: `pinot-segment-local/.../index/vector/VectorIndexType.java`
**Method**: `validate` (line ~77)
**Risk**: LOW
**Why**: Remove the `"HNSW".equals(vectorIndexType)` check and replace with a set of supported types.
**What could break**: Invalid configs that were previously rejected might now be accepted. Mitigate by maintaining an explicit allowlist.

### IP-3: VectorIndexUtils -- Distance Function Mapping

**File**: `pinot-segment-local/.../store/VectorIndexUtils.java`
**Method**: `toSimilarityFunction` (line ~62)
**Risk**: MEDIUM
**Why**: Currently returns Lucene `VectorSimilarityFunction`. A new backend would need its own distance function type. Consider making this method backend-specific or introducing a Pinot-level distance function abstraction.
**What could break**: Any code that calls `toSimilarityFunction` and expects a Lucene type. Currently only `HnswVectorIndexCreator` and `MutableVectorIndex` call it.
**Approach**: Keep this method for the HNSW backend; IVF_FLAT would have its own distance function logic.

### IP-4: File Extension Constants

**File**: `pinot-segment-spi/.../V1Constants.java`
**Risk**: LOW
**Why**: New file extensions needed for IVF_FLAT format (e.g., `.vector.ivfflat.index`).
**What could break**: Nothing -- additive change.

### IP-5: VectorIndexUtils -- Index Detection and Cleanup

**File**: `pinot-segment-local/.../store/VectorIndexUtils.java`
**Methods**: `hasVectorIndex`, `cleanupVectorIndex`
**Risk**: LOW-MEDIUM
**Why**: Currently only checks for HNSW-specific file patterns. Must be extended to detect IVF_FLAT files too.
**What could break**: Index detection during segment load could miss new index files if not updated.

### IP-6: SegmentDirectoryPaths#findVectorIndexIndexFile

**File**: `pinot-segment-spi/.../store/SegmentDirectoryPaths.java`
**Method**: `findVectorIndexIndexFile` (line ~144)
**Risk**: MEDIUM
**Why**: Currently hard-codes HNSW file extensions in precedence order. Must be extended for new backends.
**What could break**: Loading segments with new index types if the file lookup does not find them.

### IP-7: VectorIndexHandler (Segment Reprocessing)

**File**: `pinot-segment-local/.../loader/invertedindex/VectorIndexHandler.java`
**Methods**: `createVectorIndexForColumn`, `handleDictionaryBasedColumn`, `handleNonDictionaryBasedColumn`
**Risk**: LOW
**Why**: Uses `StandardIndexes.vector().createIndexCreator(context, config)` which is already backend-agnostic through the SPI. The only coupling is the file extensions in the inProgress marker file (line ~118-119).
**What could break**: The marker file path is HNSW-specific. Must be generalized.

### IP-8: SegmentV1V2ToV3FormatConverter

**File**: `pinot-segment-local/.../converter/SegmentV1V2ToV3FormatConverter.java`
**Method**: `copyVectorIndexIfExists` (line ~264)
**Risk**: LOW-MEDIUM
**Why**: Copies vector index directories during format conversion. Currently scans for `.vector.v912.hnsw.index` suffix only.
**What could break**: V1-to-V3 conversion would not copy new backend index files if patterns are not updated.

### IP-9: SingleFileIndexDirectory / FilePerIndexDirectory

**Files**: Both in `pinot-segment-local/.../store/`
**Risk**: LOW
**Why**: Special-case handling for vector indexes (directories vs single files). If IVF_FLAT uses a different storage format (e.g., single flat files), this may need adjustment.

---

## 12. File Inventory

Complete list of files involved in vector indexing:

### SPI Layer (pinot-segment-spi)

| File | Classes | Role |
|---|---|---|
| `pinot-segment-spi/.../index/StandardIndexes.java` | `StandardIndexes` | `vector()` factory method, `VECTOR_ID = "vector_index"` |
| `pinot-segment-spi/.../index/IndexPlugin.java` | `IndexPlugin` | SPI service interface |
| `pinot-segment-spi/.../index/IndexType.java` | `IndexType` | Generic index type interface |
| `pinot-segment-spi/.../index/AbstractIndexType.java` | `AbstractIndexType` | Base class with deserialization framework |
| `pinot-segment-spi/.../index/IndexService.java` | `IndexService` | ServiceLoader-based plugin registry |
| `pinot-segment-spi/.../index/IndexConfigDeserializer.java` | `IndexConfigDeserializer` | Config deserialization helpers |
| `pinot-segment-spi/.../index/creator/VectorIndexConfig.java` | `VectorIndexConfig`, `VectorDistanceFunction` | Config POJO |
| `pinot-segment-spi/.../index/creator/VectorIndexCreator.java` | `VectorIndexCreator` | Creator SPI |
| `pinot-segment-spi/.../index/reader/VectorIndexReader.java` | `VectorIndexReader` | Reader SPI: `getDocIds(float[], int)` |
| `pinot-segment-spi/.../datasource/DataSource.java` | `DataSource` | `getVectorIndex()` accessor |
| `pinot-segment-spi/.../store/SegmentDirectoryPaths.java` | `SegmentDirectoryPaths` | `findVectorIndexIndexFile()` |
| `pinot-segment-spi/.../V1Constants.java` | `V1Constants.Indexes` | File extension constants |

### Implementation Layer (pinot-segment-local)

| File | Classes | Role |
|---|---|---|
| `pinot-segment-local/.../index/vector/VectorIndexPlugin.java` | `VectorIndexPlugin` | @AutoService SPI entry point |
| `pinot-segment-local/.../index/vector/VectorIndexType.java` | `VectorIndexType`, `ReaderFactory`, `IndexType` | **Central dispatch** -- factory, validation, reader creation |
| `pinot-segment-local/.../creator/impl/vector/HnswVectorIndexCreator.java` | `HnswVectorIndexCreator` | Lucene HNSW index writer |
| `pinot-segment-local/.../creator/impl/vector/XKnnFloatVectorField.java` | `XKnnFloatVectorField` | Custom Lucene field (high-dim support) |
| `pinot-segment-local/.../creator/impl/vector/lucene99/HnswCodec.java` | `HnswCodec` | Custom Lucene codec |
| `pinot-segment-local/.../creator/impl/vector/lucene99/HnswVectorsFormat.java` | `HnswVectorsFormat` | Custom KnnVectorsFormat |
| `pinot-segment-local/.../index/readers/vector/HnswVectorIndexReader.java` | `HnswVectorIndexReader`, `DocIdTranslator` | Lucene HNSW index reader |
| `pinot-segment-local/.../index/readers/vector/HnswDocIdCollector.java` | `HnswDocIdCollector` | Lucene Collector for docId translation |
| `pinot-segment-local/.../realtime/impl/vector/MutableVectorIndex.java` | `MutableVectorIndex` | Realtime Lucene HNSW reader+writer |
| `pinot-segment-local/.../store/VectorIndexUtils.java` | `VectorIndexUtils` | Lucene config builder, distance mapping, file utilities |
| `pinot-segment-local/.../index/loader/invertedindex/VectorIndexHandler.java` | `VectorIndexHandler` | Index reprocessing/rebuilding |
| `pinot-segment-local/.../index/datasource/BaseDataSource.java` | `BaseDataSource` | `getVectorIndex()` implementation |
| `pinot-segment-local/.../store/FilePerIndexDirectory.java` | `FilePerIndexDirectory` | Vector index cleanup |
| `pinot-segment-local/.../store/SingleFileIndexDirectory.java` | `SingleFileIndexDirectory` | Vector index detection/cleanup |
| `pinot-segment-local/.../index/converter/SegmentV1V2ToV3FormatConverter.java` | `SegmentV1V2ToV3FormatConverter` | V1->V3 vector index copy |

### Query Layer (pinot-common, pinot-core, pinot-query-planner)

| File | Classes | Role |
|---|---|---|
| `pinot-common/.../predicate/Predicate.java` | `Predicate.Type` | `VECTOR_SIMILARITY` enum value |
| `pinot-common/.../predicate/VectorSimilarityPredicate.java` | `VectorSimilarityPredicate` | Predicate POJO (column, vector, topK) |
| `pinot-common/.../request/context/RequestContextUtils.java` | `RequestContextUtils` | Predicate construction, `getVectorValue()` |
| `pinot-common/.../function/scalar/VectorFunctions.java` | `VectorFunctions` | Scalar UDFs for distance computation |
| `pinot-common/.../parsers/CalciteSqlParser.java` | `CalciteSqlParser` | SQL validation of VECTOR_SIMILARITY |
| `pinot-common/.../parsers/rewriter/PredicateComparisonRewriter.java` | `PredicateComparisonRewriter` | Predicate normalization |
| `pinot-sql/FilterKind.java` | `FilterKind` | `VECTOR_SIMILARITY` enum value |
| `pinot-core/.../plan/FilterPlanNode.java` | `FilterPlanNode` | Dispatches to VectorSimilarityFilterOperator |
| `pinot-core/.../operator/filter/VectorSimilarityFilterOperator.java` | `VectorSimilarityFilterOperator` | Filter operator wrapping VectorIndexReader |
| `pinot-query-planner/.../sql/fun/PinotOperatorTable.java` | `PinotOperatorTable` | VECTOR_SIMILARITY function registration |

### Config Layer (pinot-spi)

| File | Classes | Role |
|---|---|---|
| `pinot-spi/.../config/table/FieldConfig.java` | `FieldConfig.IndexType` | `VECTOR` enum value |

### Test Files

| File | Purpose |
|---|---|
| `pinot-segment-spi/src/test/.../index/VectorConfigTest.java` | Config serialization/deserialization tests |
| `pinot-segment-local/src/test/.../creator/HnswVectorIndexCreatorTest.java` | Unit test for creator/reader roundtrip |
| `pinot-segment-local/src/test/.../index/VectorIndexTest.java` | Vector index integration test |
| `pinot-integration-tests/src/test/.../custom/VectorTest.java` | Full end-to-end integration test |
| `pinot-core/src/test/.../function/VectorFunctionsTest.java` | Scalar function tests |
| `pinot-core/src/test/.../FunctionRegistryTest.java` | Function registration test |

---

## Summary of Abstraction Requirements

### What is already backend-neutral (no changes needed)

1. `VectorIndexReader` SPI interface -- returns `ImmutableRoaringBitmap`, no Lucene types
2. `VectorIndexCreator` SPI interface -- accepts `float[]`, no Lucene types
3. `VectorIndexConfig` -- has `vectorIndexType` string field, `properties` map for backend-specific params
4. `VectorSimilarityPredicate` -- clean predicate POJO
5. `VectorSimilarityFilterOperator` -- uses `VectorIndexReader` SPI only
6. `FilterPlanNode` dispatch -- gets reader from `DataSource`, no backend assumptions
7. `FilterKind.VECTOR_SIMILARITY` -- backend-agnostic
8. `DataSource#getVectorIndex()` -- returns SPI type
9. The entire query execution path from SQL parse to filter operator is backend-agnostic

### What must change for multi-backend

1. **VectorIndexType#validate** -- remove hard `"HNSW"` check, replace with supported-backends set
2. **VectorIndexType#createIndexCreator** -- dispatch by vectorIndexType string to appropriate creator
3. **VectorIndexType.ReaderFactory#createIndexReader** -- dispatch by index type to appropriate reader
4. **VectorIndexType#createMutableIndex** -- dispatch or return null for backends without mutable support
5. **VectorIndexUtils** -- make backend-specific (or split into backend-specific utils)
6. **V1Constants** -- add new file extensions for IVF_FLAT
7. **SegmentDirectoryPaths#findVectorIndexIndexFile** -- extend for new file patterns
8. **VectorIndexUtils#hasVectorIndex** and `cleanupVectorIndex` -- extend for new file patterns
9. **SegmentV1V2ToV3FormatConverter#copyVectorIndexIfExists** -- extend for new file patterns
10. **SingleFileIndexDirectory / FilePerIndexDirectory** -- may need updates if new backend uses different storage
11. **VectorIndexHandler marker file** -- generalize HNSW-specific extension
