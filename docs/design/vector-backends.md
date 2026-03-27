# Multi-Backend Vector Indexing -- Phase 1 Design

**Status:** Proposed
**Author:** Integration Lead (multi-agent)
**Date:** 2026-03-26

---

## 1. Problem Statement

Apache Pinot currently supports a single vector index backend: Lucene-based HNSW.
This backend is tightly coupled throughout the segment creation, loading, and query
paths. The Lucene HNSW implementation is excellent for moderate-dimensional,
moderate-cardinality workloads, but it has limitations:

- **Memory overhead:** Lucene HNSW builds a full graph structure per segment.
  For very large segments or high-dimensional vectors, memory consumption is high.
- **No algorithm choice:** Users cannot select a different ANN algorithm that
  might better fit their recall/latency/memory trade-off.
- **Tight coupling:** `VectorIndexType`, `VectorIndexHandler`, `FilterPlanNode`,
  and `VectorSimilarityFilterOperator` all assume a single Lucene HNSW backend.
  Adding a new backend requires touching many files.

Phase 1 introduces a backend-neutral abstraction layer and adds IVF_FLAT as the
first alternative ANN backend for immutable (offline) segments only.

### Goals

1. Preserve existing VECTOR_SIMILARITY SQL syntax unchanged.
2. Preserve existing Lucene HNSW behavior for all current users (backward compat).
3. Introduce a backend-neutral vector index abstraction behind which HNSW and new
   backends are plugged.
4. Add IVF_FLAT as the first new ANN backend (immutable segments only).
5. Support COSINE, L2 (EUCLIDEAN), and INNER_PRODUCT distance functions.
6. Support query-time tuning via `SET vector.nprobe = N` query option.
7. Support exact rerank on ANN candidates.
8. Add exact-scan fallback when no ANN index exists.

### Non-Goals (Phase 1)

- IVF_PQ or any product quantization variant.
- Scalar quantization.
- Native Faiss or JNI integration.
- Radius/range ANN syntax changes.
- Advanced filtered-ANN optimizer work.
- Realtime mutable IVF support.

---

## 2. Current Architecture (Audit Summary)

### 2.1 Key Files and Their Roles

| Layer | File | Role |
|-------|------|------|
| **SPI - Config** | `pinot-segment-spi/.../creator/VectorIndexConfig.java` | Stores `vectorIndexType` (String), `vectorDimension`, `vectorDistanceFunction`, and a `properties` map. Already backend-neutral in schema. |
| **SPI - Reader** | `pinot-segment-spi/.../reader/VectorIndexReader.java` | Interface: `getDocIds(float[] vector, int topK) -> ImmutableRoaringBitmap`. Backend-agnostic signature. |
| **SPI - Creator** | `pinot-segment-spi/.../creator/VectorIndexCreator.java` | Interface: `add(float[])`, `seal()`. Backend-agnostic signature. |
| **SPI - IndexType** | `pinot-segment-spi/.../StandardIndexes.java` | Registers `VECTOR_ID = "vector_index"`. Single registration point. |
| **SPI - Constants** | `pinot-segment-spi/.../V1Constants.java` | File extensions for HNSW index files (`.vector.v912.hnsw.index`, etc.) and docid mapping. |
| **Local - IndexType** | `pinot-segment-local/.../vector/VectorIndexType.java` | Extends `AbstractIndexType`. **Hard-codes** `"HNSW".equals(...)` in `validate()`, `createIndexCreator()`. `ReaderFactory` directly instantiates `HnswVectorIndexReader`. |
| **Local - Creator** | `pinot-segment-local/.../creator/impl/vector/HnswVectorIndexCreator.java` | Creates Lucene HNSW index on disk. Uses Lucene `IndexWriter`, `VectorSimilarityFunction`. |
| **Local - Reader** | `pinot-segment-local/.../readers/vector/HnswVectorIndexReader.java` | Opens Lucene directory, performs `KnnFloatVectorQuery`, translates Lucene docIds to Pinot docIds via memory-mapped `DocIdTranslator`. |
| **Local - Handler** | `pinot-segment-local/.../loader/invertedindex/VectorIndexHandler.java` | Segment load-time handler: creates/removes vector indexes. Calls `StandardIndexes.vector().createIndexCreator(...)`. |
| **Local - Utils** | `pinot-segment-local/.../store/VectorIndexUtils.java` | Lucene-specific: `toSimilarityFunction()`, `getIndexWriterConfig()`, file cleanup. |
| **Local - Mutable** | `pinot-segment-local/.../realtime/impl/vector/MutableVectorIndex.java` | Mutable HNSW for realtime segments. Out of scope for phase 1 changes. |
| **Core - Filter** | `pinot-core/.../filter/VectorSimilarityFilterOperator.java` | Receives `VectorIndexReader`, calls `getDocIds(vector, topK)`, returns `BitmapDocIdSet`. |
| **Core - Plan** | `pinot-core/.../plan/FilterPlanNode.java` | On `VECTOR_SIMILARITY` predicate: gets `dataSource.getVectorIndex()`, creates `VectorSimilarityFilterOperator`. **Currently fails if no vector index.** |
| **Common - Predicate** | `pinot-common/.../predicate/VectorSimilarityPredicate.java` | Carries `float[] value` and `int topK`. |

### 2.2 Key Observations

1. **VectorIndexConfig already supports multiple backends in schema.** The
   `vectorIndexType` field is a String, and `properties` is a generic map.
   The coupling is in the code that consumes this config, not the config itself.

2. **The SPI interfaces (VectorIndexReader, VectorIndexCreator) are already
   backend-neutral.** Their signatures do not reference Lucene.

3. **All Lucene coupling is in `pinot-segment-local`:** specifically in
   `VectorIndexType.java` (hard-coded HNSW checks), `HnswVectorIndexCreator`,
   `HnswVectorIndexReader`, `VectorIndexUtils`, and `MutableVectorIndex`.

4. **FilterPlanNode currently hard-fails** when no vector index exists. There is
   no exact-scan fallback path.

5. **No query-option plumbing** exists for vector-specific options like `nprobe`.
   The `VectorSimilarityFilterOperator` receives only `VectorIndexReader` and
   the predicate.

---

## 3. Architecture Overview

```
                     SQL: VECTOR_SIMILARITY(col, [...], topK)
                                    |
                          VectorSimilarityPredicate
                                    |
                      +----- FilterPlanNode -----+
                      |                          |
               has ANN index?            no ANN index
                      |                          |
            VectorSimilarityFilterOp     ExactVectorScanFilterOp
                      |                     (new, phase 1)
               VectorIndexReader
                   (SPI iface)
                      |
            +--------------------+
            |                    |
     HnswVectorIndex     IvfFlatVectorIndex
      Reader (Lucene)       Reader (new)
```

### 3.1 Abstraction Layer

The existing SPI interfaces are already suitable:

- `VectorIndexReader.getDocIds(float[], int)` -- no changes needed.
- `VectorIndexCreator.add(float[])`, `seal()` -- no changes needed.
- `VectorIndexConfig` -- already has `vectorIndexType` and `properties`.

The changes are in the **dispatch logic** inside `VectorIndexType` (the
`pinot-segment-local` class that implements `AbstractIndexType`), which currently
hard-codes HNSW. We will make it dispatch by `vectorIndexType` string.

### 3.2 New Components

| Component | Package | Description |
|-----------|---------|-------------|
| `VectorBackendType` (enum) | `pinot-segment-spi/.../index/creator/` | `HNSW`, `IVF_FLAT` -- validated enum for the `vectorIndexType` string. |
| `IvfFlatVectorIndexCreator` | `pinot-segment-local/.../creator/impl/vector/` | Pure-Java IVF_FLAT index builder. |
| `IvfFlatVectorIndexReader` | `pinot-segment-local/.../readers/vector/` | Pure-Java IVF_FLAT index reader with nprobe support. |
| `ExactVectorScanFilterOperator` | `pinot-core/.../filter/` | Brute-force scan fallback when no ANN index is present. |
| `VectorSearchParams` | `pinot-core/.../query/request/context/` | Carries query options (`nprobe`, `exactRerank`, `maxCandidates`). |

### 3.3 What Does NOT Change

- `VectorSimilarityPredicate` -- unchanged.
- SQL syntax -- unchanged.
- `VectorIndexReader` interface -- unchanged.
- `VectorIndexCreator` interface -- unchanged.
- `VectorIndexConfig` schema -- unchanged (already supports arbitrary types).
- `MutableVectorIndex` -- unchanged (stays HNSW-only for phase 1).
- Lucene HNSW behavior -- unchanged for existing configs.

---

## 4. Detailed Design

### 4.1 VectorBackendType Enum

```java
// pinot-segment-spi/.../index/creator/VectorIndexConfig.java (inner enum)
public enum VectorBackendType {
  HNSW,
  IVF_FLAT;

  public static VectorBackendType fromString(String type) {
    if (type == null) {
      return HNSW; // default for backward compatibility
    }
    return valueOf(type.toUpperCase());
  }
}
```

This enum lives inside (or alongside) `VectorIndexConfig` in the SPI module.
The `fromString` method provides backward compatibility: any existing config
with `vectorIndexType: "HNSW"` or null continues to resolve to HNSW.

### 4.2 VectorIndexType Dispatch Changes

Currently, `VectorIndexType.createIndexCreator()` hard-codes:
```java
Preconditions.checkState("HNSW".equals(indexConfig.getVectorIndexType()), ...);
return new HnswVectorIndexCreator(...);
```

After phase 1:
```java
VectorBackendType backend = VectorBackendType.fromString(
    indexConfig.getVectorIndexType());
switch (backend) {
  case HNSW:
    return new HnswVectorIndexCreator(name, indexDir, indexConfig);
  case IVF_FLAT:
    return new IvfFlatVectorIndexCreator(name, indexDir, indexConfig);
  default:
    throw new IllegalArgumentException("Unsupported vector index type: " + backend);
}
```

Similarly for `ReaderFactory.createIndexReader()` and `validate()`.

### 4.3 IVF_FLAT Index Format (Immutable Segments Only)

IVF_FLAT (Inverted File with Flat vectors) partitions the vector space into
`nlist` Voronoi cells via k-means clustering on the training set, then assigns
each vector to its nearest centroid.

**On-disk layout** (single file per column: `<col>.vector.ivfflat.index`):

```
+------------------+
| Header (fixed)   |  magic, version, nlist, dimension, distanceFunc, numVectors
+------------------+
| Centroids        |  nlist * dimension * 4 bytes (float32)
+------------------+
| Inverted Lists   |  For each centroid i:
|   list_offset[i] |    offset into vector data section
|   list_size[i]   |    number of vectors in this list
+------------------+
| Vector Data      |  For each list: docId (int32) + vector (dim * float32)
+------------------+
```

**Training strategy:** Single-pass k-means++ initialization with configurable
iterations (`properties.ivfTrainIterations`, default 10). For segments with
fewer than `nlist * 39` vectors, fall back to exact scan (no IVF index built).

**Configuration properties:**
- `nlist` (int, default 128): number of Voronoi cells/centroids.
- `ivfTrainIterations` (int, default 10): k-means iteration count.
- `ivfTrainSampleRatio` (float, default 1.0): fraction of vectors to sample for training.

### 4.4 IVF_FLAT Search (nprobe)

At query time, the reader:
1. Computes distance from the query vector to all `nlist` centroids.
2. Selects the `nprobe` closest centroids (default 8).
3. Scans all vectors in those `nprobe` inverted lists.
4. Returns the top-K closest vectors as a `MutableRoaringBitmap`.

The `nprobe` parameter is supplied via query option:
```sql
SET vector.nprobe = 16;
SELECT ... WHERE VECTOR_SIMILARITY(embedding, ARRAY[...], 10)
```

### 4.5 Query Options and VectorSearchParams

New query option keys in `CommonConstants.Request.QueryOptionKey`:
```java
public static final String VECTOR_NPROBE = "vector.nprobe";
public static final String VECTOR_EXACT_RERANK = "vector.exactRerank";
public static final String VECTOR_MAX_CANDIDATES = "vector.maxCandidates";
```

A new `VectorSearchParams` class aggregates these:
```java
public class VectorSearchParams {
  private final int _nprobe;         // default 8
  private final boolean _exactRerank; // default false
  private final int _maxCandidates;  // default topK * 10
}
```

These params flow from `QueryContext` through `FilterPlanNode` into the
filter operators.

### 4.6 VectorIndexReader Interface (No Change Needed)

The current interface:
```java
public interface VectorIndexReader extends IndexReader {
  ImmutableRoaringBitmap getDocIds(float[] vector, int topK);
}
```

For the IVF_FLAT reader, `nprobe` is set on the reader instance at construction
time or via a setter before query execution. This keeps the SPI interface stable.
The reader is constructed per-segment at load time, and the `nprobe` value is
passed through the `VectorIndexConfig` properties as a default, overridable at
query time.

**Query-time override approach:** The `VectorSimilarityFilterOperator` will
be modified to accept `VectorSearchParams` and call a new overloaded method
on IVF_FLAT readers. To avoid changing the SPI interface, we use a pattern:

```java
// In VectorSimilarityFilterOperator:
if (_vectorIndexReader instanceof NprobeAware) {
    ((NprobeAware) _vectorIndexReader).setNprobe(params.getNprobe());
}
bitmap = _vectorIndexReader.getDocIds(vector, topK);
```

Where `NprobeAware` is a marker interface in `pinot-segment-spi`:
```java
public interface NprobeAware {
  void setNprobe(int nprobe);
}
```

### 4.7 Exact-Scan Fallback

When `FilterPlanNode` encounters a `VECTOR_SIMILARITY` predicate but no vector
index exists on the column, instead of throwing an exception, it creates an
`ExactVectorScanFilterOperator` that:

1. Reads all float[] vectors from the forward index.
2. Computes exact distances.
3. Returns the top-K as a bitmap.

This is also used when the query option `vector.exactRerank = true` is set,
in which case ANN candidates are reranked by exact distance.

### 4.8 Exact Rerank on ANN Candidates

When `vector.exactRerank = true` and `vector.maxCandidates` is set:
1. The ANN index returns `maxCandidates` results (instead of `topK`).
2. The reranker reads the actual vectors for those candidates from the
   forward index.
3. Exact distances are computed and only the true top-K are returned.

### 4.9 Configuration Model (Backward Compatible)

**Existing config (continues to work):**
```json
{
  "fieldConfigList": [{
    "name": "embedding",
    "encodingType": "RAW",
    "indexTypes": ["VECTOR"],
    "properties": {
      "vectorIndexType": "HNSW",
      "vectorDimension": 128,
      "vectorDistanceFunction": "COSINE"
    }
  }]
}
```

**New IVF_FLAT config:**
```json
{
  "fieldConfigList": [{
    "name": "embedding",
    "encodingType": "RAW",
    "indexes": {
      "vector": {
        "vectorIndexType": "IVF_FLAT",
        "vectorDimension": 128,
        "vectorDistanceFunction": "COSINE",
        "version": 1,
        "properties": {
          "nlist": "256",
          "ivfTrainIterations": "15"
        }
      }
    }
  }]
}
```

Both the legacy (`indexTypes` + `properties`) and new (`indexes.vector`) config
paths are already supported by the existing `VectorIndexType.createDeserializerForLegacyConfigs()`.

### 4.10 Distance Functions

The existing `VectorDistanceFunction` enum in `VectorIndexConfig`:
```java
public enum VectorDistanceFunction {
  COSINE, INNER_PRODUCT, EUCLIDEAN, DOT_PRODUCT;
}
```

For IVF_FLAT, we implement pure-Java distance computations:
- **COSINE:** `1 - (dot(a,b) / (norm(a) * norm(b)))`
- **EUCLIDEAN (L2):** `sum((a[i] - b[i])^2)`
- **INNER_PRODUCT:** `-dot(a,b)` (negated so that "smaller = more similar")

These are encapsulated in a `VectorDistanceComputer` utility class.

### 4.11 File Extension Constants

New constant in `V1Constants.Indexes`:
```java
public static final String VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION = ".vector.ivfflat.index";
```

The `VectorIndexType.getFileExtensions()` method is extended to include this.
The `VectorIndexUtils.cleanupVectorIndex()` and `hasVectorIndex()` methods
are extended for the new file type.

---

## 5. PR Split Plan

### PR 1: Abstraction Layer and Config (Foundation)

**Branch:** `feature/vector-backend-abstraction`
**Modules:** `pinot-segment-spi`, `pinot-segment-local`
**Risk:** Low (additive, no behavior change)

Changes:
- Add `VectorBackendType` enum to `pinot-segment-spi` (in or near `VectorIndexConfig`).
- Add `VectorBackendType.fromString()` with null-safe HNSW default.
- Add `V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION`.
- Add `NprobeAware` marker interface to `pinot-segment-spi`.
- Update `VectorIndexType.validate()` to accept `HNSW` and `IVF_FLAT`.
- Update `VectorIndexType.getFileExtensions()` to include IVF_FLAT extension.
- Add unit tests for `VectorBackendType.fromString()`, config validation.

**Dependencies:** None.

---

### PR 2: Wrap Existing HNSW Behind Dispatch

**Branch:** `feature/vector-hnsw-dispatch`
**Modules:** `pinot-segment-local`
**Risk:** Medium (changes dispatch paths, must preserve behavior exactly)

Changes:
- Modify `VectorIndexType.createIndexCreator()` to switch on `VectorBackendType`.
  For `HNSW`, behavior is identical to current code.
  For `IVF_FLAT`, throw `UnsupportedOperationException` (not yet implemented).
- Modify `VectorIndexType.ReaderFactory.createIndexReader()` to switch on backend type.
  Read the backend type from `VectorIndexConfig` on the column's `FieldIndexConfigs`.
  For `HNSW`, behavior is identical.
- Modify `VectorIndexUtils.cleanupVectorIndex()` and `hasVectorIndex()` to handle
  IVF_FLAT file extension.
- Update `SegmentDirectoryPaths.findVectorIndexIndexFile()` to also look for
  IVF_FLAT files.
- Add tests verifying HNSW path is unchanged.

**Dependencies:** PR 1.

---

### PR 3: IVF_FLAT Creator/Builder

**Branch:** `feature/vector-ivfflat-creator`
**Modules:** `pinot-segment-local`
**Risk:** Low (new code, no existing behavior affected)

Changes:
- Add `VectorDistanceComputer` utility class (COSINE, L2, INNER_PRODUCT).
- Add `KMeansPlusPlusTrainer` for centroid computation.
- Add `IvfFlatVectorIndexCreator` implementing `VectorIndexCreator`.
  - Constructor: takes column name, index dir, config (nlist, dimension, distance func).
  - `add(float[])`: buffers vectors.
  - `seal()`: trains centroids, assigns vectors, writes on-disk format.
  - `close()`: finalizes file.
- Add `IvfFlatIndexFileFormat` (constants for header, read/write utilities).
- Wire `IVF_FLAT` case in `VectorIndexType.createIndexCreator()` to use new creator.
- Add unit tests: index creation, file format verification, small-dataset correctness.

**Dependencies:** PR 2.

---

### PR 4: IVF_FLAT Reader/Searcher

**Branch:** `feature/vector-ivfflat-reader`
**Modules:** `pinot-segment-local`
**Risk:** Low (new code, no existing behavior affected)

Changes:
- Add `IvfFlatVectorIndexReader` implementing `VectorIndexReader` and `NprobeAware`.
  - Constructor: memory-maps the IVF_FLAT index file, reads header and centroids.
  - `getDocIds(float[], int)`: centroid scan + inverted list scan + top-K selection.
  - `setNprobe(int)`: sets the nprobe parameter.
  - `close()`: releases memory-mapped buffer.
- Wire `IVF_FLAT` case in `VectorIndexType.ReaderFactory.createIndexReader()`.
- Add unit tests: search correctness, recall measurement, nprobe sensitivity.

**Dependencies:** PR 3.

---

### PR 5: Runtime Integration (Query Options, Fallback, Rerank)

**Branch:** `feature/vector-runtime-integration`
**Modules:** `pinot-spi`, `pinot-core`, `pinot-common`
**Risk:** Medium (touches query execution path)

Changes:
- Add query option keys to `CommonConstants.Request.QueryOptionKey`:
  `VECTOR_NPROBE`, `VECTOR_EXACT_RERANK`, `VECTOR_MAX_CANDIDATES`.
- Add `VectorSearchParams` class in `pinot-core`.
- Modify `FilterPlanNode` for `VECTOR_SIMILARITY` case:
  - Extract `VectorSearchParams` from `QueryContext.getQueryOptions()`.
  - If vector index exists: create `VectorSimilarityFilterOperator` with params.
  - If no vector index: create `ExactVectorScanFilterOperator`.
- Modify `VectorSimilarityFilterOperator`:
  - Accept `VectorSearchParams`.
  - Apply `nprobe` to `NprobeAware` readers before calling `getDocIds`.
  - If `exactRerank` is true, override topK with `maxCandidates`, then rerank.
- Add `ExactVectorScanFilterOperator` (reads forward index, computes distances).
- Add unit tests for query option parsing, fallback behavior, rerank logic.

**Dependencies:** PR 4.

---

### PR 6: Benchmarks

**Branch:** `feature/vector-benchmarks`
**Modules:** `pinot-perf` (or new benchmark module)
**Risk:** None (test-only)

Changes:
- JMH benchmarks comparing:
  - HNSW vs IVF_FLAT index creation time.
  - HNSW vs IVF_FLAT query latency at various dimensions and nprobe values.
  - Exact scan vs ANN recall and latency.
  - Memory consumption comparison.
- Benchmark datasets: synthetic random vectors (128d, 768d, 1536d).

**Dependencies:** PR 5.

---

### PR 7: Documentation

**Branch:** `feature/vector-docs`
**Modules:** docs
**Risk:** None

Changes:
- User-facing documentation for multi-backend vector indexing.
- Configuration reference for IVF_FLAT properties.
- Query option reference for `vector.nprobe`, `vector.exactRerank`, `vector.maxCandidates`.
- Migration guide (nothing to migrate -- existing configs keep working).

**Dependencies:** PR 6.

---

## 6. PR Dependency Graph

```
PR1 (Abstraction)
  |
  v
PR2 (HNSW Dispatch)
  |
  v
PR3 (IVF_FLAT Creator)
  |
  v
PR4 (IVF_FLAT Reader)
  |
  v
PR5 (Runtime Integration)
  |
  v
PR6 (Benchmarks)
  |
  v
PR7 (Documentation)
```

All PRs are strictly sequential. PR1 and PR2 can potentially be merged into a
single PR if the combined diff is small enough (likely under 500 lines total).

---

## 7. Testing Strategy

### Unit Tests (per-PR)

| PR | Test Class | What it covers |
|----|------------|---------------|
| PR1 | `VectorBackendTypeTest` | Enum parsing, null default, unknown type handling |
| PR1 | `VectorIndexConfigTest` (extended) | Config deserialization with IVF_FLAT type |
| PR2 | `VectorIndexTypeTest` (extended) | Creator/reader dispatch for HNSW path unchanged |
| PR3 | `IvfFlatVectorIndexCreatorTest` | Index creation, file format, small dataset |
| PR3 | `KMeansPlusPlusTrainerTest` | Centroid quality, convergence, edge cases |
| PR3 | `VectorDistanceComputerTest` | Distance function correctness |
| PR4 | `IvfFlatVectorIndexReaderTest` | Search correctness, recall, nprobe behavior |
| PR5 | `VectorSearchParamsTest` | Query option parsing |
| PR5 | `ExactVectorScanFilterOperatorTest` | Exact scan correctness |
| PR5 | `VectorSimilarityFilterOperatorTest` | Rerank logic |

### Integration Tests

- Extend existing vector index integration tests to cover IVF_FLAT.
- Test segment creation with IVF_FLAT config, segment loading, and query execution.
- Test backward compatibility: existing HNSW segments load and query correctly.
- Test exact-scan fallback when no vector index is configured.

### Recall Tests

- For IVF_FLAT: measure recall@K against exact brute-force at various nprobe values.
- Minimum recall target: 90% recall@10 with nprobe >= 8 on 10K vectors.

---

## 8. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| IVF_FLAT recall too low for small nprobe | Medium | Medium | Provide clear documentation on nprobe tuning. Default nprobe=8 targets 90%+ recall. |
| K-means training slow on large segments | Low | Medium | Training samples a configurable fraction. Single-pass is sufficient for phase 1. |
| Memory-mapped IVF file too large | Low | Low | Same pattern as Lucene HNSW (already memory-mapped). IVF_FLAT is actually more compact. |
| Backward compatibility break | Low | High | PR2 is specifically about preserving exact HNSW behavior. Extensive tests. |
| Query option plumbing breaks existing queries | Low | High | New options are purely additive. No existing option semantics change. |

---

## 9. Conflict Resolution Notes for Agent Outputs

When agent outputs disagree, apply these rules:

1. **SPI interface stability wins.** If one agent proposes changing
   `VectorIndexReader.getDocIds()` signature and another does not, prefer the
   approach that keeps the SPI interface unchanged. Use marker interfaces
   (`NprobeAware`) or constructor injection instead.

2. **Backward compatibility wins.** If two approaches exist and one requires
   config migration while the other does not, prefer the no-migration approach.

3. **Smaller diff wins.** If two approaches achieve the same result but one
   touches fewer files, prefer the smaller diff.

4. **pinot-segment-spi changes require extra scrutiny.** The SPI module is
   consumed by all other modules. Changes here should be strictly additive
   (new interfaces, new enum values, new constants).

5. **VectorIndexConfig.getVectorIndexType() returns String, not enum.** This is
   by design -- the enum parsing happens at the dispatch site, not in the config.
   This allows the config format to remain stable even as new backends are added.

6. **Mutable path is untouched in phase 1.** Any agent output that modifies
   `MutableVectorIndex` should be rejected unless it is purely defensive
   (e.g., adding a comment).

---

## 10. Assumptions Requiring Validation

The following assumptions must be validated by the archaeology agent before
implementation proceeds:

1. **Config parsing path:** `VectorIndexConfig` is deserialized from table config
   JSON via both the legacy path (`FieldConfig.indexTypes` + `properties`) and the
   new path (`FieldConfig.indexes.vector`). The legacy path goes through
   `VectorIndexType.createDeserializerForLegacyConfigs()`. Validate that both
   paths correctly populate `vectorIndexType`, `vectorDimension`, and `properties`.

2. **Lucene coupling scope:** Confirm that Lucene imports are confined to
   `pinot-segment-local` and do not leak into `pinot-segment-spi`, `pinot-core`,
   or `pinot-common`. Specifically verify that `VectorSimilarityFilterOperator`
   and `FilterPlanNode` have no Lucene imports.

3. **Segment build path:** Confirm the full segment creation pipeline:
   `SegmentColumnarIndexCreator` -> `StandardIndexes.vector().createIndexCreator()`
   -> `VectorIndexCreator.add()` / `seal()` / `close()`. Confirm no other entry
   points exist for vector index creation in the offline path.

4. **Segment load path:** Confirm that `VectorIndexHandler.updateIndices()` is the
   only path that creates vector indexes at segment load time (for adding indexes
   to existing segments). Confirm that `VectorIndexType.ReaderFactory` is the only
   path for opening a vector index reader during segment loading.

5. **DataSource.getVectorIndex() dispatch:** Confirm that `FilterPlanNode` is the
   only place that calls `dataSource.getVectorIndex()` and that the vector index
   reader returned there is the same instance created by `ReaderFactory`.

6. **QueryContext availability in FilterPlanNode:** Confirm that `_queryContext`
   in `FilterPlanNode` provides access to `queryOptions` map, which is where
   `vector.nprobe` and other vector query options will be read from.

7. **V1/V3 segment format handling:** Confirm that IVF_FLAT index files need to
   follow the same V1/V3 conversion pattern as HNSW Lucene files (written in V1
   format during creation, converted to V3 by `SegmentV1V2ToV3FormatConverter`).

8. **Forward index availability for exact scan:** Confirm that when a column has
   a vector index configured, the forward index for that column is always available
   (not disabled). This is needed for the exact-scan fallback and rerank paths.

---

## Appendix A: File Inventory

All files that will be created or modified, organized by PR.

### New Files
- `pinot-segment-spi/.../index/creator/VectorBackendType.java` (PR1)
- `pinot-segment-spi/.../index/reader/NprobeAware.java` (PR1)
- `pinot-segment-local/.../creator/impl/vector/IvfFlatVectorIndexCreator.java` (PR3)
- `pinot-segment-local/.../creator/impl/vector/KMeansPlusPlusTrainer.java` (PR3)
- `pinot-segment-local/.../creator/impl/vector/VectorDistanceComputer.java` (PR3)
- `pinot-segment-local/.../creator/impl/vector/IvfFlatIndexFileFormat.java` (PR3)
- `pinot-segment-local/.../readers/vector/IvfFlatVectorIndexReader.java` (PR4)
- `pinot-core/.../filter/ExactVectorScanFilterOperator.java` (PR5)
- `pinot-core/.../query/request/context/VectorSearchParams.java` (PR5)

### Modified Files
- `pinot-segment-spi/.../V1Constants.java` (PR1) -- add IVF_FLAT file extension
- `pinot-segment-local/.../vector/VectorIndexType.java` (PR1, PR2, PR3, PR4) -- dispatch logic
- `pinot-segment-local/.../store/VectorIndexUtils.java` (PR2) -- cleanup/detection for IVF_FLAT
- `pinot-segment-spi/.../store/SegmentDirectoryPaths.java` (PR2) -- find IVF_FLAT files
- `pinot-spi/.../utils/CommonConstants.java` (PR5) -- query option keys
- `pinot-core/.../plan/FilterPlanNode.java` (PR5) -- fallback + params
- `pinot-core/.../filter/VectorSimilarityFilterOperator.java` (PR5) -- nprobe + rerank
