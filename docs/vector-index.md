<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Vector Index

Apache Pinot supports approximate nearest neighbor (ANN) search on float array columns through its vector index subsystem. You use the `VECTOR_SIMILARITY` SQL function to find the top-K most similar vectors to a query vector. Under the hood, Pinot builds an ANN index on the column to accelerate these queries.

This page covers the available ANN backends, how to configure them, query-time tuning options, and operational guidance.

---

## Table of Contents

1. [Supported ANN Backends](#supported-ann-backends)
2. [Column Requirements](#column-requirements)
3. [Configuration](#configuration)
   - [HNSW Configuration](#hnsw-configuration)
   - [IVF_FLAT Configuration](#ivf_flat-configuration)
   - [Distance Functions](#distance-functions)
4. [Querying](#querying)
   - [Basic Query Syntax](#basic-query-syntax)
   - [Query-Time Options](#query-time-options)
5. [Exact Rerank](#exact-rerank)
6. [Exact Scan Fallback](#exact-scan-fallback)
7. [Backend Selection Guide](#backend-selection-guide)
8. [Migration and Backward Compatibility](#migration-and-backward-compatibility)
9. [Phase 1 Limitations](#phase-1-limitations)

---

## Supported ANN Backends

Pinot supports two ANN index backends:

| Backend | Algorithm | Segment Support | Library |
|---------|-----------|-----------------|---------|
| **HNSW** | Hierarchical Navigable Small World graph | Mutable (realtime) and immutable (offline) | Apache Lucene |
| **IVF_FLAT** | Inverted File with flat (uncompressed) vectors | Immutable (offline) segments only | Pure Java |

**HNSW** is the default backend and has been available in Pinot since vector indexes were first introduced. It provides high recall (typically 95%+) at the cost of higher memory usage and build time.

**IVF_FLAT** is new in this release. It partitions vectors into Voronoi cells using k-means clustering and searches only a subset of cells at query time. It offers faster build times and lower memory usage than HNSW, with tunable recall via the `nprobe` parameter. IVF_FLAT is currently supported on immutable (offline) segments only.

---

## Column Requirements

Vector indexes can only be created on columns that meet both of these criteria:

- **Data type:** `FLOAT`
- **Cardinality:** Multi-value (the column stores an array of floats representing the vector)

The column must use `RAW` encoding (no dictionary encoding).

---

## Configuration

You add a vector index through the `fieldConfigList` in your table config. The `vectorIndexType` property selects the backend. If you omit `vectorIndexType`, it defaults to `HNSW`.

### HNSW Configuration

HNSW is the default backend. If you are already using vector indexes in Pinot, your existing configuration continues to work unchanged.

**Minimal example:**

```json
{
  "fieldConfigList": [
    {
      "name": "embedding",
      "encodingType": "RAW",
      "indexTypes": ["VECTOR"],
      "properties": {
        "vectorIndexType": "HNSW",
        "vectorDimension": "128",
        "vectorDistanceFunction": "COSINE"
      }
    }
  ]
}
```

**Example with all tuning properties:**

```json
{
  "fieldConfigList": [
    {
      "name": "embedding",
      "encodingType": "RAW",
      "indexTypes": ["VECTOR"],
      "properties": {
        "vectorIndexType": "HNSW",
        "vectorDimension": "128",
        "vectorDistanceFunction": "COSINE",
        "maxCon": "16",
        "beamWidth": "100",
        "maxDimensions": "2048",
        "maxBufferSizeMB": "16.0",
        "useCompoundFile": "true",
        "mode": "BEST_SPEED"
      }
    }
  ]
}
```

#### HNSW Property Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `vectorIndexType` | String | `"HNSW"` | Backend type. Can be omitted for HNSW (it is the default). |
| `vectorDimension` | String (int) | *required* | Number of dimensions in each vector. Must be a positive integer. |
| `vectorDistanceFunction` | String | `"COSINE"` | Distance function. See [Distance Functions](#distance-functions). |
| `maxCon` | String (int) | `"16"` | Maximum number of connections per node in the HNSW graph. Higher values improve recall at the cost of memory and build time. |
| `beamWidth` | String (int) | `"100"` | Beam width during graph construction. Higher values improve recall at the cost of build time. |
| `maxDimensions` | String (int) | `"2048"` | Maximum allowed vector dimensions. Vectors exceeding this are rejected. |
| `maxBufferSizeMB` | String (double) | Lucene default (~16.0) | Lucene IndexWriter RAM buffer size in MB. |
| `useCompoundFile` | String (boolean) | Lucene default (`"true"`) | Whether to use Lucene compound file format. |
| `mode` | String | `"BEST_SPEED"` | Lucene 9.12 codec mode. Either `BEST_SPEED` or `BEST_COMPRESSION`. |

> **Note:** HNSW supports both mutable (realtime) and immutable (offline) segments.

---

### IVF_FLAT Configuration

IVF_FLAT partitions vectors into `nlist` clusters (Voronoi cells) using k-means++ during index build. At query time, only the `nprobe` closest clusters are searched.

> **Warning:** IVF_FLAT is supported on **immutable (offline) segments only** in this release. For realtime tables with IVF_FLAT configured, consuming segments (mutable segments) will not have a vector index. The index is built when the segment is sealed and converted to an immutable segment. During the mutable phase, vector similarity queries on those segments use the [exact scan fallback](#exact-scan-fallback).

**Minimal example:**

```json
{
  "fieldConfigList": [
    {
      "name": "embedding",
      "encodingType": "RAW",
      "indexTypes": ["VECTOR"],
      "properties": {
        "vectorIndexType": "IVF_FLAT",
        "vectorDimension": "128",
        "vectorDistanceFunction": "EUCLIDEAN",
        "nlist": "64"
      }
    }
  ]
}
```

**Example with all tuning properties:**

```json
{
  "fieldConfigList": [
    {
      "name": "embedding",
      "encodingType": "RAW",
      "indexTypes": ["VECTOR"],
      "properties": {
        "vectorIndexType": "IVF_FLAT",
        "vectorDimension": "128",
        "vectorDistanceFunction": "EUCLIDEAN",
        "nlist": "128",
        "trainSampleSize": "50000",
        "trainingSeed": "42",
        "minRowsForIndex": "1000"
      }
    }
  ]
}
```

#### IVF_FLAT Property Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `vectorIndexType` | String | -- | Must be `"IVF_FLAT"` for this backend. |
| `vectorDimension` | String (int) | *required* | Number of dimensions in each vector. Must be a positive integer. |
| `vectorDistanceFunction` | String | `"COSINE"` | Distance function. See [Distance Functions](#distance-functions). |
| `nlist` | String (int) | `"128"` | Number of Voronoi cells (centroids) to create during training. Higher values create finer partitions, improving recall at the cost of build time. |
| `trainSampleSize` | String (int) | `max(nlist * 40, 10000)` | Number of vectors sampled for k-means training. Must be >= `nlist`. If the segment has fewer vectors than this value, all vectors are used. |
| `trainingSeed` | String (long) | `System.nanoTime()` | Random seed for reproducible k-means training. Set this to a fixed value for deterministic index builds. |
| `minRowsForIndex` | String (int) | *(none)* | Minimum number of rows required to build the index. If the segment has fewer rows, no index is built. |

> **Note:** If `nlist` exceeds the number of vectors in the segment, it is automatically clamped to the number of vectors.

#### IVF_FLAT Validation Rules

- `nlist` must be a positive integer.
- `trainSampleSize` must be a positive integer and must be >= `nlist`.
- HNSW-specific properties (`maxCon`, `beamWidth`, `maxDimensions`, etc.) cannot be used with IVF_FLAT. The validator rejects cross-backend properties with a clear error message.

---

### Distance Functions

Both backends support the same set of distance functions. All distance functions return a value where **lower means more similar**.

| Distance Function | Description | Value Range |
|-------------------|-------------|-------------|
| `COSINE` | Cosine distance: `1 - cosine_similarity(a, b)`. | [0, 2] |
| `EUCLIDEAN` | Squared Euclidean (L2) distance: `sum((a[i] - b[i])^2)`. No square root is applied because it does not affect ranking. | [0, +inf) |
| `L2` | Alias for `EUCLIDEAN`. | [0, +inf) |
| `INNER_PRODUCT` | Negative dot product: `-dot(a, b)`. Higher similarity (larger dot product) maps to lower distance. | (-inf, +inf) |
| `DOT_PRODUCT` | Same as `INNER_PRODUCT`. | (-inf, +inf) |

> **Note:** For `COSINE` distance with IVF_FLAT, k-means training uses L2 distance on normalized vectors internally. This is mathematically equivalent to clustering by angular similarity.

---

## Querying

### Basic Query Syntax

The `VECTOR_SIMILARITY` function is the same regardless of which backend is configured:

```sql
SELECT productId, description
FROM products
WHERE VECTOR_SIMILARITY(embedding, ARRAY[0.1, 0.2, 0.3, ...], 10)
ORDER BY $similarity
LIMIT 10
```

The third argument (`10` in this example) is the top-K value -- the number of nearest neighbors to return.

The query syntax has not changed with the addition of IVF_FLAT. Existing queries work unchanged.

### Query-Time Options

You can tune vector search behavior per query using `SET` options. These options are additive -- queries without these options behave exactly as before.

```sql
SET vectorNprobe = 16;
SET vectorExactRerank = true;
SET vectorMaxCandidates = 200;
SELECT productId, description
FROM products
WHERE VECTOR_SIMILARITY(embedding, ARRAY[0.1, 0.2, 0.3, ...], 10)
```

#### Query Option Reference

| Option | Type | Default | Applies To | Description |
|--------|------|---------|------------|-------------|
| `vectorNprobe` | int | `4` | IVF_FLAT only | Number of Voronoi cells to probe during search. Higher values improve recall at the cost of latency. Ignored for HNSW. Clamped to [1, nlist]. |
| `vectorExactRerank` | boolean | `false` | All backends | When `true`, ANN candidates are re-scored using exact distance computed from the forward index, then re-sorted to return the true top-K. |
| `vectorMaxCandidates` | int | `topK * 10` | All backends (with rerank) | Maximum number of ANN candidates to retrieve before applying exact rerank. Only meaningful when `vectorExactRerank` is `true`. Must be >= topK. |

> **Note:** The `vectorNprobe` option only affects segments with an IVF_FLAT index. For segments with an HNSW index (or no index), the option is ignored.

---

## Exact Rerank

Exact rerank improves the accuracy of ANN results by re-scoring candidates with exact distance computation. The process is:

1. The ANN index returns `vectorMaxCandidates` candidates (default: `topK * 10`).
2. For each candidate, the actual vector is read from the forward index.
3. Exact L2 squared distance is computed between each candidate and the query vector.
4. The candidates are re-sorted by exact distance and the top-K are returned.

**When to use rerank:**

- When you need higher precision than the ANN index alone provides.
- When the latency cost of reading `vectorMaxCandidates` vectors from the forward index is acceptable.
- Particularly useful with IVF_FLAT at low `nprobe` values, where ANN recall may be lower.

**Example:**

```sql
SET vectorExactRerank = true;
SET vectorMaxCandidates = 100;
SELECT productId
FROM products
WHERE VECTOR_SIMILARITY(embedding, ARRAY[0.1, 0.2, 0.3, ...], 10)
```

This retrieves 100 ANN candidates, re-scores them with exact distance, and returns the 10 closest.

> **Note:** Exact rerank always uses L2 squared distance for re-scoring, regardless of the configured distance function on the index.

---

## Exact Scan Fallback

When a segment does not have a vector index on the queried column, Pinot automatically falls back to an exact brute-force scan. This can happen when:

- The vector index was not configured when the segment was built.
- The segment is a mutable (realtime) segment and the backend is IVF_FLAT (which only supports immutable segments).
- The column's forward index is available but no vector index file exists.

The exact scan reads every vector from the forward index, computes L2 squared distance to the query vector, and returns the top-K results.

> **Warning:** Exact scan is expensive. It scans all documents in the segment. A warning is logged when this fallback is triggered: `"Performing exact vector scan fallback on column: ... This is expensive -- consider adding a vector index."` If you see this warning frequently, ensure the vector index is configured and built on the relevant segments.

---

## Backend Selection Guide

Use this decision framework to choose the right approach:

### Use HNSW when:

- You need the highest recall (typically 95%+).
- Your table includes realtime ingestion and you need vector search on mutable segments.
- Your dataset is small to moderate (up to millions of vectors per segment).
- Accuracy is more important than build time or memory.

### Use IVF_FLAT when:

- Your workload is offline/batch only, or you can tolerate exact-scan fallback on mutable segments.
- You want faster index build times compared to HNSW.
- You want lower memory usage during index build (vectors are buffered in Java heap, no Lucene overhead).
- You are willing to tune `nprobe` to balance recall and latency.
- Your segments are large and you want a simpler, pure-Java index without Lucene dependency.

### Use no index (exact scan) when:

- Your segments are very small (fewer than ~1000 vectors).
- Vector similarity queries are infrequent or one-time.
- You are experimenting and do not want to pay the index build cost.

### Tuning IVF_FLAT Recall

IVF_FLAT recall depends on the `nprobe` / `nlist` ratio:

| nprobe / nlist Ratio | Approximate Recall | Notes |
|----------------------|--------------------|-------|
| 1.0 (nprobe = nlist) | ~100% | Equivalent to brute-force scan of all clusters. Full accuracy but no speedup. |
| 0.25 | ~85-95% | Good balance for most workloads. |
| 0.10 | ~70-85% | Fast, lower recall. Consider using exact rerank. |
| 0.01-0.05 | < 70% | Very fast, but may miss relevant results. |

These recall numbers are approximate and depend on your data distribution. Test with your actual data.

---

## Migration and Backward Compatibility

**Existing HNSW users: your configuration and queries are unchanged.**

- Table configs without `vectorIndexType` (or with `"vectorIndexType": "HNSW"`) continue to work exactly as before.
- The HNSW backend implementation, Lucene index format, and query behavior are not modified.
- SQL `VECTOR_SIMILARITY` syntax is unchanged.
- New query options (`vectorNprobe`, `vectorExactRerank`, `vectorMaxCandidates`) are purely additive. Queries that do not use these options behave identically to previous versions.

**Optional clarification step:** If you want to make your intent explicit, you can add `"vectorIndexType": "HNSW"` to your existing field config properties. This has no effect on behavior but makes the backend choice visible.

```json
{
  "properties": {
    "vectorIndexType": "HNSW",
    "vectorDimension": "128",
    "vectorDistanceFunction": "COSINE"
  }
}
```

---

## Phase 1 Limitations

The following limitations apply to this release:

1. **IVF_FLAT: immutable segments only.** IVF_FLAT indexes are not built on mutable (realtime consuming) segments. Segments must be sealed/converted to immutable before the IVF_FLAT index is available. On mutable segments, vector similarity queries fall back to exact brute-force scan.

2. **No product quantization.** IVF_PQ, scalar quantization, and other compression techniques are not supported. Vectors are stored uncompressed (flat) in the IVF index.

3. **No JNI/native library integration.** The IVF_FLAT implementation is pure Java. There is no Faiss or HNSWLIB native integration.

4. **No radius/range search.** Only top-K nearest neighbor search is supported. Range queries (find all vectors within distance D) are not available.

5. **No filtered-ANN optimization.** Pre-filtering or post-filtering with ANN indexes is not optimized beyond the existing behavior.

6. **Exact rerank uses L2 squared distance.** The rerank step always uses L2 squared distance regardless of the index's configured distance function.

7. **IVF_FLAT index is fully loaded into Java heap.** The entire index (centroids, inverted lists, vectors) is loaded into memory at segment load time. For very large segments, this increases heap usage.
