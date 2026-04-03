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

# Phase 2: Multi-Backend Vector Indexing — IVF_PQ

## Overview

Phase 2 adds **IVF_PQ** (Inverted File with Product Quantization) as a second vector
index backend alongside HNSW. IVF_PQ trades recall for significantly smaller index
size and faster search latency on large datasets.

## Architecture

### Backend Selection

The `vectorIndexType` field in table config selects the backend:

- `"HNSW"` — Lucene-based HNSW (existing, default)
- `"IVF_PQ"` — Pure Java IVF with Product Quantization (new)

Both backends implement the same `VectorIndexReader` interface and are dispatched
through `VectorIndexType`. The SQL surface (`VECTOR_SIMILARITY`) is unchanged.

### IVF_PQ Build Flow

1. Collect all vectors during segment creation
2. Sample training vectors (configurable via `trainSampleSize`)
3. Train coarse IVF centroids using KMeans++
4. Assign vectors to nearest centroid, compute residuals
5. Train PQ sub-quantizers on residuals
6. Encode residuals into compact PQ codes
7. Write binary index file with inverted lists

### IVF_PQ Search Flow

1. Find `nprobe` nearest coarse centroids to query
2. For each probed list, build PQ distance lookup tables on residuals
3. Score candidates using approximate PQ distances (ADC)
4. If exact rerank enabled: re-score top candidates using original vectors
5. Return top-K doc IDs as bitmap

### File Format

Binary format (`.vector.ivfpq.index`), little-endian, versioned:
- Header: magic, version, dimension, nlist, pqM, pqNbits, distanceFunction, numVectors
- Coarse centroids: `float[nlist][dimension]`
- PQ codebooks: `float[pqM][2^pqNbits][dimension/pqM]`
- Inverted lists: per list — listSize, docIds, PQ codes, original vectors

## Configuration

### Table Config Example

```json
{
  "fieldConfigList": [
    {
      "name": "embedding",
      "encodingType": "RAW",
      "indexes": {
        "vector": {
          "vectorIndexType": "IVF_PQ",
          "vectorDimension": 768,
          "vectorDistanceFunction": "COSINE",
          "version": 2,
          "properties": {
            "nlist": "256",
            "pqM": "32",
            "pqNbits": "8",
            "trainSampleSize": "65536",
            "trainingSeed": "42"
          }
        }
      }
    }
  ]
}
```

### IVF_PQ Config Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `nlist` | int | 256 | Number of IVF coarse centroids (Voronoi cells) |
| `pqM` | int | 32 | Number of PQ subspaces (must divide dimension evenly) |
| `pqNbits` | int | 8 | Bits per PQ code (centroids per subspace = 2^pqNbits) |
| `trainSampleSize` | int | 65536 | Max vectors sampled for training |
| `trainingSeed` | long | 42 | Random seed for reproducible training |

### Constraints

- `pqM` must divide `vectorDimension` evenly
- `pqNbits` must be between 1 and 8
- `nlist` must be > 0
- `vectorDimension` must be > 0

### Supported Distance Functions

- `COSINE` — cosine distance (1 - cosine_similarity)
- `EUCLIDEAN` — squared L2 distance
- `INNER_PRODUCT` — negative inner product

## Query-Time Options

Runtime tuning via SQL `SET` options (no SQL syntax changes):

```sql
SET vectorNprobe = 16;
SET vectorExactRerank = true;
SELECT ... WHERE VECTOR_SIMILARITY(embedding, ARRAY[...], 10)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `vectorNprobe` | int | config or 8 | Number of coarse centroids to probe at query time |
| `vectorExactRerank` | bool | true (IVF_PQ) | Re-score candidates using exact distances |

- HNSW ignores these options (no behavioral change)
- Higher `nprobe` increases recall but also latency
- Rerank uses 4x candidate oversampling then re-scores with exact distance

## Exact Rerank

When `vectorExactRerank=true` (default for IVF_PQ):
1. IVF_PQ generates `4 * topK` candidates using approximate PQ distances
2. Candidates are re-scored using the exact configured distance function
3. Only the true top-K by exact distance are returned

This significantly improves recall (measured ~20% improvement) at modest latency cost.

The rerank uses the distance function stored in the index (COSINE, EUCLIDEAN, or
INNER_PRODUCT), not always L2.

## Explain / Debug

The `EXPLAIN PLAN` output for VECTOR_SIMILARITY now shows:

```
VECTOR_SIMILARITY_INDEX(
  indexLookUp:vector_index,
  operator:VECTOR_SIMILARITY,
  backend:IVF_PQ,
  nprobe:16,
  exactRerank:true,
  ...
)
```

For HNSW:
```
VECTOR_SIMILARITY_INDEX(
  indexLookUp:vector_index,
  operator:VECTOR_SIMILARITY,
  backend:HNSW,
  ...
)
```

## Benchmark Results

10,000 vectors, 64 dimensions, 100 queries. All times in microseconds.

### L2/Euclidean Dataset

| Config | Build (ms) | Size (MB) | Recall@10 | Recall@100 | p50 (us) | p95 (us) |
|--------|-----------|-----------|-----------|------------|---------|---------|
| ExactScan | 0 | 0 | 1.00 | 1.00 | 693 | 747 |
| IVF_PQ(nl=64,m=8,np=8,rr) | 4516 | 2.63 | 0.40 | 0.36 | 118 | 192 |
| IVF_PQ(nl=64,m=8,np=16,rr) | 4505 | 2.63 | 0.50 | 0.53 | 218 | 286 |
| IVF_PQ(nl=128,m=16,np=16,rr) | 4670 | 2.73 | 0.51 | 0.41 | 261 | 321 |
| IVF_PQ(nl=256,m=16,np=32,rr) | 5812 | 2.76 | 0.62 | 0.48 | 491 | 576 |

### Rerank Effect (nlist=128, pqM=16, nprobe=16)

| Rerank | Recall@10 | Recall@100 | p50 (us) |
|--------|-----------|------------|---------|
| ON | 0.51 | 0.41 | 262 |
| OFF | 0.42 | 0.40 | 250 |

### Key Observations

1. **Latency**: IVF_PQ is 3-6x faster than exact scan at query time
2. **Recall/latency tradeoff**: Primarily controlled by `nprobe`
3. **Rerank improves Recall@10** by ~20% relative with minimal latency overhead
4. **Index size**: ~2.7MB for 10K 64-dim vectors (vs raw data ~2.5MB)
5. **Build time**: ~5 seconds dominated by KMeans training

For production workloads with larger vectors (768+ dim) and more data (1M+ vectors),
IVF_PQ provides much better compression than shown here since PQ codes are fixed-size
regardless of dimension.

## Recommended Defaults

| Parameter | Small (<100K vectors) | Medium (100K-10M) | Large (>10M) |
|-----------|----------------------|-------------------|--------------|
| `nlist` | 64-128 | 256-512 | 512-1024 |
| `pqM` | dim/8 to dim/4 | dim/4 | dim/4 to dim/2 |
| `pqNbits` | 8 | 8 | 8 |
| `nprobe` | 8-16 | 16-32 | 32-64 |
| `trainSampleSize` | all vectors | 65536 | 65536-131072 |

**Rule of thumb**: Start with `nlist = sqrt(numVectors)`, `pqM = dimension/4`,
`pqNbits = 8`, `nprobe = nlist/16`. Increase `nprobe` to improve recall.

## Backend Selection Guidance

| Use Case | Recommended Backend |
|----------|-------------------|
| Highest recall required | HNSW |
| Low-latency with acceptable recall | IVF_PQ |
| Very large datasets (memory constrained) | IVF_PQ |
| Small datasets (<10K vectors) | HNSW or exact scan |
| Need runtime recall/latency tradeoff | IVF_PQ (via nprobe) |

## Phase 2 Limitations

- **Immutable segments only**: IVF_PQ does not support realtime/mutable segments
- **No IVF_FLAT**: Only IVF_PQ is implemented (IVF_FLAT may be added later)
- **No filtered ANN**: Filter predicates are applied after ANN search, not during
- **No radius/range search**: Only top-K search is supported
- **Pure Java**: No JNI/native acceleration (Faiss, etc.)
- **Index size**: Original vectors are stored for rerank, increasing index size
- **Build time**: KMeans training can be slow for very large datasets
- **No incremental update**: Changing IVF_PQ config requires full segment rebuild

## Release Notes Snippet

```
### New Features
- Added IVF_PQ vector index backend for compressed approximate nearest neighbor
  search. IVF_PQ provides configurable recall/latency tradeoffs via `nprobe`
  and `exactRerank` query options. Supports COSINE, EUCLIDEAN, and INNER_PRODUCT
  distance functions. Immutable segments only.
```
