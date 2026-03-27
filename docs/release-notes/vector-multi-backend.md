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

# Multi-Backend Vector Indexing (Phase 1)

## Summary

Pinot now supports multiple ANN (approximate nearest neighbor) backends for vector indexes. Phase 1 adds **IVF_FLAT** as a new backend alongside the existing **HNSW** backend, introduces query-time tuning options, and provides an automatic exact-scan fallback when no ANN index is present.

## New Capabilities

- **IVF_FLAT backend:** A pure-Java inverted file index with flat (uncompressed) vectors. Vectors are partitioned into configurable Voronoi cells (`nlist`) via k-means++ training. At query time, only the `nprobe` closest cells are searched. Supported on immutable (offline) segments only.

- **Query-time options:**
  - `SET vectorNprobe = N` -- controls how many IVF cells to probe (default: 4). Higher values improve recall at the cost of latency. Ignored for HNSW.
  - `SET vectorExactRerank = true` -- re-scores ANN candidates with exact distance from the forward index before returning top-K.
  - `SET vectorMaxCandidates = N` -- controls how many ANN candidates to retrieve before reranking (default: topK * 10).

- **Exact-scan fallback:** When a segment has no vector index (e.g., mutable segments with IVF_FLAT, or segments built before the index was added), Pinot automatically falls back to brute-force exact vector scan using the forward index.

- **Backend-neutral config model:** A `vectorIndexType` property in the field config selects the backend (`HNSW` or `IVF_FLAT`). Backend-specific properties are validated and cross-backend property mixing is rejected.

- **Distance functions:** Both backends support `COSINE`, `EUCLIDEAN` (alias: `L2`), `INNER_PRODUCT` (alias: `DOT_PRODUCT`).

## Configuration Changes

- New property `vectorIndexType` in field config `properties`. Values: `"HNSW"` (default), `"IVF_FLAT"`.
- New IVF_FLAT-specific properties: `nlist`, `trainSampleSize`, `trainingSeed`, `minRowsForIndex`.
- New query option keys: `vectorNprobe`, `vectorExactRerank`, `vectorMaxCandidates`.

## Backward Compatibility

- **Existing HNSW configurations are unchanged.** Configs without `vectorIndexType` (or with `"vectorIndexType": "HNSW"`) continue to work identically.
- **SQL syntax is unchanged.** The `VECTOR_SIMILARITY` function works the same way.
- **New query options are additive.** Queries that do not use the new options behave exactly as before.

## Known Limitations

- IVF_FLAT is supported on immutable (offline) segments only. Mutable (realtime consuming) segments fall back to exact scan.
- No product quantization (IVF_PQ) or scalar quantization.
- Exact rerank uses L2 squared distance regardless of the index's configured distance function.
- IVF_FLAT index is fully loaded into Java heap at segment load time.
