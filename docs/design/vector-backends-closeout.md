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

# Vector Backends Close-Out

## Goal

Finish the remaining concrete gaps after Phase 4 (`#18119`) without reopening roadmap-wide
design questions. This phase stays narrow: only the missing runtime behavior, parity gaps,
benchmarks, and documentation needed to make the shipped vector stack internally consistent.

## Merge Contract

The close-out phase should remain split into small, mergeable PRs with additive interfaces only:

1. PR1: archaeology and this design note
2. PR2: generic quantizer framework in the real build and search path
3. PR3: HNSW runtime controls that affect real search behavior
4. PR4: `IVF_ON_DISK` filter-aware ANN parity
5. PR5: approximate radius capability parity
6. PR6: mutable versus immutable convergence layer
7. PR7: benchmarks, docs, and release notes

Cross-PR rule: if a change requires a new SPI or query option field, land the SPI as an additive
no-op seam before switching behavior.

## Compatibility Policy

- Existing SQL remains unchanged, including `VECTOR_SIMILARITY(...)` and
  `VECTOR_SIMILARITY_RADIUS(...)`.
- Existing query options remain valid. Previously accepted but metadata-only options become real;
  they must not start rejecting queries that Phase 4 accepted.
- Existing vector index configs remain valid, including legacy HNSW configs with omitted
  `vectorIndexType`.
- New explain and debug fields are additive. Existing fields keep their meaning.
- Capability flags must describe actual runtime behavior, not roadmap intent.
- When Pinot falls back to exact scan or post-filter behavior, explain and debug output must say so
  explicitly.

## Shared Interface Decisions

### Query-time search params

`pinot-core/src/main/java/org/apache/pinot/core/operator/filter/VectorSearchParams.java` is the
single runtime search-params object. It should remain the only place where query options are turned
into per-query vector runtime controls.

Additive seam:

- Keep `vectorNprobe`, `vectorExactRerank`, `vectorMaxCandidates`, and `vectorEfSearch`.
- Add Pinot-owned HNSW options here only if they can be wired to real runtime behavior in the same
  PR.
- Do not add backend-specific ad hoc parsing inside filter operators.

### Reader capability interfaces

`pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/reader/VectorIndexReader.java`
is currently too narrow for the remaining gaps. The close-out phase can add small additive SPI
interfaces if they are immediately used:

- `FilterAwareVectorIndexReader` already exists and should remain the only gate for
  `FILTER_THEN_ANN`.
- `NprobeAware` remains the runtime hook for IVF-family search breadth.
- `EfSearchAware` should move from metadata-only state to real HNSW visit-budget control.
- If approximate radius becomes real, add a dedicated additive SPI rather than overloading
  `getDocIds(...)`.

### Capability flags

`pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/creator/VectorBackendType.java`
and `VectorBackendCapabilities.java` remain the source of truth for backend capability reporting.

Policy:

- `supportsFilterAwareSearch` stays true only if the reader can honor pre-filter ANN directly.
- `supportsApproximateRadius` flips to true only where the backend exposes a real index-assisted
  approximate threshold candidate path.
- `supportsRuntimeSearchParams` means actual runtime effect, not explain-only bookkeeping.

## File and Method Map

### Quantizer integration

Current gap:

- `VectorIndexConfigValidator.validateQuantizerProperty(...)` still rejects all non-`FLAT`
  quantizers.
- `FlatQuantizer` and `ScalarQuantizer` exist, but `IvfFlatVectorIndexCreator`,
  `IvfFlatVectorIndexReader`, and `IvfOnDiskVectorIndexReader` still persist and search raw
  float vectors only.
- `IVF_PQ` already has backend-native PQ encoding and should preserve its legacy config surface.

Primary insertion points:

- `pinot-segment-spi/.../VectorIndexConfigValidator.java`
  - `validateQuantizerProperty(...)`
  - backend-specific property allowlists
- `pinot-segment-local/.../VectorIndexType.java`
  - `createIndexCreator(...)`
  - `createIndexReader(...)`
- `pinot-segment-local/.../IvfFlatVectorIndexCreator.java`
  - constructor config parsing
  - `seal()`
  - `writeIndex(...)`
- `pinot-segment-local/.../IvfFlatVectorIndexReader.java`
  - constructor file parsing
  - `getDocIds(...)`
  - `getDocIds(..., preFilterBitmap)`
- `pinot-segment-local/.../IvfOnDiskVectorIndexReader.java`
  - header parsing
  - `scanInvertedList(...)`
- `pinot-segment-local/.../VectorQuantizationUtils.java`
  - helper factory methods and distance helpers

Expected shape:

- Make `quantizer=FLAT|SQ8|SQ4` real first for the IVF flat-family storage format.
- Preserve `IVF_PQ` as a backend-native path; do not force it through the generic scalar
  quantizer abstraction.
- Preserve legacy configs for `HNSW`, `IVF_FLAT`, and `IVF_PQ`.
- Land file-format versioning or header tagging only if backward reading of old segments stays
  intact.

### HNSW runtime controls

Current gap:

- `VectorSearchParams` parses `vectorEfSearch`, but
  `VectorSimilarityFilterOperator.configureBackendParams(...)` logs that it is explain-only.
- `HnswVectorIndexReader` stores `_efSearchOverride`, but `getDocIds(...)` still uses bare
  `KnnFloatVectorQuery`.

Primary insertion points:

- `pinot-common/.../QueryOptionsUtils.java`
  - vector runtime option parsing
- `pinot-core/.../VectorSearchParams.java`
  - additive runtime fields
- `pinot-core/.../VectorSimilarityFilterOperator.java`
  - `configureBackendParams(...)`
  - `refreshExplainContext(...)`
- `pinot-segment-local/.../HnswVectorIndexReader.java`
  - `getDocIds(...)`
  - `getDocIds(..., preFilterBitmap)`
  - `getIndexDebugInfo()`

Expected shape:

- Make `vectorEfSearch` control the Lucene visit budget for HNSW search rather than only
  recording the requested value.
- Any Pinot-owned runtime knobs for relative-distance checking or bounded-queue behavior must
  change the actual Lucene collector or HNSW traversal path. If Lucene does not expose a stable
  hook, do not ship a Pinot option that only changes explain text.
- Explain/debug must report both requested and effective HNSW controls.

### IVF_ON_DISK filter-aware parity

Current gap:

- `VectorBackendType` advertises `supportsFilterAwareSearch(true)` for `IVF_ON_DISK`.
- `IvfOnDiskVectorIndexReader` does not implement `FilterAwareVectorIndexReader`.
- `FilterPlanNode.tryAttachVectorPreFilters(...)` only activates `FILTER_THEN_ANN` when the
  operator reports pre-filter support.

Primary insertion points:

- `pinot-segment-local/.../IvfOnDiskVectorIndexReader.java`
  - implement `FilterAwareVectorIndexReader`
  - `getDocIds(..., preFilterBitmap)`
  - `getIndexDebugInfo()`
- `pinot-core/.../VectorSimilarityFilterOperator.java`
  - search-mode explain/debug reporting
- `pinot-core/.../FilterPlanNode.java`
  - existing pre-filter attach logic should work once reader capability is real

Expected shape:

- Reuse the same `nprobe`/centroid selection behavior as unfiltered IVF_ON_DISK.
- Apply the bitmap during inverted-list scanning, not after ANN result collection.
- Preserve exact fallback behavior and do not add hidden caches or sticky query-scoped state.

### Approximate radius capability parity

Current gap:

- `VectorRadiusFilterOperator` always runs candidate-then-exact-threshold or full exact scan.
- All backends report `supportsApproximateRadius(false)`, even when some can cheaply produce
  index-assisted approximate candidates.

Primary insertion points:

- `pinot-core/.../VectorRadiusFilterOperator.java`
  - `executeIndexAssistedSearch(...)`
  - explain/debug attributes
- `pinot-segment-spi/.../VectorBackendType.java`
  - capability flags
- Backend readers that can support a native or backend-aware threshold candidate path

Expected shape:

- Keep `VECTOR_SIMILARITY_RADIUS` backward-compatible and exact by default at the result level.
- Approximate radius support means the backend can supply a threshold-aware candidate set before
  exact forward-index refinement. It does not mean Pinot may silently return incomplete results.
- If only one backend family is feasible in this phase, promote the capability only there and keep
  the others false.

### Mutable versus immutable parity

Current gap:

- `VectorIndexType.createMutableIndex(...)` only returns `MutableVectorIndex` for `HNSW`.
- `MutableVectorIndex` still uses Lucene `KnnFloatVectorQuery` directly and does not share the
  immutable runtime-control hooks or pre-filter semantics.
- IVF-family configs on mutable segments degrade to exact scan with only a warning log.

Primary insertion points:

- `pinot-segment-local/.../VectorIndexType.java`
  - mutable index selection and warnings
- `pinot-segment-local/.../MutableVectorIndex.java`
  - search path
  - debug/config reporting
- `pinot-core/.../VectorSearchStrategy.java`
  - mutable-segment routing
- integration tests in `pinot-integration-tests/.../VectorTest.java` and
  `IvfPqVectorRealtimeTest.java`

Expected shape:

- The mergeable path is a convergence layer, not full mutable IVF-family indexing.
- Best safe outcome for this phase:
  - HNSW mutable path shares the same runtime-control semantics as immutable HNSW.
  - IVF-family configs on consuming segments get explicit explain/debug fallback reasons instead of
    silent behavior drift.
  - Mixed consuming and immutable queries become operationally understandable.
- Full mutable IVF-family ANN remains deferred unless it drops out as a very small patch.

## Ranked Risks

1. HNSW runtime controls can easily regress correctness if Pinot changes Lucene traversal without
   understanding visit limits and filtered fallback semantics.
2. Generic quantizer integration can create unreadable segment artifacts if file-format tagging and
   versioning are not handled explicitly.
3. `IVF_ON_DISK` filter-aware support can look correct in unit tests while accidentally degrading
   to post-filter behavior under load if the bitmap is applied after candidate truncation.
4. Approximate radius capability can be misreported if Pinot marks support based on candidate
   retrieval rather than the full query contract.
5. Mutable parity can expand into realtime indexing scope creep. Keep the phase on convergence and
   explainability, not new mutable backend implementations.

## Test Plan

Per PR, add only the tests needed for the behavior being changed:

- PR2 quantizers
  - validator/config unit tests
  - reader/creator round-trip tests for `SQ8` and `SQ4`
  - explain/debug tests for effective quantizer reporting
- PR3 HNSW runtime controls
  - unit tests proving `vectorEfSearch` affects the actual HNSW reader call path
  - query-option parsing tests
  - integration test covering accepted query options and explain output
- PR4 IVF_ON_DISK pre-filter
  - unit tests for `getDocIds(..., preFilterBitmap)`
  - operator tests ensuring `FILTER_THEN_ANN` activates only when supported
- PR5 radius parity
  - cosine and Euclidean threshold tests
  - capability-flag tests matching real backend behavior
- PR6 mutable parity
  - mixed consuming/immutable integration coverage
  - explain/debug fallback reporting tests
- PR7 benchmarks and docs
  - perf harnesses for quantizers, HNSW controls, IVF_ON_DISK filtering, radius, and mixed
    mutable/immutable workloads

## Deferred Items

These should stay out of the final phase unless they collapse into trivial follow-ups:

- Full mutable IVF-family ANN indexing
- New SQL syntax for vector runtime controls
- Hidden heuristics that change backend choice without explain output
- Native or JNI-backed vector libraries
