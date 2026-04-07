# Phase 3: Vector Query Execution Semantics

## Overview

Phase 3 adds explicit and explainable vector query execution semantics to Apache Pinot,
building on the backend-neutral vector indexing (Phase 1) and IVF_PQ compressed search (Phase 2).

**Goals:**
- Make execution mode selection explicit, centralized, and visible in explain output
- Support filtered ANN with predictable behavior and candidate budget management
- Add approximate threshold/radius search via ANN + exact refinement
- Support compound retrieval patterns (filter + top-K, filter + threshold)
- Preserve backward compatibility for all existing VECTOR_SIMILARITY queries

**Non-goals for Phase 3:**
- New ANN backends (no JNI/Faiss)
- Realtime mutable IVF/PQ
- Full cost-based optimizer
- Broad SQL redesign

## Execution Mode Model

Phase 3 defines 8 explicit execution modes in `VectorExecutionMode`:

| Mode | Description | When Selected |
|------|-------------|---------------|
| `ANN_TOP_K` | Pure ANN top-K, no filter, no rerank | Default for simple queries |
| `ANN_TOP_K_WITH_RERANK` | ANN + exact distance rerank | `vectorExactRerank=true` or IVF_PQ default |
| `ANN_THEN_FILTER` | ANN first, then metadata filter | Filter present, no rerank |
| `ANN_THEN_FILTER_THEN_RERANK` | ANN, filter, then rerank | Filter + rerank |
| `FILTER_THEN_ANN` | Filter first, then ANN on subset | Future: filter-aware backends |
| `ANN_THRESHOLD_SCAN` | ANN candidates + exact threshold | `vectorDistanceThreshold` set |
| `ANN_THRESHOLD_THEN_FILTER` | ANN + threshold + filter | Threshold + filter |
| `EXACT_SCAN` | Brute-force scan | No vector index available |

### Mode Selection Rules

Selection is centralized in `VectorQueryExecutionContext.selectExecutionMode()`:

1. No vector index -> `EXACT_SCAN`
2. Threshold + filter -> `ANN_THRESHOLD_THEN_FILTER`
3. Threshold (no filter) -> `ANN_THRESHOLD_SCAN`
4. Filter + rerank -> `ANN_THEN_FILTER_THEN_RERANK`
5. Filter (no rerank) -> `ANN_THEN_FILTER`
6. Rerank (no filter) -> `ANN_TOP_K_WITH_RERANK`
7. Default -> `ANN_TOP_K`

Threshold predicates take priority over filter-only modes. The planner favors explicit
correctness over aggressive optimization.

## Backend Capability Model

`VectorBackendCapabilities` declares what each backend can do:

| Capability | HNSW | IVF_FLAT | IVF_PQ |
|------------|------|----------|--------|
| `supportsTopKAnn` | Yes | Yes | Yes |
| `supportsFilterAwareSearch` | No | No | No |
| `supportsApproximateRadius` | No | No | No |
| `supportsExactRerank` | Yes | Yes | Yes |
| `supportsRuntimeSearchParams` | No | Yes | Yes |

This model is extensible. Future backends can declare `supportsFilterAwareSearch=true`
to enable `FILTER_THEN_ANN` mode without code changes in the planner.

## Filtered ANN Semantics

When a `VECTOR_SIMILARITY` predicate is combined with metadata filters in an AND:

1. **Detection**: `FilterPlanNode` detects the AND(VECTOR_SIMILARITY, ...) pattern
2. **Over-fetch**: ANN retrieves `topK * 2` candidates (default) to compensate for
   post-filter loss, unless `vectorMaxCandidates` is explicitly set
3. **Post-filter**: Standard Pinot bitmap AND combines ANN and filter results
4. **Explain**: Reports execution mode as `ANN_THEN_FILTER` (or `_THEN_RERANK`)

**Limitation**: Post-filter may return fewer than top-K results when the filter is
highly selective. This is documented and explicit. Future work may add filter-selectivity
estimation to choose between post-filter and exact scan.

## Threshold/Radius Search

Enabled via the `vectorDistanceThreshold` query option:

```sql
SET vectorDistanceThreshold = 0.5;
SELECT * FROM table WHERE VECTOR_SIMILARITY(embedding, ARRAY[1.0, 0.0], 100)
```

**Behavior:**
1. ANN generates candidate pool (default: `topK * 10`)
2. Exact distance is computed for each candidate using the forward index
3. Only candidates within the distance threshold are returned
4. If combined with metadata filter, mode is `ANN_THRESHOLD_THEN_FILTER`

**Limitations:**
- Requires a forward index on the vector column
- Candidate pool size limits recall for threshold queries
- Distance threshold semantics depend on the distance function (lower is closer
  for L2/cosine, higher absolute value is closer for dot product)

## Query Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `vectorNprobe` | int | 4 | IVF probe count |
| `vectorExactRerank` | boolean | backend-dependent | Enable exact rerank |
| `vectorMaxCandidates` | int | topK * 10 | Max ANN candidates |
| `vectorDistanceThreshold` | float | (none) | Distance threshold for radius search |

## Explain Output

The execution mode is now visible in both human-readable and structured explain output:

```
VECTOR_SIMILARITY_INDEX(
  indexLookUp:vector_index,
  executionMode:ANN_THEN_FILTER,
  backend:HNSW,
  distanceFunction:COSINE,
  effectiveNprobe:0,
  effectiveExactRerank:false,
  effectiveCandidateCount:20,
  topK to search:10
)
```

Structured attributes include `executionMode` as a separate field for programmatic access.

## File Layout

### SPI (pinot-segment-spi)
- `VectorBackendCapabilities` - capability metadata
- `VectorExecutionMode` - execution mode enum
- `VectorBackendType` - extended with `getCapabilities()`

### Runtime (pinot-core)
- `VectorQueryExecutionContext` - planning context with mode selection logic
- `VectorExplainContext` - extended with execution mode
- `VectorSimilarityFilterOperator` - threshold refinement, filtered over-fetch, mode reporting
- `VectorSearchParams` - extended with `distanceThreshold`
- `FilterPlanNode` - AND pattern detection for filtered ANN

### Query Options (pinot-common, pinot-spi)
- `QueryOptionsUtils` - `getVectorDistanceThreshold()` accessor
- `CommonConstants.QueryOptionKey` - `VECTOR_DISTANCE_THRESHOLD` constant

## PR Split

1. **PR1**: Backend capability model + execution mode enum (SPI)
2. **PR2**: Filtered ANN runtime semantics + explain/debug (core)
3. **PR3**: Threshold/radius search (core + common)
4. **PR4**: Compound query behavior + tests (core)
5. **PR5**: Benchmarks + design docs + release notes

## Backward Compatibility

All existing queries work unchanged:
- `VECTOR_SIMILARITY(col, vec, k)` without query options -> `ANN_TOP_K` (same as Phase 2)
- Existing nprobe, rerank, maxCandidates options work the same
- No schema or table config changes required
- No wire protocol changes

## Future Work

- Filter-selectivity estimation for automatic EXACT_SCAN fallback
- `FILTER_THEN_ANN` for backends that support filter-aware search
- Native radius search in backend readers
- Cost-based execution mode selection
- Broker-side recall optimization for distributed queries
