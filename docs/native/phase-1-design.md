# Phase 1 Detailed Design: Native Aggregation + Group-By Kernels

**Status:** Draft (revised 2026-05-20 after SSE/MSE engine landscape investigation)
**Parent doc:** `../../RUST_REWRITE_DESIGN.md`

---

## 1. Scope

### In scope

- **Aggregations:** `SUM`, `COUNT`, `MIN`, `MAX` (HLL deferred to Phase 1.E — see §15 Decision Log)
- **Data types:** `INT`, `LONG`, `FLOAT`, `DOUBLE` (fixed-width primitives only)
- **Forward index formats:** dictionary-encoded fixed-bit (`FixedBitSVForwardIndexReader`) and raw fixed-byte (`FixedByteChunkSVForwardIndexReader`, uncompressed only). POC uses materialized Java arrays via `BlockValSet`; direct `PinotDataBuffer` access is Phase 1.B.
- **Group-by:** single-column, integer-keyed (dictionary IDs from a dict-encoded column)
- **Segment type:** offline immutable segments + realtime consuming segments + star-tree (all inherited automatically — see §2)
- **Engine coverage:** all five aggregation contexts (see §2.1) via a single integration point

### Out of scope (deferred or never)

- HLL distinct count — Phase 1.E (deferred 2026-05-20; see §15)
- String columns (`MIN(strCol)`, `MAX(strCol)`, dictionary string keys for group-by) — Phase 1+
- Multi-value (MV) columns — Phase 1+
- Multi-column group-by — Phase 1+
- Null handling (`nullHandlingEnabled`) — added at end of Phase 1, behind sub-flag
- `BigDecimal` precision for `SUM` — Phase 1+ (POC stays in `double`)
- Aggregations beyond the four listed (PERCENTILE, AVG, etc.) — Phase 1+
- Compressed `FixedByteChunkSV` forward index — handled by Java fallback in Phase 1
- MSE non-aggregation operators (Filter, Project, Sort, Window, Join)
- Broker-side merge of intermediate results

---

## 2. Engine landscape: where this acceleration applies

**Verified by code reading 2026-05-20.** This section is foundational. Future maintainers should read it before changing the integration point.

### 2.1 The five aggregation contexts

Pinot has multiple execution contexts in which aggregation happens. They share more than they appear to.

| # | Context | Operator class | Data shape | AggregationFunction reuse | File path |
|---|---|---|---|---|---|
| 1 | **SSE / V1 per-segment** | `AggregationOperator`, `GroupByOperator` | `ValueBlock` (columnar from segment) | Original | `pinot-core/src/main/java/org/apache/pinot/core/operator/query/AggregationOperator.java` |
| 2 | **MSE leaf stage** | Delegates to V1 via `LeafOperator._queryExecutor.execute(...)` | Same `ValueBlock` as V1 | Same classes | `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/LeafOperator.java:510` |
| 3 | **MSE intermediate stage** | `AggregateOperator` (MSE) + `MultistageAggregationExecutor` + `MultistageGroupByExecutor` | `RowHeapDataBlock` wrapped in `DataBlockValSet` / `RowBasedBlockValSet` adapters | Same classes | `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/AggregateOperator.java` |
| 4 | **Star-tree** | `StarTreeAggregationExecutor extends DefaultAggregationExecutor` | `ValueBlock` (pre-aggregated columns) | Same classes | `pinot-core/src/main/java/org/apache/pinot/core/startree/executor/StarTreeAggregationExecutor.java` |
| 5 | **Realtime consuming segments** | Same V1 path | Same `ValueBlock` (realtime impl) | Same classes | no separate path — uses #1 |
| 6 | **SSE Materialized View refresh** | V1 SSE via broker query path | Same as V1 | Same classes | `pinot-materialized-view/src/main/java/org/apache/pinot/materializedview/executor/MaterializedViewQueryExecutor.java` |

### 2.2 The architectural insight

**Every aggregation context in Pinot — both engines, both query stages, star-tree, realtime, and MV refresh — calls `AggregationFunction.aggregate(int length, AggregationResultHolder, Map<ExpressionContext, BlockValSet>)`** (or the corresponding `aggregateGroupBySV` / `aggregateGroupByMV` for grouped queries).

They differ only in:
- How the `ValueBlock` is constructed (segment scan vs row-block adapter)
- Which downstream merge handles the intermediate results
- Whether group-by is involved (separate `aggregateGroupBySV` / `aggregateGroupByMV` invocation)

All `AggregationFunction` instances are obtained from a single factory:

```
pinot-core/src/main/java/org/apache/pinot/core/query/aggregation/function/AggregationFunctionFactory.java
  → AggregationFunctionFactory.getAggregationFunction(FunctionContext, boolean nullHandlingEnabled)
```

**Routing at this factory layer reaches every context above with one change point.**

### 2.3 Coverage of Phase 1 integration

| Query shape | Accelerated by Phase 1? | Mechanism |
|---|---|---|
| Pure SSE `SELECT SUM(x) FROM t GROUP BY y` | Yes | V1 path uses native AggregationFunction |
| MSE leaf-pushdown agg (single-table, no JOIN) | Yes | MSE `LeafOperator` delegates to V1; same path |
| MSE post-JOIN agg (intermediate stage) | Yes | MSE intermediate reuses same AggregationFunction; native kicks in via factory |
| MSE post-shuffle final agg | Yes | Same |
| Star-tree aggregation | Yes | `StarTreeAggregationExecutor` extends V1 executor; calls same AggregationFunction |
| Realtime consuming-segment agg | Yes | Same V1 path; only the ValueBlock impl differs |
| MV refresh aggregation | Yes | V1 SSE query path |

This was the surprise of the investigation: **a single integration point covers all aggregation in Pinot.** The MSE intermediate stage reuses V1's `AggregationFunction` classes because the MSE engineers chose to layer `DataBlockValSet` / `RowBasedBlockValSet` adapters on top of MSE's `RowHeapDataBlock` rather than rewrite the aggregation library.

### 2.4 Caveat — row → column extraction cost in MSE intermediate

The MSE intermediate stage processes rows (`RowHeapDataBlock` is `List<Object[]>`). To call `AggregationFunction.aggregate(ValueBlock)`, the rows must be transposed into columnar `BlockValSet`s — this is the work that `DataBlockValSet` / `RowBasedBlockValSet` do per-block.

Implication for Phase 1: the native kernel runs after that row-to-column extraction has already happened. We **accelerate the aggregation kernel** but **do not eliminate the extraction cost**. For MSE intermediate aggregation, the proportional speedup will be smaller than for V1 leaf because extraction overhead is a larger fraction of total time.

For V1 leaf, MSE leaf-pushdown, star-tree, realtime, and MV refresh, the `ValueBlock` is constructed directly from columnar segment data — no transpose cost. Full speedup applies.

This is a real but acceptable trade-off for Phase 1. Eliminating MSE intermediate extraction overhead is a separate, larger effort (likely requires changing MSE's exchange format to be columnar) and is not in Phase 1 scope.

### 2.5 What this does NOT cover

- **MSE non-aggregation operators** — Filter, Project, Sort, Window, Join. Not in Phase 1 scope (some Phase 2+, some never).
- **Broker-side merge** of intermediate results in `BrokerReduceService`. Would need its own integration point; deferred indefinitely (broker is not CPU-bound on most queries).
- **MSE WindowAggregateOperator** — windowed aggregations like `SUM(x) OVER (PARTITION BY y)`. Uses a different code path (`WindowAggregateOperator` in `pinot-query-runtime`); not covered by factory routing. Phase 1+.

### 2.6 Cross-engine intermediate result compatibility

Since our native `AggregationFunction` subclasses inherit `createAggregationResultHolder()` / `createGroupByResultHolder()` / `extractAggregationResult()` / `merge()` / `extractFinalResult()` from their Java parents unchanged, **the intermediate result types are identical** to the Java path. A `Double` produced by `NativeSumAggregationFunction` is byte-for-byte indistinguishable from one produced by `SumAggregationFunction`. The broker merge layer needs no changes; mixed-version clusters (native server + Java server) work transparently.

---

## 3. Architecture overview

```
┌─────────────────────────────────────────────────────────────┐
│ AggregationFunctionFactory.getAggregationFunction(...)      │
│                                                              │
│   if (NativeAggregationRouter.shouldAccelerate(fn, ctx))    │
│      → NativeSumAggregationFunction / NativeMin / ...       │
│   else                                                       │
│      → SumAggregationFunction / MinAggregationFunction / ...│
└─────────────────────────────────────────────────────────────┘
                              │
                              │  used by ANY of:
                              │  - V1 AggregationOperator      (SSE per-segment)
                              │  - MSE LeafOperator → V1       (MSE leaf)
                              │  - MSE AggregateOperator       (MSE intermediate)
                              │  - StarTreeAggregationExecutor (star-tree)
                              │  - realtime, MV refresh
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ Native*AggregationFunction.aggregate(length, holder, bvsMap) │
│                                                              │
│   bvs = bvsMap.get(_expression)                              │
│   switch (bvs.getValueType().getStoredType()) {              │
│     case LONG:                                               │
│       long[] vals = bvs.getLongValuesSV();                   │
│       double sum = PinotNativeAgg.sumLong(vals, length);     │
│       holder.setValue(holder.getDoubleResult() + sum);       │
│       return;                                                │
│     case INT/FLOAT/DOUBLE: ...                               │
│     default: super.aggregate(...);  // fallback              │
│   }                                                          │
└─────────────────────────────────────────────────────────────┘
                              │   JNI (~1 call per function per block)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ pinot-native (Rust)                                          │
│                                                              │
│   Kernels       (SUM/COUNT/MIN/MAX, SIMD)                   │
│   GroupTable    (SwissTable + per-group state, vectorized)  │
│   DictUnpack    (FixedBit bit-packed reader — Phase 1.B)    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                  Java primitive arrays (via GetPrimitiveArrayCritical)
```

### Key choices

- **Integration point:** `AggregationFunctionFactory`, not plan-maker. Covers all five aggregation contexts (§2.1) with one fork.
- **No new operator/plan-node classes.** Standard `AggregationOperator`, `AggregationPlanNode`, `DefaultAggregationExecutor` are reused unchanged.
- **JNI granularity (POC):** per-function-per-block. For a 10M-row segment with 3 functions, ~3000 calls ≈ 150µs of JNI overhead. Acceptable for POC; Phase 1.D will batch to per-block-all-functions.
- **Buffer access (POC):** `GetPrimitiveArrayCritical` on the materialized Java primitive array returned by `BlockValSet.getLongValuesSV()`. True zero-copy from `PinotDataBuffer` is Phase 1.B.
- **Result handling:** native subclass inherits parent's `AggregationResultHolder` / `extractAggregationResult` / `merge` — no result-type drift, no broker-side change.

---

## 4. JNI interface

### 4.1 POC interface (per-function-per-block)

```java
public final class PinotNativeAgg {
    static native boolean isAvailable();
    static native int probe();   // self-test: returns 0x5049_4E4F == 'PINO'

    // SUM kernels — Phase 1.A POC has only sumLong; rest land in Phase 1.B
    static native double sumLong(long[] values, int length);
    static native double sumInt(int[] values, int length);
    static native double sumFloat(float[] values, int length);
    static native double sumDouble(double[] values, int length);

    // MIN / MAX kernels — Phase 1.B
    static native long minLong(long[] values, int length);
    static native long maxLong(long[] values, int length);
    // ... int/float/double variants

    // COUNT is trivial in Java (length itself); not exposed natively.

    // Group-by kernels — Phase 1.D
    // (signature TBD; will take grouped agg context handle)
}
```

### 4.2 Lifecycle (POC)

The POC is **stateless**. Each native call takes its inputs and returns a scalar. No per-query context, no per-segment plan handle. This is the simplest possible interface.

For Phase 1.D, we introduce a thread-local `NativeAggregationContext` that batches multiple aggregations within a block; the more complex lifecycle from the original §3 (per-query / per-segment / per-block handles) comes back at that point.

### 4.3 Buffer ownership rules

- Native code uses `GetPrimitiveArrayCritical` to pin the Java primitive array for the duration of the call.
- No JNI methods that allocate or block are issued while the critical pin is held.
- The native function does NOT free the array. Ownership stays in Java.

### 4.4 Error handling

- All native paths wrapped in `std::panic::catch_unwind`.
- On failure, the native function returns a sentinel: `NaN` for `double`, `Long.MIN_VALUE` for `long`, etc.
- The Java side checks for the sentinel and falls back to `super.aggregate(...)` — degrading gracefully to the Java path for that one block.

### 4.5 Why per-function-per-block is acceptable for POC

- Per-row JNI: ~10K calls × 50ns = 500µs/block. Kernel work is faster. Killer.
- Per-function-per-block (POC): ~3 calls/block × 1000 blocks/segment = 3000 calls ≈ 150µs/segment. ~3× overhead vs target. Acceptable.
- Per-block-all-functions (Phase 1.D): 1 call/block × 1000 blocks/segment ≈ 50µs/segment. Target.
- Per-segment: 1 call/segment ≈ 50ns. Couples Rust to Pinot block size and operator-level cancellation; not worth it.

---

## 5. Forward index access from Rust

### 5.1 POC approach: via BlockValSet materialized arrays

The POC takes the path of least resistance. `BlockValSet.getLongValuesSV()` returns a `long[]` already materialized by Pinot's existing forward index reader code. We pin that array via `GetPrimitiveArrayCritical` and SIMD over it. No new Rust-side forward index reader.

Cost: The Java path *also* calls `BlockValSet.getLongValuesSV()` in its aggregate loop (`SumAggregationFunction.aggregateSV`), so we're not adding any extraction cost the Java path doesn't pay. We just SIMD over the result.

Win surface: the SIMD reduction itself + elimination of Java JIT method-dispatch overhead per block.

### 5.2 Phase 1.B: zero-copy from PinotDataBuffer

For the larger kernel win we'd want to **eliminate** the `BlockValSet.getLongValuesSV()` materialization entirely — read forward index bytes directly from `PinotDataBuffer` in Rust, SIMD-unpack dictionary IDs, SIMD-lookup dictionary, SIMD-sum. This is Phase 1.B.

Requires:
- Public `PinotDataBuffer.toNativeAddress()` returning `OptionalLong` (decided 2026-05-20 — see §15)
- Rust-side bit-unpacker for `FixedBitSVForwardIndexReader`
- Rust-side big-endian byte-swap (Pinot index files are big-endian, hosts are little-endian)

### 5.3 Compressed chunks

Not in Phase 1 scope. If a column uses chunk compression (LZ4/ZSTD), the native router's eligibility check fails and Java path handles it.

---

## 6. Aggregation kernels

### 6.1 SUM

- **i32, i64:** match Java semantics — per-value `int → double` / `long → double` conversion with straight `+=` accumulation. No Kahan compensation (Java path doesn't use it).
- **f32, f64:** straight reduce.
- **SIMD:** 256-bit lanes (AVX2) → 4×i64 or 8×i32 per cycle; 512-bit lanes on AVX-512 capable hosts; 128-bit NEON on ARM.
- **POC implementation:** scalar Rust with manual 4-way unroll, relying on LLVM auto-vectorization. Explicit intrinsics in Phase 1.B.
- **Tail handling:** scalar loop for `length % laneWidth`.

### 6.2 MIN / MAX

- Signed min/max (Pinot only has signed primitives).
- SIMD min/max per lane, then horizontal reduce.
- **NaN handling:** Java's `Math.min(NaN, x) == NaN`. Native must match exactly. Validate via differential test.

### 6.3 COUNT

- Trivial: `count = length` if non-null-aware.
- Null-aware (deferred): popcnt over null bitmap.
- COUNT is fast enough in Java that there is no native version — saves a JNI call.

### 6.4 HLL — DEFERRED

Deferred to Phase 1.E per decision 2026-05-20 (§15). Rationale: clearspring HLL byte-exact parity is the highest-risk single deliverable in Phase 1; deferring it lets the rest of Phase 1 ship without that risk on the critical path.

### 6.5 Kernel dispatch table

```rust
fn kernel_for(function: AggFn, ty: DataType) -> KernelFn {
    match (function, ty) {
        (AggFn::Sum, DataType::I32) => sum_i32_avx2,
        (AggFn::Sum, DataType::I64) => sum_i64_avx2,
        (AggFn::Sum, DataType::F32) => sum_f32_avx2,
        (AggFn::Sum, DataType::F64) => sum_f64_avx2,
        (AggFn::Min, DataType::I32) => min_i32_avx2,
        // ... etc
    }
}
```

Runtime ISA dispatch via `is_x86_feature_detected!("avx512f")` at context creation, NOT compile-time `target_feature`.

---

## 7. Group-by hash table

### 7.1 Design

SwissTable-style (cribbed from Abseil / hashbrown):

- Open addressing with quadratic probing
- 1-byte **control bytes** per slot, scanned with SIMD (16 bytes per 128-bit lane)
- Control byte encodes: empty, deleted, or low 7 bits of hash
- Per slot: `(group_id: u32, per_function_state: [...])`
- Per-function state stored in a parallel struct-of-arrays for cache efficiency:
  - `sum_state: Vec<f64>` indexed by group_id
  - `min_state: Vec<i64>` indexed by group_id
- Hash table stores `group_key → group_id`; aggregate state lives in SoA arrays.

### 7.2 Vectorized batch lookup

For a block of 10K group keys:
1. Compute hash for 16 keys in parallel (SIMD multiply + shift)
2. Compute initial bucket for each
3. Probe in parallel — read 16 control bytes per bucket, SIMD compare against hash low-7 bits
4. Resolve matches and insertions in scalar fallback for the rare slow path

DuckDB / Photon technique. SIMD-compares 16 candidate slots at once.

### 7.3 Sizing

- Pre-size to `groupKeyUpperBound` if known (from `DictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound()`)
- Start at 1024 and grow by 2× with rehash otherwise
- **No spill in Phase 1.** If `NUM_GROUPS_LIMIT` hit, return error code and fall back to Java. Matches Pinot's existing trim semantics roughly; full parity deferred.

### 7.4 Cardinality regimes

Phase 1 benchmark must cover all three regimes:
- ≤ 256 groups: hot in L1
- 256 – 100K groups: hot in L2/L3
- > 100K groups: starts to miss; hash table quality matters more

---

## 8. Operator integration

### 8.1 Routing at AggregationFunctionFactory

Single change point: `pinot-core/src/main/java/org/apache/pinot/core/query/aggregation/function/AggregationFunctionFactory.java`'s `getAggregationFunction(FunctionContext, boolean nullHandlingEnabled)` method.

Add a feature-flag gate (conceptual; final form may differ):

```java
public static AggregationFunction getAggregationFunction(
    FunctionContext function, boolean nullHandlingEnabled) {

    AggregationFunction base = createBaseAggregationFunction(function, nullHandlingEnabled);
    if (NativeAggregationRouter.shouldAccelerate(function, nullHandlingEnabled)) {
        return NativeAggregationRouter.wrap(base, function);
    }
    return base;
}
```

`NativeAggregationRouter.shouldAccelerate` checks (short-circuiting):
1. `PinotNativeAgg.isAvailable()` (library loaded)
2. Feature flag (server / table / query option)
3. Function type ∈ {SUM, COUNT, MIN, MAX} (Phase 1 scope)
4. Null handling disabled
5. Aggregated expression is a simple column reference (no transforms)

If any check fails, return the original Java `AggregationFunction`.

### 8.2 New classes (revised)

```
pinot-core/src/main/java/org/apache/pinot/core/query/aggregation/function/
  NativeSumAggregationFunction.java     (extends SumAggregationFunction)
  NativeMinAggregationFunction.java     (extends MinAggregationFunction)
  NativeMaxAggregationFunction.java     (extends MaxAggregationFunction)
  NativeAggregationRouter.java          (routing + eligibility)
```

`NativeCountAggregationFunction` is **not** created — COUNT is trivial in Java and the JNI overhead would dwarf any kernel speedup.

Each `Native*AggregationFunction` overrides:
- `aggregate(int length, AggregationResultHolder, Map<ExpressionContext, BlockValSet>)`
- `aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder, Map<...>)`

Both methods follow the same pattern: dispatch on `getStoredType()`, call into `PinotNativeAgg` for in-scope types, fall back to `super.aggregate(...)` / `super.aggregateGroupBySV(...)` otherwise.

### 8.3 No new plan nodes or operators

The original §7 in the pre-2026-05-20 draft of this doc proposed `NativeAggregationPlanNode` / `NativeGroupByPlanNode` / `NativeAggregationOperator` / `NativeGroupByOperator`. **All four are dropped.** The standard `AggregationPlanNode` / `AggregationOperator` / `DefaultAggregationExecutor` are used unchanged.

This is the single biggest revision in the 2026-05-20 update. Reason: the engine-landscape investigation showed that the original plan-maker fork would have missed MSE intermediate aggregation entirely (different operator factory). Factory-level routing covers all five engines uniformly.

### 8.4 PinotDataBuffer.toNativeAddress()

Decided 2026-05-20 (§15): add a public `PinotDataBuffer.toNativeAddress()` returning `OptionalLong`, with `UnsafePinotBuffer` overriding to return the native address. **Deferred to Phase 1.B** — the POC uses `BlockValSet.getLongValuesSV()` + `GetPrimitiveArrayCritical`, no need for direct buffer access yet.

### 8.5 Per-block-all-functions batching (deferred to Phase 1.D)

The per-block-all-functions JNI optimization (50µs/segment overhead vs 150µs at per-function-per-block) is deferred to Phase 1.D. Implementation sketch:
1. Thread-local `NativeAggregationContext` allocated per-query (or per-operator-init)
2. Each `Native*AggregationFunction.aggregate()` enqueues into the context instead of issuing JNI immediately
3. A wrapping executor (or a hook in `DefaultAggregationExecutor`) flushes the context at end-of-block
4. The wrapping-executor approach reintroduces something like the plan-maker fork — but layered ON TOP of an already-working factory-routed integration, so the work is purely additive.

Until 1.D, JNI granularity is per-function-per-block.

---

## 9. Configuration

### 9.1 Server-level

`pinot-spi/CommonConstants.Server`:
- `pinot.server.query.native.aggregation.enabled` — boolean, default `false` initially, `true` after Phase 1 GA
- `pinot.server.query.native.aggregation.minSegmentRows` — int, default `100_000`. Don't bother with JNI overhead on tiny segments.

### 9.2 Table-level

`QueryConfig`:
- `nativeAggregationEnabled` — optional boolean, table override

### 9.3 Query option

Query option `useNativeAggregation=true|false` for A/B testing.

### 9.4 Routing precedence

```
queryOption > tableConfig > serverConfig
```

---

## 10. Differential testing

`pinot-native-difftest` module (new):

1. **Schema-driven query generator:** for each known table schema, generate random queries:
   ```
   SELECT <agg list> [FROM t] [WHERE pred] [GROUP BY col]
   ```
   restricted to in-scope types and aggregations.
2. **Execute each query twice** — once with `useNativeAggregation=true`, once with `false`.
3. **Diff:**
   - Non-grouped: scalar equality with epsilon for floats (`|a - b| ≤ max(|a|, |b|) * 1e-12`)
   - Grouped: result-set equality (order-insensitive)
4. **Run as CI gate:** ≥10K queries pre-merge, ≥100K nightly.

### 10.1 Tricky cases the differ MUST cover

- Empty segment (length=0)
- Single-row segment
- All-equal values (group-by has one group)
- All-unique values (group-by has N groups)
- INT overflow on SUM
- LONG overflow on SUM (Java wraps; we must match)
- NaN/Inf on float aggregations
- Negative values, zero values, boundary values (`Integer.MIN_VALUE`, etc.)
- **Cross-engine:** same query run on SSE and MSE must produce identical results in both Java and native modes
- **Star-tree on/off:** same query with and without star-tree must produce identical results in both modes
- **Realtime + offline:** hybrid table with one consuming and one committed segment

---

## 11. Phase 1 benchmarks

JMH benchmarks under `pinot-perf` module:

1. **NonGroupedAggBench** — 100M rows, dictionary-encoded LONG, cardinality 10K. SUM / MIN / MAX separately and combined. Threads 1, 4, 16. Target: ≥4× single-thread, ≥3× at 16 threads.
2. **GroupedAggBench** — 100M rows, group-by INT column, cardinality 256 / 10K / 1M. `SUM(longCol) GROUP BY intCol`. Target: ≥3× single-thread on all cardinalities.
3. **EndToEndQueryBench** — full Pinot query lifecycle. Target: ≥25% CPU reduction on aggregation-dominant queries.
4. **MseEndToEndBench** — same query via MSE engine (both leaf-pushdown and post-JOIN variants). Quantifies the MSE intermediate row-extraction cost. Target: ≥15% on MSE leaf-pushdown; **best-effort** on MSE intermediate (≥5%).
5. **RegressionBench** — non-accelerated workloads (string aggregation, HLL via Java path, large blocks with native disabled). Target: <5% slowdown.

---

## 12. Risks specific to Phase 1

| Risk | Mitigation |
|---|---|
| JNI handle / array-pin leaks → native arena grows unbounded | All `GetPrimitiveArrayCritical` calls bracketed by `ReleasePrimitiveArrayCritical` (handled by `jni-rs` `AutoElements` RAII guard). Phase 1.D context-handle path: always destroy in `finally`. |
| AVX-512 enabled on host but unsupported by CPU → SIGILL | Use `is_x86_feature_detected!` at runtime, NOT compile-time `target_feature`. |
| Vectorized group-by has subtle wrong-result bug for specific hash collisions | Differential test with synthetically pathological key distributions. |
| Eligibility check adds latency on the Java side | Eligibility check must be O(1) per query (cached on `AggregationFunctionFactory` invocation), NOT per block. |
| MSE intermediate row→column extraction dwarfs kernel savings | Documented limitation (§2.4). Benchmark §11.4 quantifies. Eliminating extraction is a separate, larger effort. |
| Cross-engine merge semantics drift (broker receives "different" intermediates from native vs Java servers) | Native subclass inherits parent's `createAggregationResultHolder` / `merge` / `extractFinalResult` unchanged. Asserted in differential tests across mixed-engine clusters. |
| Star-tree codepath inadvertently bypassed by native engine eligibility check | Native router's eligibility check must inspect `useStarTree` and refuse acceleration when star-tree path is in use — star-tree pre-aggregation already wins; no point double-accelerating. Add as explicit eligibility rule. |
| Java JIT and our SIMD kernel actually perform within noise on small blocks | Eligibility's `minSegmentRows` threshold (default 100K) skips JNI for small segments. Tune threshold via benchmark. |

---

## 13. Open questions

Resolved 2026-05-20:
- ~~PinotDataBuffer accessor: Option A vs B vs C?~~ → **B** (public `toNativeAddress()` returning `OptionalLong`).
- ~~HLL parity strategy?~~ → **Deferred to Phase 1.E.** Removes parity risk from Phase 1 critical path.
- ~~Plan-maker fork vs factory routing?~~ → **Factory routing.** Covers all five engines uniformly.
- ~~POC depth?~~ → **Scaffold + SUM(LONG) wired into AggregationOperator.**

Still open:
1. **Star-tree eligibility.** Should native engine refuse when `_useStarTree == true` (let star-tree's pre-aggregation win)? Lean yes; add to eligibility check in Phase 1.B.
2. **Group-by hash table for high cardinality.** When group count exceeds memory, what's the spill behavior? Java path uses trim; native may need parity.
3. **String columns in aggregation.** `MIN(strCol)` / `MAX(strCol)` are common but non-trivial. In Phase 1 scope or punt? Lean punt — defer to Phase 1+.
4. **Build system.** `rust-maven-plugin` or shell-out? Lean shell-out (current POC choice with `exec-maven-plugin`); switch if it bites.
5. **MSE intermediate benchmark methodology.** How do we benchmark MSE intermediate aggregation without pulling in the entire MSE harness? Likely via `pinot-query-runtime` test fixtures. Owner: TBD.
6. **`AggregationFunctionFactory` is final or extensible?** Check whether external plugins extend it; if so, our factory fork could be bypassed by plugin functions. May need a separate decorator pattern at a different layer.

---

## 14. Phase 1 execution sequence

```
Phase 0 (foundation)               ──┐
  pinot-native module                 │  2 weeks
  Cargo workspace + jni-rs            │
  Native lib loader                   │
  Hello-world JNI + probe           ──┘

Phase 1.A (POC kernel)             ──┐
  NativeAggregationRouter             │
  NativeSumAggregationFunction (LONG) │  2 weeks
  AggregationFunctionFactory fork     │
  Cross-engine smoke tests            │
  First JMH number                  ──┘

Phase 1.B (scalar kernels + zero-copy)   ──┐
  SUM/MIN/MAX for INT/LONG/FLOAT/DOUBLE     │
  PinotDataBuffer.toNativeAddress()         │  3 weeks
  Dictionary-encoded forward index in Rust  │
  Bit-unpacking SIMD                        │
  Differential tester ≥1K queries         ──┘

Phase 1.C (operator coverage)            ──┐
  Cover MSE leaf, MSE intermediate,         │
    star-tree, realtime, MV refresh         │  2 weeks
  Star-tree eligibility decision            │
  MSE intermediate benchmark              ──┘

Phase 1.D (group-by + per-block batching) ──┐
  SwissTable                                 │
  Vectorized batch lookup                    │  3 weeks
  Per-group aggregate state                  │
  NativeAggregationContext (batching)        │
  All Phase 1 gates run                    ──┘

Phase 1.E (HLL — separate)               ──┐
  Clearspring MurmurHash3 parity            │  3 weeks
  HLL register update SIMD                  │  (separable from rest)
  Serialization parity                      │
  Differential ≥10K queries               ──┘

Phase 1 GO/NO-GO decision ── per RUST_REWRITE_DESIGN.md §4.1
```

Total ~15 weeks (was 12, +3 for HLL as its own phase). POC (Phase 0 + 1.A) is the first 4 weeks — that's the first numeric milestone.

---

## 15. Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-20 | Phase 1 target is aggregation + group-by, NOT filter scan | Filter scan acceleration is largely moot in tuned production where indexes cover the hot paths. Aggregation is index-independent and dominates post-filter CPU. |
| 2026-05-20 | Start with classic JNI, plan migration to FFM | Java 21 floor; FFM requires 22+. Boundary design is interop-agnostic. |
| 2026-05-20 | Native engine ships as opt-in via `auto` mode with java fallback | Mixed-version cluster safety; customer ability to disable instantly. |
| 2026-05-20 | HLL deferred to Phase 1.E (separate from 1.A–D) | Clearspring byte-exact parity is highest-risk single deliverable; keep it off the critical path. |
| 2026-05-20 | `PinotDataBuffer.toNativeAddress()` returns `OptionalLong` | Future-proof, fallback for non-Unsafe buffer implementations. |
| 2026-05-20 | Rust workspace with sub-crates (`ffi`, `kernels`) | Cleaner long-term separation; kernel crate testable without JNI. |
| 2026-05-20 | POC scope: scaffold + `SUM(LONG)` wired into `AggregationOperator` | First number end-to-end; validates plumbing all the way through. |
| 2026-05-20 | **Integration at `AggregationFunctionFactory`, NOT plan-maker fork** | Verified by code reading: every aggregation context in Pinot (V1, MSE leaf, MSE intermediate, star-tree, realtime, MV refresh) uses the same `AggregationFunction` base classes obtained from this factory. Single fork point covers all of them. Plan-maker fork would have missed MSE intermediate. |
| 2026-05-20 | No new `Native*Operator` / `Native*PlanNode` classes | Subsumed by the factory-routing decision. Standard operators are used unchanged; only the `AggregationFunction` instances differ. |
| 2026-05-20 | POC uses materialized Java arrays via `GetPrimitiveArrayCritical`, NOT direct `PinotDataBuffer` access | Simplest possible POC interface. Direct buffer access is Phase 1.B. |
| 2026-05-20 | JNI granularity in POC is per-function-per-block (~150µs/segment); per-block-all-functions deferred to Phase 1.D | Acceptable overhead for POC; optimization is purely additive on top of factory routing. |

---

## 16. Appendix: SSE/MSE investigation notes

Investigation conducted 2026-05-20 via code reading. Key files referenced:

**SSE / V1 path:**
- `pinot-core/src/main/java/org/apache/pinot/core/operator/query/AggregationOperator.java` — per-segment operator, instantiates `DefaultAggregationExecutor`
- `pinot-core/src/main/java/org/apache/pinot/core/query/aggregation/DefaultAggregationExecutor.java` — block loop calling `AggregationFunction.aggregate(ValueBlock)`
- `pinot-core/src/main/java/org/apache/pinot/core/query/aggregation/function/AggregationFunction.java` — interface
- `pinot-core/src/main/java/org/apache/pinot/core/query/aggregation/function/AggregationFunctionFactory.java` — factory (single fork point)

**MSE leaf → V1 bridge:**
- `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/LeafOperator.java:510` — `_queryExecutor.execute(request, _executorService, _resultsBlockStreamer)` — delegates to V1 `QueryExecutor`
- `pinot-query-planner/src/main/java/org/apache/pinot/query/planner/logical/LeafStageToPinotQuery.java` — converts MSE leaf-stage RelNode to V1 `ServerQueryRequest`
- `pinot-query-planner/src/main/java/org/apache/pinot/query/planner/physical/v2/opt/rules/LeafStageAggregateRule.java` — pushes MSE aggregate down to leaf when possible

**MSE intermediate aggregation:**
- `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/AggregateOperator.java` — operator (imports `org.apache.pinot.core.query.aggregation.function.AggregationFunction` at line 43)
- `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/MultistageAggregationExecutor.java` — per-block driver
- `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/MultistageGroupByExecutor.java` — per-block group-by driver
- `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/operator/factory/DefaultAggregateOperatorFactory.java` — factory
- `pinot-query-runtime/src/main/java/org/apache/pinot/query/runtime/blocks/RowHeapDataBlock.java` — row block format
- `DataBlockValSet` / `FilteredDataBlockValSet` / `RowBasedBlockValSet` — row→ValueBlock adapters (in `pinot-query-runtime/.../operator/`)

**Star-tree:**
- `pinot-core/src/main/java/org/apache/pinot/core/startree/executor/StarTreeAggregationExecutor.java` — extends `DefaultAggregationExecutor`

**Routing:**
- `pinot-query-planner/src/main/java/org/apache/pinot/query/parser/utils/ParserUtils.java:36-44` — `canCompileWithMultiStageEngine` decides SSE vs MSE
- `pinot-broker/src/main/java/org/apache/pinot/broker/requesthandler/BaseSingleStageBrokerRequestHandler.java` — broker engine routing
- Query option: `useMultiStageEngine`

**Materialized View:**
- `pinot-materialized-view/src/main/java/org/apache/pinot/materializedview/executor/MaterializedViewQueryExecutor.java` — MV refresh path
- `pinot-plugins/pinot-minion-tasks/pinot-minion-builtin-tasks/src/main/java/org/apache/pinot/plugin/minion/tasks/materializedview/MaterializedViewTaskExecutor.java` — minion task driver

---

*End of Phase 1 design (rev. 2026-05-20). The major revision in this version is §2 + §8 — factory-level routing replaces the plan-maker fork.*
