# Pinot Native Acceleration via Rust — Design Document

**Status:** Draft  
**Last updated:** 2026-05-20  
**Owner:** Siddharth Teotia

---

## 1. TL;DR

Progressively rewrite Pinot's CPU-bound query execution path in Rust, exposed to the JVM via JNI (later FFM/Panama). The motivation is **cost-per-QPS reduction**, **tail-latency tightening**, and **access to hardware capabilities the JVM cannot reach** — not "Rust is faster than Java" in the abstract.

The first milestone is **aggregation kernels + group-by hash table for primitive-typed columns**, not filter scans. Filter scan acceleration is deferred to a narrower Phase 2 scope covering only predicates that indexes cannot serve.

The project is gated. Phase 1 must hit explicit numeric targets; if it doesn't, the initiative stops.

---

## 2. Motivation

### 2.1 What Pinot is CPU-bound on in well-tuned production

Approximate split for a non-star-tree analytical query on a properly indexed table:

| Phase | % of CPU |
|---|---|
| Filter (index-served) | 10-20% |
| Projection / transform | 10-15% |
| Group-by hash table + aggregation | 40-60% |
| Result serialization (DataTable) | 10-15% |
| Amortized GC overhead | 5-15% |

**Implication:** The dominant cost in tuned deployments is post-filter (group-by + aggregation), not filter itself. Filter scan acceleration is a misleading first target because users with filter problems can usually fix them by adding an index.

### 2.2 Where the JVM is a real ceiling

- **GC pause budget.** Pinot's mission is sub-second p99. Even with ZGC/Shenandoah, you need 30-40% headroom to avoid pause amplification. Native code with arena allocators removes that headroom requirement.
- **Vector ISA access.** Java Vector API is incubator and lags AVX-512 / SVE2 by years. JIT auto-vectorization is opportunistic and breaks under small refactors.
- **Hash table primitives.** Java's `HashMap` and even Eclipse Collections are bounded by virtual dispatch and boxed key paths. Open-addressing tables with SIMD probing (à la SwissTable / Rust's `hashbrown`) are 3-5× faster on group-by hot paths.
- **Per-row object allocation.** Operator pipelines allocate per-block intermediate objects (DocIdSetBlock, ProjectionBlock, etc.). Even with escape analysis, this is GC pressure that arena allocation eliminates entirely.
- **Predictable execution.** Native code makes worst-case latency *bounded*. JIT deoptimization, GC pauses, and safepoint stalls collectively make Java's worst case unpredictable.

### 2.3 Where filter scans still matter (the minority case)

In a tuned deployment, filter scans persist for:
- Function/expression predicates: `lower(col) = 'x'`, `col1 + col2 > k`, `regexp_like` outside text index, `json_extract` outside JSON index
- High-cardinality columns where inverted/range indexes are storage-prohibitive
- Real-time consuming segments with weaker/no index coverage
- Selectivity-driven scan choice (predicate matches >~30% of rows)
- Wide tables where indexing every column is impractical

These are real but secondary. They justify a Phase 2 effort, not Phase 1.

### 2.4 Engine coverage — one integration point reaches both SSE and MSE

Verified by code reading 2026-05-20 (full notes in `docs/native/phase-1-design.md` §2 and §16). Pinot has two query engines:

- **SSE / V1** — the classic scatter-gather engine, dominant in production traffic today
- **MSE / V2** — the multi-stage engine for JOINs, window functions, and complex SQL

The surprise: **all aggregation in Pinot, across both engines, routes through the same `AggregationFunction` classes obtained from `AggregationFunctionFactory.getAggregationFunction(...)`.** Specifically:

| Context | Path |
|---|---|
| SSE per-segment aggregation | `AggregationFunction` direct |
| MSE leaf stage | `LeafOperator` delegates to V1 `QueryExecutor` (same `AggregationFunction`) |
| MSE intermediate stage | `AggregateOperator` instantiates same `AggregationFunction` via factory, wraps row blocks in `DataBlockValSet` |
| Star-tree | `StarTreeAggregationExecutor extends DefaultAggregationExecutor` — same calls |
| Realtime consuming segments | Same V1 path |
| Materialized View refresh | V1 SSE via broker query path |

**Implication for Phase 1 architecture:** routing at the `AggregationFunctionFactory` layer accelerates all six contexts with a single change point. The original plan (plan-maker fork in `InstancePlanMakerImplV2`) would have missed MSE intermediate aggregation. The phase-1 design doc has been revised accordingly.

**Caveat:** MSE intermediate processes rows (`RowHeapDataBlock`) and transposes them to columnar `BlockValSet`s before each aggregation call. The transpose cost is NOT eliminated by Phase 1 — only the aggregation kernel itself is. Proportional speedup on MSE intermediate aggregation will therefore be smaller than on V1 leaf / MSE leaf / star-tree / realtime / MV refresh. Eliminating that transpose is a separate, larger effort (would require columnar MSE exchange format) and is not in any phase of this initiative as currently scoped.

---

## 3. Goals

### 3.1 In scope

- Native (Rust) implementations of CPU-bound hot-path operators
- JNI boundary at the per-segment granularity to minimize call overhead
- Build/distribution pipeline that ships prebuilt native libs alongside JARs
- Feature-flag fallback so any deployment can disable native acceleration
- Differential test infrastructure proving result equivalence with the Java path
- Mixed-version cluster safety (native servers and Java servers must produce identical results)

### 3.2 Out of scope (initially)

- Replacing the broker. Broker is coordination-bound, not CPU-bound.
- Replacing the controller. Wrong layer.
- Multi-stage query engine coordination (MSQE). Stage dispatch is orchestration, not compute.
- Helix / ZooKeeper interaction. Stays in Java.
- Realtime *ingestion* path (stream consumers, segment commit). Different problem, different bottlenecks.

### 3.3 Explicit non-goals

- "Rust is faster than Java" as a project framing. Replace with cost/QPS and p99.9 targets.
- Cherry-picked microbenchmarks. All wins must show on production-representative query traces.
- Rewriting the entire server in one pass. Hybrid coexistence is the steady state for years.

---

## 4. Success Metrics

These are the gates. The project advances only if each phase meets its gate.

### 4.1 Phase 1 gates (aggregation + group-by kernels)

| Metric | Target | Measurement |
|---|---|---|
| Single-thread aggregation throughput (SUM/COUNT/MIN/MAX on INT/LONG, non-grouped) | ≥4× vs Java JIT baseline | JMH benchmark, 100M-row segment, AVX2 minimum |
| Group-by aggregation throughput (single key, 1K-100K groups) | ≥3× vs Java baseline | JMH, same dataset |
| End-to-end query CPU on a production-representative trace | ≥25% reduction | Replay of internal trace or TPC-H SF100 |
| p99.9 latency under sustained load | ≥2× tighter | Steady-state load test, 1hr window |
| Correctness vs Java path | 100% identical results | Differential tester, ≥10K randomly generated queries |
| Regression on non-accelerated workloads | <5% slowdown | Pinot's existing regression benchmark suite |

**If any gate fails:** stop, reassess. Phase 2 is not authorized.

### 4.2 Phase 2 gates (non-indexable filter predicates)

| Metric | Target |
|---|---|
| Function-predicate scan (e.g., `lower(col) = 'x'`) | ≥3× single-thread |
| Realtime-segment scan filter | ≥2× single-thread |
| Correctness | 100% |
| Regression on index-served filters | <5% |

### 4.3 Long-term north-star metrics

- **CPU cost per QPS at p99 SLA:** -40% over 24 months
- **Required GC headroom per host:** -30% (allowing higher segment density)
- **p99.9 latency on customer-tier SLA:** -50%

---

## 5. What Won't Move (Honest Disclosure)

Be ready to explain why these don't improve when reviewers ask:

- **I/O-bound queries** — disk read latency dominates; Rust changes nothing
- **Network-bound scatter-gather** — broker→server RPC is the bottleneck
- **Inverted-index equality lookups** — already byte-shuffle bound; Java RoaringBitmap and CRoaring perform similarly
- **Star-tree-served queries** — pre-aggregated; the work is already done
- **MSQE coordination overhead** — orchestration cost
- **Startup / segment load time** — likely *worse* initially due to native lib loading
- **Cold-cache page faults** — kernel issue, not language issue

The benchmark mix must include some of these so reviewers see we're honest about scope.

---

## 6. Architecture

### 6.1 Interop technology

**Decision:** Start with classic **JNI**. Design the boundary so swapping to **FFM (Project Panama)** later is mechanical.

Rationale:
- JNI works on Java 21 (Pinot's current floor)
- FFM is Java 22+; not yet a deployment-realistic floor
- Per-call overhead of classic JNI (~10-50ns) is acceptable *if* we keep the call boundary at per-segment granularity, not per-row
- Migration path: when Java 22 becomes the floor, the Rust side stays unchanged; only the Java-side stub changes

Rust binding crate: `jni-rs`. Mature, well-maintained.

### 6.2 Memory ownership

- **Forward/inverted indexes:** Pinot already MMaps these into `PinotDataBuffer`. Rust borrows zero-copy via `slice::from_raw_parts` from the buffer's native address. No copy.
- **Intermediate state (hash tables, partial aggregates):** Owned in Rust arena allocators, freed when the per-query native context is dropped.
- **Result handles:** Two patterns:
  - **Pattern A (serialize-and-copy):** Rust serializes result into a byte buffer; Java deserializes. ~10-50µs per segment. Simple. Use for first cut.
  - **Pattern B (opaque native handle):** Rust returns a `long` handle; subsequent operators read it via additional JNI calls. Faster but expands the boundary.
- **Lifecycle:** Each query owns a native context (`long ctx`) allocated at start, freed in `finally`. Per-segment work allocates from the context's arena.

### 6.3 Result interop for aggregation

Phase 1 results are small (scalar aggregates or group-by hash tables with O(num groups) entries). Pattern A (serialize) is fine for non-grouped; group-by may need Pattern B if the group cardinality is high enough that serialization dominates.

### 6.4 Build & distribution

- Rust crate lives at `pinot-native/` (new top-level module) with submodules per operator family
- Cargo build invoked from Maven via `rust-maven-plugin` (or shell-out with explicit `cargo` command)
- Cross-compilation matrix:
  - linux-x86_64 (AVX2 minimum, AVX-512 detected at runtime)
  - linux-aarch64 (NEON minimum, SVE2 detected at runtime)
  - darwin-aarch64 (NEON, Apple Silicon)
  - darwin-x86_64 (AVX2)
  - windows-x86_64 (optional, on demand)
- Prebuilt `.so` / `.dylib` / `.dll` packaged inside the JAR under `native/<os>-<arch>/`
- Java loader: extract to temp dir on first use, `System.load`, cache

### 6.5 Fallback and feature flag

- Server config: `pinot.server.query.engine=java|native|auto` (default `auto`)
- Table override: same key under table config
- Query option: `useNativeEngine=true|false` for A/B testing
- `auto` = native if library loaded successfully and arch supported, else java
- **Hard requirement:** A server with native disabled must return identical results to one with native enabled. Mixed-version safe.

### 6.6 Differential testing

- New module `pinot-native-difftest`
- Random query generator parameterized by schema (use Pinot's existing schema fixtures)
- Each generated query executes twice: once with `useNativeEngine=true`, once with `false`
- Diff: row sets, scalar aggregates, group-by results (order-insensitive)
- Run as a CI gate on every native-engine PR
- Target: ≥10K queries pre-merge, ≥100K queries per nightly run

### 6.7 Observability

- Per-operator timer split: "java" vs "native" wall time
- Memory: native arena high-water mark per query, exported as a metric
- Crash diagnostics: native panic captured, converted to Java exception, native stack written to a local diagnostic file (not server logs — too noisy)

---

## 7. Phased Execution Plan

### Phase 0 — Foundation (2-3 weeks)

**Goal:** Plumbing exists end-to-end, with a trivial native call returning a known value.

- [ ] Create `pinot-native/` Maven + Cargo module
- [ ] CI matrix building native libs on linux-x64, darwin-arm64 minimum
- [ ] Native lib loader in Java with arch detection and fallback
- [ ] `pinot-native-difftest` module skeleton
- [ ] Trivial "echo" JNI call exercised in a unit test
- [ ] Documentation in `pinot-native/README.md` for contributor onboarding

**Exit criteria:** A no-op native call works on at least two architectures. No correctness or performance claims yet.

### Phase 1 — Aggregation + Group-By Kernels (8-12 weeks)

**Goal:** Demonstrate the value proposition on a real production hot path.

Scope:
- Non-grouped aggregations: `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` over INT/LONG/FLOAT/DOUBLE forward index (dictionary-encoded and raw)
- Single-key group-by with the same aggregations
- Dictionary expansion (gather) when dictionary-encoded
- Forward-index reading via zero-copy MMap borrow

Deliverables:
- JMH benchmarks for each aggregation kernel, isolated
- End-to-end query benchmark via Pinot's existing harness
- Differential test corpus of ≥10K aggregation queries
- Production trace replay on at least one internal workload

**Exit criteria:** All Phase 1 gates in §4.1 met. If not met, project stops here.

### Phase 2 — Non-Indexable Filter Predicates (8-12 weeks)

**Goal:** Cover the filter scans that indexes cannot eliminate.

Scope:
- Function predicates: `lower/upper/length/substring + comparison`
- Arithmetic predicates: `col1 op col2`, `col +/- k op c`
- JSON path predicates outside JSON-index coverage
- Realtime consuming-segment scans
- Selectivity-driven scan fallback in the optimizer

Out of scope:
- Index-served filter (no win, see §5)
- Star-tree filter
- Text-index filter

### Phase 3 — Projection + Transform (8-12 weeks)

**Goal:** Native projection of forward-index slices and basic scalar transforms.

### Phase 4 — Sketch Aggregations (timeline TBD)

Distinct count via HLL/Theta, percentile via KLL/T-digest. High SIMD potential, isolated kernels.

### Phase 5 — Segment Reader Ownership (open-ended)

The rubicon. Rust owns segment loading, index reading, MMap lifecycle. Significant rewrite of `pinot-segment-local`. Authorized only after Phases 1-4 demonstrate cumulative ROI.

---

## 8. Failure Modes

Ordered by likelihood, not severity. For each: detection signal and mitigation.

### 8.1 JNI overhead eats the SIMD win

**Symptom:** Per-segment native call costs (~5-20µs) accumulate to where end-to-end gain is <20% despite kernel speedup >5×.  
**Detection:** Per-operator timer shows native wall time competitive with Java, but query-level CPU savings underwhelm.  
**Mitigation:** Batch segments per JNI call where the broker can group them. Move to FFM/Panama when Java 22 is the floor.

### 8.2 The benchmark is cherry-picked

**Symptom:** Phase 1 shows 5× on one query class, real workload moves <10%.  
**Detection:** Production trace replay diverges from JMH.  
**Mitigation:** §4.1 gates require both isolated JMH *and* trace replay. Both must pass. Don't replace trace replay with synthetic.

### 8.3 Differential tester finds a real bug post-launch

**Symptom:** A customer query returns different results under native vs java.  
**Detection:** Differential tester missed an edge case (NaN, overflow, locale, null handling, decimal precision).  
**Mitigation:** Native engine ships in `auto` mode with table-level disable. Operations can revert any table instantly. Add the failing case to the differential corpus permanently.

### 8.4 Native lib distribution breaks on a customer arch

**Symptom:** Customer on `darwin-x86_64` or `windows-x86_64` (not in default matrix) can't start the server.  
**Detection:** Server startup logs report load failure; falls back to java path automatically.  
**Mitigation:** `auto` mode logs a warning but does not fail. Java path is always present. Document the supported matrix prominently.

### 8.5 Mixed-version cluster produces inconsistent results

**Symptom:** Native server A and java server B return different rows for a query that fans out to both.  
**Detection:** Result diff in scatter-gather merge; or customer report.  
**Mitigation:** Java is always the reference. Native must match exactly, validated by the differential tester on every operator change. Treat any divergence as P0.

### 8.6 The Rust codebase becomes unmaintained

**Symptom:** Only one or two people understand the native code; PRs sit; the experiment becomes a liability.  
**Detection:** Calendar.  
**Mitigation:** From day one, native code reviews require two reviewers. Onboarding docs in `pinot-native/README.md`. Quarterly internal training. If the project survives Phase 2, hire at least one engineer with the explicit charter of maintaining the native code.

### 8.7 GC wins don't materialize because Java path still owns the bookkeeping

**Symptom:** Native engine reduces compute but Java still allocates DocIdSetBlock, ProjectionBlock, etc. — GC pauses unchanged.  
**Detection:** GC log analysis post-Phase-1.  
**Mitigation:** Plan Phase 1 deliverables to include reducing per-block Java allocations on the accelerated path. Don't claim GC wins until measured.

### 8.8 We rebuild and ship something the JVM ecosystem already provides

**Symptom:** Someone points at GraalVM native image or the next Java Vector API release and asks why we didn't just use that.  
**Mitigation:** Periodically (every 6 months) re-evaluate the JVM landscape. If a JVM feature closes the gap, be willing to shrink the project's scope. The goal is faster Pinot, not more Rust.

---

## 9. Open Questions

These need resolution before Phase 1 starts in earnest:

1. **Benchmark workload selection.** Internal production trace vs TPC-H vs synthetic? Owner: needs an internal partner who can share a query trace.
2. **AVX-512 strategy.** Target it explicitly with runtime detection, or rely on AVX2 only for portability? Lean AVX-512 with runtime dispatch.
3. **Group-by hash table for high cardinality.** When group count exceeds memory, what's the spill behavior? Java path uses trim; native may need parity.
4. **Null handling.** Native kernels must support Pinot's null vector exactly. Validate semantics with the differential tester from day one.
5. **String columns in aggregation.** `MIN(strCol)` / `MAX(strCol)` are common but non-trivial in Rust. In Phase 1 scope or punt?
6. **Build system.** `rust-maven-plugin` or shell out? Lean shell-out for simplicity; switch if it bites.
7. **Where does the native engine fit in the operator tree?** Replace specific Java operator classes via a factory, or run as a parallel pipeline behind a feature flag? Lean factory replacement at `AggregationOperator` / `GroupByOperator` boundary.

---

## 10. Decision Log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-20 | Phase 1 target is aggregation + group-by, NOT filter scan | Filter scan acceleration is largely moot in tuned production where indexes cover the hot paths. Aggregation is index-independent and dominates post-filter CPU. |
| 2026-05-20 | Start with classic JNI, plan migration to FFM | Java 21 floor; FFM requires 22+. Boundary design is interop-agnostic. |
| 2026-05-20 | Native engine ships as opt-in via `auto` mode with java fallback | Mixed-version cluster safety; customer ability to disable instantly. |
| 2026-05-20 | Project gated at end of each phase by explicit numeric metrics | Avoids sunk-cost rationalization. |
| 2026-05-20 | **Integration at `AggregationFunctionFactory`, NOT plan-maker fork** | Code-read investigation showed all six aggregation contexts in Pinot (SSE V1, MSE leaf, MSE intermediate, star-tree, realtime, MV refresh) share the same `AggregationFunction` base classes via the factory. Single change point covers all engines. See `docs/native/phase-1-design.md` §2, §8, §16. |
| 2026-05-20 | HLL deferred to Phase 1.E (separable from rest of Phase 1) | Clearspring byte-exact parity is the highest-risk single deliverable. Keeping it off the critical path lets the rest of Phase 1 ship with lower risk. |
| 2026-05-20 | POC interface uses materialized Java arrays via `GetPrimitiveArrayCritical` | Simpler than direct `PinotDataBuffer` access; defers `toNativeAddress()` to Phase 1.B. Note: BlockValSet.getLongValuesSV() materialization is already on the Java hot path, so POC adds no extraction cost over baseline. |

---

## 11. Appendix: Why Not X?

**Why not GraalVM native image?**  
Doesn't give SIMD or arena allocation. Removes startup time but not steady-state CPU cost. Doesn't address tail latency from GC.

**Why not just use Java Vector API?**  
It's incubator, lags target ISAs, and JIT integration is fragile. Useful tactically (and we may already use it in places), but doesn't address hash tables, allocation discipline, or GC.

**Why not Zig / C++ / C?**  
Rust's ownership model materially reduces the unsafe surface area at the JNI boundary, where memory bugs become heap corruptions in the JVM that are nearly impossible to diagnose. The safety win is real even if performance parity exists with C++.

**Why not call CRoaring / faiss / etc. directly via JNI without writing new Rust?**  
That's a separate, complementary effort. Use existing C/C++ libraries where they exist. Rust is for the parts where we have to write the code anyway.

---

*End of document. Comments and challenges welcome — this is a draft.*
