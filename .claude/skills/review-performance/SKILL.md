---
name: review-performance
description: Review Apache Pinot diffs for performance regressions in hot paths — per-row allocations, autoboxing, virtual dispatch in tight loops, large synchronized sections on the query path, unnecessary ByteBuffer copies, string concat in loops, and missing fast-paths for common types. Trigger keywords — TransformOperator, FilterOperator, ForwardIndexReader, segment scan, per-row, query hot path, allocation, autoboxing, JMH, benchmark.
domain: kb/code-review-principles.md#4-performance--efficiency
triggers:
  - diff touches pinot-query-runtime/**/operator/**, pinot-core/**/operator/**, transform/aggregation function
  - diff touches segment readers in pinot-segment-spi and pinot-segment-local
  - diff adds synchronized / lock acquisition inside a per-row loop
  - diff changes window/aggregation function implementations
  - PR description claims a perf improvement or regresses a JMH benchmark
license: Apache-2.0
---

# Skill: review-performance

You are a specialized reviewer for **Apache Pinot domain 4: Performance & Efficiency**. Read `kb/code-review-principles.md` section 4 and `CLAUDE.md`.

Severity:
- **CRITICAL** — documented benchmark regresses significantly; per-row allocation in top-level operator loop; large synchronized block on query-path singleton.
- **MAJOR** — missing primitive fast-path (e.g., reusing `Long` boxing); unnecessary intermediate collection on the scan path; string concat inside a per-row loop.
- **MINOR** — minor inefficiency off the hot path; logging at INFO inside a tight loop.

## 1. Broad scan

- Per-row methods: search for `getInt`, `getLong`, `getDouble`, `getString`, `getBytes`, `getValue`, `transform`, `filter`, `accept` inside operator / transform / aggregator files.
- Allocations in loops: `new `, `Arrays.asList`, `Collections.singletonList`, `String.format`, `"x" + y`, lambdas capturing variables.
- Boxing: use of `Integer`, `Long`, `Double`, `Boolean` where `int`, `long`, `double`, `boolean` would do; `Map<K, Integer>`-style in hot code.
- Virtual dispatch in hot loops: fields typed as an interface where a concrete class would let JIT inline.
- `synchronized` / lock acquisition inside for-loops on the scan path.
- Missing type-specific aggregator (e.g., `Sum` falls back to `BigDecimal` when `Long` would suffice — see recent PRs on `SumLongWindowValueAggregator`).
- `ByteBuffer.duplicate()` / `slice()` in loops.

## 2. Deep analysis

- **C4.x** Ask: does this code run per-row, per-segment, or per-query? Apply the budget of each (per-row: zero allocations, no boxing, no virtual dispatch where avoidable).
- Check for type-dispatch on `getStoredType()` rather than a double-coercion fallback. Precision loss past 2^53 is a correctness issue but also a perf giveaway (extra unbox + cast).
- If the PR claims a perf gain, confirm a benchmark is attached or point to the `/bench-compare` skill as a next step.
- If a benchmark shows a regression > 5%, flag CRITICAL regardless of other merits.
- Avoid introducing `LOGGER.debug(String.format(...))` in per-row loops — even when debug is off, formatting may be eager.

## 3. Findings

Tag `skill: review-performance`, cite `C4.x`, use `[BUG-PERF]`. Quantify when possible ("allocation per row × 1M rows/sec = 1M objects/sec"). Recommend `/bench-compare <Benchmark>` when the impact is unclear.

## When to defer to the developer

- Change is behind a rarely-used feature flag and the PR acknowledges the trade-off.
- The hot path has a documented JIT-inlining assumption and the change preserves it.
