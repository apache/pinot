# review-performance

Review **Apache Pinot domain 4: Performance & Efficiency**. Read the applicable parts of section 4 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- Per-row methods: search for `getInt`, `getLong`, `getDouble`, `getString`, `getBytes`, `getValue`, `transform`, `filter`, `accept` inside operator / transform / aggregator files.
- Allocations in loops: `new `, `Arrays.asList`, `Collections.singletonList`, `String.format`, `"x" + y`, lambdas capturing variables.
- Boxing: use of `Integer`, `Long`, `Double`, `Boolean` where `int`, `long`, `double`, `boolean` would do; `Map<K, Integer>`-style in hot code.
- Dispatch in hot loops: investigate polymorphic call sites when profiling or code-path evidence suggests an inlining issue;
  an interface-typed field alone does not demonstrate a regression.
- `synchronized` / lock acquisition inside for-loops on the scan path.
- Missing type-specific aggregator (e.g., `Sum` falls back to `BigDecimal` when `Long` would suffice — see recent PRs on `SumLongWindowValueAggregator`).
- `ByteBuffer.duplicate()` / `slice()` in loops.

## 2. Deep analysis

- **C4.x** Establish whether code runs per-row, per-segment, or per-query. Assess added allocation, boxing, dispatch,
  and contention against the relevant workload; do not infer runtime cost from syntax alone.
- Check for type-dispatch on `getStoredType()` rather than a double-coercion fallback. Precision loss past 2^53 is a correctness issue but also a perf giveaway (extra unbox + cast).
- If the PR claims a perf gain, confirm a benchmark is attached or point to the `/bench-compare` skill as a next step.
- Evaluate benchmark regressions using comparable workloads, environments, repeated measurements, noise, and affected
  production paths. Assign severity from demonstrated impact, not a fixed percentage alone; report missing evidence.
- Avoid introducing `LOGGER.debug(String.format(...))` in per-row loops — even when debug is off, formatting may be eager.

## 3. Findings

Tag `skill: review-performance`, cite `C4.x`, use `[BUG-PERF]`. Quantify when possible ("allocation per row × 1M rows/sec = 1M objects/sec"). Recommend `/bench-compare <Benchmark>` when the impact is unclear.

## When to defer to the developer

- Change is behind a rarely-used feature flag and the PR acknowledges the trade-off.
- The hot path has a documented JIT-inlining assumption and the change preserves it.
