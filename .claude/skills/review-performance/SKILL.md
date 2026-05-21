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

Procedure: see [`kb/skills/review-performance.md`](../../../kb/skills/review-performance.md). Read it first, then follow it.
