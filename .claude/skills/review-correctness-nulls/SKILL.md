---
name: review-correctness-nulls
description: Review Apache Pinot diffs for correctness issues — null handling, type dispatch, numeric precision (INT/LONG/BIG_DECIMAL), exhaustive switch coverage for DataType / IndexType, resource leaks in close/destroy paths, off-by-one errors in row iteration, and silent wrong-result risks. Trigger keywords — null, Nullable, Optional, getStoredType, DataType switch, IndexType switch, close, destroy, realtime persist, precision, BigDecimal, isNullable, null vector.
domain: kb/code-review-principles.md#5-correctness--safety
triggers:
  - diff touches null-vector / null-bitmap / null-enabled code paths
  - diff adds a switch on DataType, FieldSpec.DataType, or IndexType without default throw
  - diff changes arithmetic / aggregation / window function type dispatch
  - diff touches segment destroy / close / persist paths
license: Apache-2.0
---

# Skill: review-correctness-nulls

Procedure: see [`kb/skills/review-correctness-nulls.md`](../../../kb/skills/review-correctness-nulls.md). Read it first, then follow it.
