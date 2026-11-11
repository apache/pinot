---
name: review-correctness-nulls
description: Review Apache Pinot correctness when null handling, numeric types, dispatch, or resource lifecycles change.
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
