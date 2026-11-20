---
name: review-testing
description: Review Apache Pinot regression coverage and test quality when behavior or tests change.
domain: kb/code-review-principles.md#6-testing-strategies
triggers:
  - diff adds or modifies any src/test/** file
  - diff adds production code without a corresponding test change
  - diff claims to fix a bug without a regression test
  - diff touches a type-dispatch or null-aware code path
license: Apache-2.0
---

# Skill: review-testing

Procedure: see [`kb/skills/review-testing.md`](../../../kb/skills/review-testing.md). Read it first, then follow it.
