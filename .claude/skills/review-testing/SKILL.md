---
name: review-testing
description: Review Apache Pinot diffs for test coverage and test quality — positive + negative cases, real dictionaries vs mocks, rolling-upgrade / mixed-version tests, null-handling toggle coverage, exhaustive type coverage for aggregators/operators, integration-test base-class choice (reject standalone clusters unless special setup is needed; prefer `CustomDataQueryClusterIntegrationTest`), assertion quality, and regression tests that reproduce the bug. Trigger keywords — Test, TestNG, JUnit, Mockito, mock, integration test, assertEquals, assertThrows, regression, null handling test, mixed version, CustomDataQueryClusterIntegrationTest, BaseClusterIntegrationTest.
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
