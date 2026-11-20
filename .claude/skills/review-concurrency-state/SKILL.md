---
name: review-concurrency-state
description: Review Apache Pinot concurrency and state safety when locks, shared callbacks, lifecycle, or distributed metadata change.
domain: kb/code-review-principles.md#2-state-management--concurrency
triggers:
  - diff adds/removes synchronized / volatile / Atomic* / lock types
  - diff touches pinot-segment-local/**/upsert/** or consumer coordinator code
  - diff touches Helix state transitions, IdealState writes, or ZK node mutations
  - diff modifies shared observer/callback registration paths
  - diff changes check-then-act sequences on concurrent collections
license: Apache-2.0
---

# Skill: review-concurrency-state

Procedure: see [`kb/skills/review-concurrency-state.md`](../../../kb/skills/review-concurrency-state.md). Read it first, then follow it.
