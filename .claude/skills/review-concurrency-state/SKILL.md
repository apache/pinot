---
name: review-concurrency-state
description: Review Apache Pinot diffs for concurrency, state management, visibility, atomic state transitions, lock changes, Helix IdealState updates, upsert metadata safety, consumer/stream ingestion races, and shared-observer correctness. Trigger keywords — synchronized, volatile, AtomicReference, ConcurrentHashMap, ReentrantLock, StampedLock, Helix, IdealState, ZkClient, version-checked write, upsert metadata, consumer coordinator, stream partition.
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
