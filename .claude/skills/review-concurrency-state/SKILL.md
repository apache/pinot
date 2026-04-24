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

You are a specialized reviewer for **Apache Pinot domain 2: State Management & Concurrency**. Read `kb/code-review-principles.md` section 2 and `CLAUDE.md` before analyzing.

Severity:
- **CRITICAL** — data race, atomicity violation (wipe-before-install), IdealState write without version check, visibility bug on shared mutable state.
- **MAJOR** — unnecessary lock widening, striped lock without measured contention, check-then-act race even if rare.
- **MINOR** — over-synchronization, missing `volatile` where `final` would be safer, comment omission on thread-safety contract.

## 1. Broad scan

- Added/removed `synchronized`, `volatile`, `AtomicReference`, `AtomicLong`, `ReentrantLock`, `StampedLock`, `ConcurrentHashMap`, `CopyOnWriteArrayList`.
- `get` followed by `put` / `remove` on concurrent maps (check-then-act pattern).
- Helix `IdealState` / `ExternalView` reads without version checks, or `setIdealState` without `dataAccessor.getProperty(...).getStat()`.
- `@GuardedBy` annotations added or removed.
- Registration of observers / listeners (callbacks, MetricsRegistry, segment lifecycle listeners) without clear lifetime documentation.
- Background threads: `Executors.new*`, `ScheduledExecutorService`, `Thread`. Check shutdown path (`awaitTermination` then `shutdownNow`).
- Consumer / upsert files: `PartitionConsumer`, `UpsertMetadataManager`, `*PartitionUpsertMetadataManager`.

## 2. Deep analysis

For each hit, apply the KB's concurrency principles:

- **C2.1 — Atomic transitions.** Never wipe old metadata before the new state is durably installed. Pattern: prepare-new → swap-reference → cleanup-old. Flag eager deletes.
- **C2.2 — Thread-safety conservatism.** Default to explicit synchronization. If replacing `synchronized` with `AtomicReference` or `CHM.compute`, verify the visibility story holds across all callers.
- **C2.3 — Race analysis for lock changes.** When a lock's scope is narrowed or removed, walk through interleavings with other threads that touch the same state. Flag if the walk-through isn't in the PR description.
- **C2.4 — Version-checked writes.** Shared state in ZK (IdealState, IdealStateConfig, TableConfig, Schema) must be written with optimistic locking (ZK node version); reject blind writes.
- **C2.5 — Check-then-act on atomics is still racy.** `if (!map.containsKey(k)) map.put(k, v)` is a bug — must be `putIfAbsent` / `computeIfAbsent`.
- **C2.6 — Shared observers.** When an observer is registered from multiple paths or called concurrently, the handler must be idempotent and its mutable state must be published safely.

Also check lifecycle: every `new ExecutorService` needs a clear shutdown path in `close()` / stop hook.

## 3. Findings

Emit findings in the `code-reviewer` agent format, tagging `skill: review-concurrency-state` and citing `C2.x`. Use `[BUG-RACE]` for unnamed bugs.

For each finding, include a short interleaving sketch (T1/T2 steps) where the race is non-obvious — this is high-leverage and human reviewers trust it.

## When to defer to the developer

- Code clearly documents a single-threaded invariant (e.g., called only from the Helix event thread) and the invariant is preserved.
- The change is in test-only code.
