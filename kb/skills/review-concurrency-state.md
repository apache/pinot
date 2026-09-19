# review-concurrency-state

Review **Apache Pinot domain 2: State Management & Concurrency**. Read the applicable parts of section 2 in
`kb/code-review-principles.md` and relevant repository conventions not already loaded. Reuse material already read.

Use the canonical severity definitions and Review Delivery rules in `kb/code-review-principles.md`. Assess demonstrated
impact; pattern matches are investigation triggers, not findings or automatic severity assignments.

## 1. Broad scan

- Added/removed `synchronized`, `volatile`, `AtomicReference`, `AtomicLong`, `ReentrantLock`, `StampedLock`, `ConcurrentHashMap`, `CopyOnWriteArrayList`.
- `get` followed by `put` / `remove` on concurrent maps (check-then-act pattern).
- Helix read-modify-write paths: check that writes validate the version read, directly or through the update helper.
  Ordinary `IdealState` / `ExternalView` reads do not require version checks.
- `@GuardedBy` annotations added or removed.
- Registration of observers / listeners (callbacks, MetricsRegistry, segment lifecycle listeners) without clear lifetime documentation.
- Background threads: `Executors.new*`, `ScheduledExecutorService`, `Thread`. Check shutdown path (`awaitTermination` then `shutdownNow`).
- Consumer / upsert files: `PartitionConsumer`, `UpsertMetadataManager`, `*PartitionUpsertMetadataManager`.

## 2. Deep analysis

For each hit, apply the KB's concurrency principles:

- **C2.1 — Atomic transitions.** Never wipe old metadata before the new state is durably installed. Pattern: prepare-new → swap-reference → cleanup-old. Flag eager deletes.
- **C2.2 — Thread-safety conservatism.** Establish ownership, thread confinement, publication, and lifecycle contracts
  before proposing synchronization. For lock or atomic-operation changes, verify visibility and atomicity across callers.
- **C2.3 — Race analysis for lock changes.** Walk through the key interleavings with other threads touching the same state.
  Report reachable unsafe interleavings as findings and unresolved contracts as coverage limits; missing PR prose alone is not a race finding.
- **C2.4 — Version-checked writes.** Shared ZK read-modify-write operations (IdealState, IdealStateConfig, TableConfig,
  Schema) must preserve concurrent updates through version checks or an equivalent update helper; flag unprotected writes.
- **C2.5 — Check-then-act on atomics is still racy.** For concurrent access, verify whether
  `if (!map.containsKey(k)) map.put(k, v)` requires `putIfAbsent` / `computeIfAbsent` or is protected by a wider invariant.
- **C2.6 — Shared observers.** When an observer is registered from multiple paths or called concurrently, the handler must be idempotent and its mutable state must be published safely.

Also check lifecycle: every `new ExecutorService` needs a clear shutdown path in `close()` / stop hook.

## 3. Findings

Emit findings in the `code-reviewer` agent format, tagging `skill: review-concurrency-state` and citing `C2.x`. Use `[BUG-RACE]` for unnamed bugs.

For each finding, include a short interleaving sketch (T1/T2 steps) where the race is non-obvious — this is high-leverage and human reviewers trust it.

## When to defer to the developer

- Code clearly documents a single-threaded invariant (e.g., called only from the Helix event thread) and the invariant is preserved.
- The change is in test-only code.
