# Apache Pinot Code Review Principles

Engineering principles governing Apache Pinot development, organized by severity within each domain.

Severity definitions:
- **CRITICAL**: Must fix before merge. Data loss, corruption, silent wrong results, backward incompatibility, security issues, race conditions.
- **MAJOR**: Should fix. Strong justification needed to skip. Performance regressions, design violations, missing tests, wrong abstractions.
- **MINOR**: Improves quality. Acceptable to defer. Naming, style, idioms, process suggestions.

Priority order: Production Safety > Backward Compatibility > Correctness > State Management > Performance > Architecture > Testing > Naming > Process

---

## 1. Configuration & Backward Compatibility

### CRITICAL

**C1.1 — Config key renames must maintain backward compatibility**
Old keys must continue working. Follow 4-step protocol: (1) honor old but mark deprecated, (2) introduce new key, (3) update docs, (4) system accepts old name for API calls.
- Trigger: Any PR that renames, removes, or changes a config key

```java
// BAD: Old key silently stops working — breaks existing deployments
- public static final String AUTH_MODE = "pinot.broker.auth.mode";
+ public static final String AUTH_MODE = "pinot.broker.authentication.mode";
```
```java
// GOOD: Old key honored with deprecation
public static final String AUTH_MODE = "pinot.broker.authentication.mode";
@Deprecated public static final String AUTH_MODE_LEGACY = "pinot.broker.auth.mode";
// Resolution: check new key first, fall back to legacy
```

**C1.2 — Schema type names are permanent API contracts**
Once introduced, type names cannot be renamed. Validate naming against industry conventions before first release.
- Trigger: Any PR introducing new schema types, data types, or enum values in public API

```java
// BAD: Introduced as "INTERMEDIATE" — later realized "VARIANT" is industry standard, but can't rename
public enum DataType { INT, LONG, FLOAT, DOUBLE, STRING, INTERMEDIATE }
```
```java
// GOOD: Validate against SQL/Parquet/Arrow conventions before committing the name
public enum DataType { INT, LONG, FLOAT, DOUBLE, STRING, VARIANT }
```

**C1.3 — Do not widen SPI method signatures unnecessarily**
Even List→Collection can break backward compatibility tests. SPI is external contract.
- Trigger: Any PR modifying method signatures in `pinot-spi` module

```java
// BAD: Widening List→Collection in SPI breaks binary compatibility for plugins
- List<GenericRow> transform(List<GenericRow> rows);
+ Collection<GenericRow> transform(Collection<GenericRow> rows);
```
```java
// GOOD: Keep SPI signatures stable; add new method if needed
List<GenericRow> transform(List<GenericRow> rows);
// If Collection variant needed, add overload without removing original
```

**C1.4 — Reverting previous PRs requires explicit justification**
Explain the problem, reference the original PR, maintain backward compatibility.
- Trigger: Any revert PR or PR that undoes prior work

```java
// BAD: Silent revert with no context — reviewers can't assess impact
// "Revert segment UUID changes" (no mention of WHY or which PR)
```
```java
// GOOD: "Revert #16909 (segment UUID format) — caused segment name collisions
// during rolling upgrade when old and new controllers coexist.
// Maintains backward compat by keeping old UUID format until upgrade completes."
```

**C1.5 — Config validation must account for multi-level override semantics**
Validate the effective state after table, instance, and default overrides, not just the raw config value. Use Enablement enum (ENABLE/DEFAULT/DISABLE).
- Trigger: Any PR adding config validation logic

```java
// BAD: Validates raw table config — misses instance-level override that disables the feature
if (tableConfig.getUpsertConfig().isEnabled()) { /* enable upsert */ }
```
```java
// GOOD: Resolve effective state through override chain
Enablement effective = resolveEnablement(tableConfig, instanceConfig, defaultConfig);
if (effective == Enablement.ENABLE) { /* enable upsert */ }
```

### MAJOR

**C1.6 — New features default to OFF**
Behavior-changing features default to disabled. Boolean flags should default to `false`. Name as `disableXXX` so default=false means enabled.
- Trigger: Any PR adding a new feature flag or boolean config

**C1.7 — Config namespace consistency**
Follow existing patterns (`pinot.query.sse.*`, `pinot.broker.*`). Broker configs use dot-separated lowercase, not camelCase. Constant names must match string values.
- Trigger: Any PR introducing new config keys

**C1.8 — Feature rollout: instance-level configs preferred**
Instance-level (maintainer-controlled) over table-level (user-controlled) for gradual rollout.
- Trigger: Any PR adding configs for gradual feature rollout

**C1.9 — Understand historical design intent of feature flags before changing behavior**
Original scope of a flag must be understood before extending to new behavior.
- Trigger: Any PR extending the behavior of an existing feature flag

**C1.10 — Backward-compatibility mechanisms must have a sunset plan**
Shims should be scrutinized for ongoing operational cost and have a clear removal date.
- Trigger: Any PR adding backward-compat shims or fallback code paths

**C1.11 — Default value changes require production testing confidence**
New features should go through a testing/validation period before changing defaults. Consider full blast radius (all table types).
- Trigger: Any PR changing default values for existing configs

**C1.12 — Do not add support for deprecated config formats for new features**
Use the current JSON format. Avoid introducing new constants for deprecated code paths.
- Trigger: Any PR adding config support for new features

### MINOR

**C1.13 — Override-with-fallback pattern**
Task-specific config overrides global defaults, with lazy resolution.
- Trigger: Any PR with hierarchical config resolution

**C1.14 — Do not deprecate/add backward-compat for unreleased features**
If a key was never released, just remove it.
- Trigger: Any PR adding backward-compat for code not yet in a release

**C1.15 — Don't misuse @Deprecated**
Only use when a replacement exists today. Use TODO comment if replacement doesn't exist yet.
- Trigger: Any PR using @Deprecated annotation

**C1.16 — Use intuitive sentinel values (0, -1) for "unlimited"**
Not Integer.MAX_VALUE. Treat non-positive as unlimited in API defaults.
- Trigger: Any PR defining "unlimited" or "no limit" semantics

---

## 2. State Management & Concurrency

### CRITICAL

**C2.1 — Atomic state transitions**
Never wipe metadata eagerly. Bundle related state changes. Distributed state mutations must succeed completely or revert completely.
- Trigger: Any PR modifying ZooKeeper, IdealState, or distributed metadata

```java
// BAD: Two separate ZK writes — if second fails, IS and segment metadata diverge
idealState.setPartitionState(segmentName, instance, "ONLINE");
helixAdmin.setResourceIdealState(clusterName, tableName, idealState);
segmentZKMetadata.setStatus(Status.DONE);
ZKMetadataProvider.setSegmentZKMetadata(propertyStore, segmentZKMetadata); // fails → inconsistent
```
```java
// GOOD: Version-checked single IS write; metadata write in same transaction or with rollback
int expectedVersion = idealState.getRecord().getVersion();
idealState.setPartitionState(segmentName, instance, "ONLINE");
if (!helixAdmin.setResourceIdealState(clusterName, tableName, idealState, expectedVersion)) {
  throw new RuntimeException("IS version conflict — retry with fresh state");
}
```

**C2.2 — Thread safety conservatism**
When uncertain about framework threading guarantees (e.g., gRPC StreamObserver), default to explicit synchronization (`volatile`, `synchronized`). Read upstream library source code to verify actual contracts.
- Trigger: Any PR modifying shared mutable state or concurrent code

```java
// BAD: Assumes gRPC StreamObserver is thread-safe (it's NOT for onNext)
class MyObserver implements StreamObserver<DataBlock> {
  private List<DataBlock> blocks = new ArrayList<>(); // unsynchronized
  public void onNext(DataBlock block) { blocks.add(block); } // called from gRPC threads
}
```
```java
// GOOD: Explicit synchronization until threading contract is verified
class MyObserver implements StreamObserver<DataBlock> {
  private final List<DataBlock> blocks = Collections.synchronizedList(new ArrayList<>());
  public void onNext(DataBlock block) { blocks.add(block); }
}
```

**C2.3 — Exhaustive race condition analysis for lock changes**
When moving from coarse-grained to fine-grained locking, enumerate ALL possible interleavings. Missing IS updates = MAJOR correctness issue.
- Trigger: Any PR changing lock granularity or locking strategy

```java
// BAD: Replaced global lock with per-table lock without analyzing interleavings
- synchronized (this) { updateRoutingTable(table); updateIdealState(table); }
+ synchronized (_tableLocks.get(table)) { updateRoutingTable(table); }
+ // IdealState update now outside lock — another thread can see stale routing
```
```java
// GOOD: Enumerate interleavings; keep coupled operations under same lock
synchronized (_tableLocks.get(table)) {
  updateRoutingTable(table);
  updateIdealState(table); // must stay atomic with routing update
}
```

**C2.4 — Version-checked (optimistic locking) writes for shared state**
IdealState and similar shared state must use version-aware writes, not naive retries without consistency guarantees.
- Trigger: Any PR writing to ZooKeeper IdealState or shared distributed state

```java
// BAD: Blind write — another controller may have modified IS concurrently
IdealState is = getIdealState(tableName);
is.setPartitionState(segment, "ONLINE");
setIdealState(tableName, is); // overwrites concurrent changes silently
```
```java
// GOOD: Version-checked write — fails on conflict, caller retries with fresh state
IdealState is = getIdealState(tableName);
int version = is.getRecord().getVersion();
is.setPartitionState(segment, "ONLINE");
if (!setIdealState(tableName, is, version)) { /* retry with fresh IS */ }
```

**C2.5 — Check-then-act on atomics without synchronization still has race conditions**
AtomicReference.get() followed by a separate operation is NOT atomic. Prefer `synchronized` over CAS when guarded operations have side effects.
- Trigger: Any PR using AtomicReference/AtomicBoolean for guarding resource creation

```java
// BAD: Check-then-act — two threads can both see null and create duplicate providers
if (_metadataProvider.get() == null) {
  _metadataProvider.set(new StreamMetadataProvider(config)); // side effect: opens connection
}
```
```java
// GOOD: synchronized when creation has side effects (network connections, resource allocation)
synchronized (this) {
  if (_metadataProvider.get() == null) {
    _metadataProvider.set(new StreamMetadataProvider(config));
  }
}
```

**C2.6 — Thread safety in shared observers must be proven correct**
Mutable state in shared gRPC observers is a MAJOR concern.
- Trigger: Any PR modifying gRPC StreamObserver implementations or shared callback handlers

```java
// BAD: Mutable counter in observer shared across gRPC threads — no synchronization
class BlockObserver implements StreamObserver<DataBlock> {
  private int blockCount = 0; // accessed from multiple gRPC threads
  public void onNext(DataBlock b) { blockCount++; processBlock(b); }
}
```
```java
// GOOD: Use atomic or synchronize; document the threading model
class BlockObserver implements StreamObserver<DataBlock> {
  // gRPC calls onNext from transport threads — must be thread-safe
  private final AtomicInteger blockCount = new AtomicInteger(0);
  public void onNext(DataBlock b) { blockCount.incrementAndGet(); processBlock(b); }
}
```

### MAJOR

**C2.7 — Lifecycle-aware state tracking**
Maps tracking state must handle ALL lifecycle events (add/replace/remove/reset). Incremental cleanup over bulk clear.
- Trigger: Any PR maintaining segment/partition/instance tracking maps

**C2.8 — State transitions in finally blocks**
Ensure state flags (terminated, completed) are set regardless of exceptions.
- Trigger: Any PR with state transition flags in try blocks

**C2.9 — Reuse thread pools**
Don't create/destroy executor services per invocation. Default pool sizes use `Runtime.getRuntime().availableProcessors()`.
- Trigger: Any PR creating new ExecutorService or thread pool

**C2.10 — Condition variables must be properly signaled**
When draining/modifying bounded queues under lock, signal all relevant conditions to avoid deadlocks.
- Trigger: Any PR modifying lock/condition logic in bounded queues or mailboxes

**C2.11 — Document the threading model before adding concurrency controls**
Establish which methods are accessed concurrently before introducing atomics/CAS.
- Trigger: Any PR adding new synchronization primitives

**C2.12 — Use concurrent collection return values to detect races**
Use `Set.add()` return value for CAS-style duplicate detection. Don't assume single-threaded execution.
- Trigger: Any PR adding elements to shared sets/maps in concurrent contexts

**C2.13 — Place state-tracking operations outside try-catch**
Ensure cleanup only runs when the operation actually started. Align cleanup lifecycle with operation lifecycle.
- Trigger: Any PR with try-finally patterns around tracked operations

### MINOR

**C2.14 — Single initialization of shared resources**
SSL context, registries initialized exactly once. Duplicate init is an anti-pattern.
- Trigger: Any PR initializing shared singleton resources

**C2.15 — Registries immutable after startup**
Do not use ConcurrentHashMap if all registration is at startup.
- Trigger: Any PR creating registry-style data structures

**C2.16 — Use correct concurrent data structures**
Only fields accessed outside lock protection need concurrent collections.
- Trigger: Any PR choosing between synchronized blocks and concurrent collections

---

## 3. Code Architecture & Module Design

### CRITICAL

**C3.1 — Fix at the right layer**
Don't patch symptoms at call sites. Fix the root cause where the operation has full context. Prefer systemic fixes over point fixes when same bug pattern exists across multiple implementations.
- Trigger: Any bug fix PR — check whether fix is at root cause or patching downstream

```java
// BAD: Null guard at every caller — treats symptom, not cause
// In BrokerQueryHandler:
if (routingTable.getEntry(tableName) != null) { ... }
// In BrokerReduceService:
if (routingTable.getEntry(tableName) != null) { ... } // same guard repeated
```
```java
// GOOD: Fix in RoutingTable itself where the null originates
// RoutingTable.getEntry() should never return null for a known table — fix the registration path
```

**C3.2 — Never read ZooKeeper on the query path**
Introduces latency and ZK availability dependency for query serving.
- Trigger: Any PR adding ZK reads in broker query path or server query processing

```java
// BAD: ZK read on every query — adds latency, creates ZK availability dependency
public BrokerResponse handleQuery(JsonNode query) {
  TableConfig config = ZKMetadataProvider.getTableConfig(propertyStore, tableName); // ZK read!
  return routeAndScatter(query, config);
}
```
```java
// GOOD: Use cached config (updated via ZK watcher), never read ZK synchronously on query path
public BrokerResponse handleQuery(JsonNode query) {
  TableConfig config = _tableCache.getTableConfig(tableName); // local cache
  return routeAndScatter(query, config);
}
```

**C3.3 — Centralize ZooKeeper writes in ZKOperator pattern**
Other classes pass metadata objects without writing directly. Separate in-memory mutation from ZK persistence for batching and version-aware writes.
- Trigger: Any PR writing to ZK outside the designated ZKOperator class

```java
// BAD: ZK write scattered in a helper class — bypasses version control and batching
class SegmentStatusUpdater {
  void markComplete(String segment) {
    SegmentZKMetadata meta = ZKMetadataProvider.getSegmentMetadata(store, segment);
    meta.setStatus(Status.DONE);
    ZKMetadataProvider.setSegmentMetadata(store, meta); // direct ZK write outside ZKOperator
  }
}
```
```java
// GOOD: Mutate in-memory, let ZKOperator handle persistence
class SegmentStatusUpdater {
  SegmentZKMetadata markComplete(SegmentZKMetadata meta) {
    meta.setStatus(Status.DONE);
    return meta; // caller (ZKOperator) handles the write with version checking
  }
}
```

**C3.4 — New feature code paths must be completely separate from existing ones**
Ensure backward compatibility by not modifying shared paths. Scope engine-specific changes to the relevant engine.
- Trigger: Any PR adding a new feature that touches existing query/ingestion paths

```java
// BAD: MSE-specific case-insensitivity injected into shared TableCache — affects SSE too
class TableCache {
  TableConfig getTableConfig(String name) {
    return _cache.get(name.toLowerCase()); // breaks SSE which is case-sensitive
  }
}
```
```java
// GOOD: Resolve to canonical name via MSE-specific path before reaching shared code
class MSEQueryPlanner {
  TableConfig resolve(String name) {
    String canonical = _tableCache.resolveCanonicalName(name); // MSE-only resolution
    return _tableCache.getTableConfig(canonical);
  }
}
```

**C3.5 — Never hack metadata into unrelated data structures**
Extend serialization formats backward-compatibly using length-based detection. Old servers must handle new format gracefully.
- Trigger: Any PR adding new fields to wire protocol or inter-node communication

```java
// BAD: Smuggling stats metadata in the exception map — old servers misinterpret as errors
dataBlock.getExceptions().put(STATS_KEY, serializedStats); // not an exception!
```
```java
// GOOD: Extend the serialization format with length-based detection
// New format: [existingFields][statsLength][statsBytes]
// Old servers: read up to existingFields, ignore trailing bytes (length-based detection)
buffer.putInt(statsBytes.length);
buffer.put(statsBytes);
```

### MAJOR

**C3.6 — Use abstraction layers for pluggability**
Prefer managers/interfaces over concrete implementations. OSS code must support enterprise extensions.
- Trigger: Any PR adding new subsystem or replacing existing implementation

**C3.7 — Centralize cross-cutting utilities**
All query option parsing in `QueryOptionsUtils`. Mechanism changes in central utilities like `HelixHelper`. Extract shared SSE/MSE logic into common utilities.
- Trigger: Any PR with utility logic duplicated across call sites

**C3.8 — Module placement**
Shared context classes go in lowest feasible module. `pinot-spi` is for interfaces only, concrete implementations in runtime modules.
- Trigger: Any PR adding new classes — check module placement

**C3.9 — Caller-side validation over internal flags**
Use existing sentinel/singleton implementations (`EmptyDocIdSet`, `MatchAllDocIdSet`) instead of adding boolean flags. Let the type system encode state.
- Trigger: Any PR adding boolean flags to control branch behavior

**C3.10 — Base classes handle common logic**
Subclasses should not re-implement what the parent already provides.
- Trigger: Any PR where subclass duplicates parent class logic

**C3.11 — Encapsulate related state in owning class**
Related state belongs inside the owning entity, not in a separate parallel map.
- Trigger: Any PR with parallel maps tracking related state for the same entity

**C3.12 — Pass pre-fetched data objects to methods instead of letting them fetch from ZK**
Reduces ZK load, avoids races with unpersisted data. Prefer richer objects over primitive identifiers.
- Trigger: Any PR where methods receive IDs and then fetch full objects from ZK

**C3.13 — Separate data structures for different consumers**
When two consumers need overlapping but distinct data, maintain separate POJOs. "Easier to maintain" trumps "avoid duplication" when duplication is in data shape, not logic.
- Trigger: Any PR creating shared data objects consumed by different subsystems

**C3.14 — Audit all management paths when introducing new entity types**
Enforce bidirectional name collision checks between entity types.
- Trigger: Any PR adding a new entity type to an existing management framework

**C3.15 — Inject instance-level dependencies through init()**
Not per-method arguments. Keep method signatures clean.
- Trigger: Any PR passing the same dependency to multiple methods on the same object

**C3.16 — Class names must reflect actual responsibility**
Data holders should not be named "Manager". Method names returning config objects should include "Config" suffix.
- Trigger: Any PR introducing new classes or renaming existing ones

### MINOR

**C3.17 — ForwardIndexConfig pattern is canonical**
Static variable + setter within the class for extension points. No global singletons or registries.
- Trigger: Any PR adding extension points for index types

**C3.18 — Prefer generic abstractions over type-specific logic**
Introduce interfaces that all types implement rather than switch/if chains.
- Trigger: Any PR with switch/if chains over data types

**C3.19 — User-friendliness over internal elegance**
Extend existing APIs to handle new cases rather than creating parallel API surfaces.
- Trigger: Any PR creating new APIs parallel to existing ones

**C3.20 — Normalize data at entry point**
Restructure input to match existing APIs rather than adding branching across multiple classes.
- Trigger: Any PR adding if/else branches to handle input format variations

**C3.21 — Single-responsibility class design**
Split by column type/data type rather than mixing all types in one monolithic class.
- Trigger: Any PR with growing monolithic classes

**C3.22 — Don't rename existing stable classes**
Add new classes alongside. Renaming causes unnecessary churn.
- Trigger: Any PR renaming established public classes

**C3.23 — Eliminate confusing state distinctions**
Consolidate states when they serve no different purpose. Remove redundant flags.
- Trigger: Any PR with multiple states that have identical handling

**C3.24 — New SPI interfaces must be validated against primary use case**
Verify the interface works with the primary consumer before committing the design.
- Trigger: Any PR adding new SPI interfaces

---

## 4. Performance & Efficiency

### CRITICAL

**C4.1 — Never use toString() for comparison in sorted data structures**
Implement `Comparable` natively for proper key comparison. Optimize insert-then-trim by pre-checking capacity bounds.
- Trigger: Any PR with custom comparators or sorted collection keys

```java
// BAD: toString() comparison — wrong ordering for numbers, allocates on every compare
TreeMap<Object, Row> sorted = new TreeMap<>(Comparator.comparing(Object::toString));
sorted.put(longKey, row); // 100L.toString() = "100" < "99" lexicographically
```
```java
// GOOD: Type-aware Comparable implementation
TreeMap<Object, Row> sorted = new TreeMap<>(TypeAwareComparator.INSTANCE);
// TypeAwareComparator handles Long/Double/String natively without toString()
```

**C4.2 — Avoid functional/stream APIs in per-block hot paths**
Prefer imperative loops to reduce overhead at data block level. Separate feature-gated logic at higher levels.
- Trigger: Any PR using streams/lambdas in data block processing or per-record paths

```java
// BAD: Stream API in per-block path — iterator/lambda overhead on every block
List<DataBlock> merged = blocks.stream()
    .filter(b -> b.getNumRows() > 0)
    .map(this::deserialize)
    .collect(Collectors.toList()); // allocates iterator, lambda, collector per block
```
```java
// GOOD: Imperative loop — minimal overhead
List<DataBlock> merged = new ArrayList<>(blocks.size());
for (DataBlock b : blocks) {
  if (b.getNumRows() > 0) { merged.add(deserialize(b)); }
}
```

### MAJOR

**C4.3 — Minimize allocations in hot paths**
No intermediate arrays, no String.format(), no String.split(), no Optional, no eager .toString() in per-record code. Pre-size data structures to known cardinality. Use lower-overhead collection factories. Consider lazy iteration over eager materialization.
- Trigger: Any PR modifying per-record processing, query execution, or data ingestion hot paths

**C4.4 — Cache repeated expressions**
Cache method return values into local variables (even simple getters). Cache immutable computed values as class constants. Cache expensive system APIs (MXBeans) in execution context.
- Trigger: Any PR with repeated method calls in loops or frequently-called methods

**C4.5 — Map iteration efficiency**
Use `entrySet()` over `keySet()+get()`. Use `put()` return values. Use `computeIfAbsent()`.
- Trigger: Any PR iterating over maps

**C4.6 — Prefer additive over subtractive approaches**
Only produce needed data rather than producing everything then removing. Whitelist > blacklist+remove.
- Trigger: Any PR with filter-then-remove patterns on collections

**C4.7 — API contracts over defensive copies**
Establish "do not modify input" contracts instead of cloning. Avoid unnecessary defensive copies in hot paths.
- Trigger: Any PR adding defensive copies of collections or arrays

**C4.8 — Only propagate explicit overrides**
Don't inject default config values on hot path. Only pass properties relevant to target subsystem.
- Trigger: Any PR passing config/properties through hot path methods

**C4.9 — Minimize work inside lambdas/closures**
Read data outside lambda when possible. Minimize captured state. Store only needed fields in hot loops.
- Trigger: Any PR with lambdas in frequently-executed code paths

**C4.10 — Guard expensive operations with emptiness checks**
Don't deep-copy or iterate when input is empty.
- Trigger: Any PR with operations on potentially-empty collections

**C4.11 — Cardinality estimation must use probabilistic sketches**
UltraLogLog preferred over HyperLogLogPlus. Never use total doc count as proxy.
- Trigger: Any PR estimating cardinality or distinct counts

**C4.12 — Parse configuration once at service initialization**
Not per-instance. Resolve config in constructor and pass as pre-resolved values.
- Trigger: Any PR reading config values in per-request or per-instance code

**C4.13 — Short-circuit disabled code paths early**
When a feature is disabled, skip the expensive calculation entirely.
- Trigger: Any PR with feature-gated logic that doesn't short-circuit

**C4.14 — Avoid unnecessary expensive index rebuilds**
Only rebuild when functional parameters change, not when metadata/version/default config changes.
- Trigger: Any PR modifying index rebuild triggers or star-tree rebuild conditions

**C4.15 — Distinguish on-disk size from heap size**
UTF-8 encoding length is not Java heap footprint. Memory estimation must account for all major contributors.
- Trigger: Any PR implementing memory/size limits or estimations

**C4.16 — Optimize the happy path**
Avoid adding extra network round-trips to successful queries. Error/cancel paths can tolerate extra overhead.
- Trigger: Any PR adding overhead to the query success path

**C4.17 — Avoid garbage creation in hot paths**
Only construct new strings when data actually changed. Check StringBuilder length before constructing.
- Trigger: Any PR with string manipulation in frequently-called code

**C4.18 — Prefer primitive arrays over boxed arrays in hot paths**
Using `Integer[]`, `Double[]`, `Long[]`, etc. introduces boxing/unboxing overhead and additional heap pressure. Use `int[]`, `double[]`, `long[]` in aggregation, scanning, and vectorized execution paths to avoid unboxing cost, reduce GC pressure, and improve cache locality.
- Trigger: Any PR using boxed arrays in query execution, aggregation, or data ingestion hot paths

```java
// BAD: Boxed array — auto-boxing/unboxing on every iteration, extra heap allocation per element
Double[] aggregates = new Double[numGroups];
for (int i = 0; i < numDocs; i++) {
  aggregates[groupIds[i]] += values[i]; // unbox, add, box on every iteration
}
```
```java
// GOOD: Primitive array — no boxing overhead, better cache locality
double[] aggregates = new double[numGroups];
for (int i = 0; i < numDocs; i++) {
  aggregates[groupIds[i]] += values[i]; // pure primitive operations
}
```

### MINOR

**C4.19 — Use simplest collection type**
List over SortedSet for small fixed collections. Prefer Java std lib over Guava.
- Trigger: Any PR choosing collection types

**C4.20 — Return null for "no match" instead of returning input unchanged**
Avoids unnecessary comparison in caller.
- Trigger: Any PR with transform methods that may no-op

**C4.21 — Prefer deadline-based cancellation over simple interruptibility**
Deadlines compose better in distributed systems. Propagate caller deadlines.
- Trigger: Any PR adding timeout/cancellation logic

**C4.22 — Use mathematical invariants as cheap state indicators**
Prefer derived checks over explicit flags updated per document.
- Trigger: Any PR tracking min/max or aggregation state

---

## 5. Correctness & Safety

### CRITICAL

**C5.1 — Fail explicitly, never silently degrade**
Throw exceptions for unexpected states. Log impossible conditions at ERROR level. Dead code paths should throw exceptions rather than silently no-op. Propagate exceptions rather than silent catch-and-log.
- Trigger: Any PR with empty catch blocks, silent returns for unexpected states, or catch-and-log patterns

```java
// BAD: Silent catch — segment load failure is swallowed, query returns partial results
try { loadSegment(segmentName); }
catch (Exception e) { LOGGER.warn("Failed to load segment", e); } // silent degradation
```
```java
// GOOD: Propagate — caller decides how to handle
try { loadSegment(segmentName); }
catch (Exception e) { throw new RuntimeException("Segment load failed: " + segmentName, e); }
```

**C5.2 — Fix root causes, not symptoms**
Don't patch downstream to tolerate upstream contract violations. Trace data flow to understand where unexpected values originate.
- Trigger: Any bug fix PR — verify the fix addresses root cause

```java
// BAD: Null guard in dictionary reader — masks the real bug (null inserted during ingestion)
public String getString(int dictId) {
  String val = _dictionary[dictId];
  return val != null ? val : ""; // hides the null that shouldn't be there
}
```
```java
// GOOD: Fix at ingestion — prevent null from entering the dictionary in the first place
// In RecordTransformer: validate non-null before dictionary insertion
Preconditions.checkNotNull(value, "Null value for column: " + column);
```

**C5.3 — Flags must be managed across ALL lifecycle paths**
If set during commit, must be cleared during upload/replace.
- Trigger: Any PR adding boolean flags or state markers set in specific code paths

```java
// BAD: Flag set on commit but never cleared on segment replace — stale flag persists
void commitSegment() { _dataCrcChecked = true; }
void replaceSegment() { loadNewSegment(); /* _dataCrcChecked still true from old segment! */ }
```
```java
// GOOD: Clear flag in ALL lifecycle transitions
void replaceSegment() { _dataCrcChecked = false; loadNewSegment(); }
void resetSegment() { _dataCrcChecked = false; }
```

**C5.4 — Sort operations must use type-appropriate comparisons**
Comparing LONGs as doubles causes precision loss. Type coercion in sort is a correctness bug.
- Trigger: Any PR with sort/compare operations across numeric types

```java
// BAD: LONG compared as double — precision loss for values > 2^53
double a = ((Number) val1).doubleValue();
double b = ((Number) val2).doubleValue();
return Double.compare(a, b); // 9007199254740993L and 9007199254740992L compare as equal
```
```java
// GOOD: Compare using the actual stored type
if (dataType == DataType.LONG) {
  return Long.compare(((Number) val1).longValue(), ((Number) val2).longValue());
}
```

**C5.5 — Never use null to signal errors**
Null provides no stack trace and is extremely hard to debug. Prefer standard JDK exceptions.
- Trigger: Any PR returning null from methods that can fail

```java
// BAD: Null return on failure — caller gets NPE with no context
public SegmentMetadata buildSegment(TableConfig config) {
  if (!validate(config)) { return null; } // caller: metadata.getSize() → NPE, no stack trace
}
```
```java
// GOOD: Throw with context — stack trace points to the failure
public SegmentMetadata buildSegment(TableConfig config) {
  if (!validate(config)) {
    throw new IllegalStateException("Invalid config for table: " + config.getTableName());
  }
}
```

**C5.6 — Never use string interpolation to build query predicates**
Construct typed Predicate and FilterContext objects to avoid injection and escaping bugs.
- Trigger: Any PR building query filters or predicates from string concatenation

```java
// BAD: String interpolation — breaks on values containing single quotes, injection risk
String filter = "column = '" + userValue + "'"; // userValue = "O'Brien" → syntax error
FilterContext ctx = RequestContextUtils.getFilter(filter);
```
```java
// GOOD: Construct typed Predicate directly
Predicate predicate = new EqPredicate("column", userValue); // handles escaping internally
FilterContext ctx = new FilterContext(FilterContext.Type.PREDICATE, predicate);
```

**C5.7 — Changing null semantics can break existing null-check patterns**
Null-to-non-null changes in API responses must consider all downstream consumers.
- Trigger: Any PR changing the nullability contract of a method return value

```java
// BAD: Changed return from nullable to empty list — breaks callers using null as "not found"
// Before: return null; // meant "segment not in this server"
// After:  return Collections.emptyList(); // callers checking == null now skip real "not found"
```
```java
// GOOD: Audit all callers before changing nullability contract
// If changing from null→non-null, grep for all `== null` / `!= null` checks on this method
// Update each caller's semantics or keep nullable contract
```

**C5.8 — Do not weaken Preconditions/safety checks without understanding invariants they protect**
When semantics are overloaded, introduce new properly-named abstraction rather than removing validation.
- Trigger: Any PR removing or relaxing Preconditions.check* calls

```java
// BAD: Removed Precondition because new use case doesn't satisfy it — but original use case needed it
// SingletonExchange was designed for exactly 1 input; removing check allows misuse
- Preconditions.checkState(_inputs.size() == 1, "Singleton must have exactly 1 input");
+ // Removed: LogicalTableExchange can have multiple inputs
```
```java
// GOOD: Create a new properly-named abstraction for the different semantic
class LocalExchange extends AbstractExchange { /* no singleton constraint */ }
// Keep SingletonExchange with its Precondition intact
```

**C5.9 — Always apply % numPartitions to ensure partition IDs are in bounds**
Consolidate partition ID logic into a single utility class with clear priority order.
- Trigger: Any PR computing or using partition IDs

```java
// BAD: Raw hash used as partition ID — can exceed numPartitions or be negative
int partitionId = key.hashCode(); // hashCode can be negative, or > numPartitions
partitionToSegments.get(partitionId); // ArrayIndexOutOfBoundsException or null
```
```java
// GOOD: Always mod by numPartitions; handle negative hash
int partitionId = Math.abs(key.hashCode() % numPartitions);
// Better: centralize in PartitionIdUtils with priority: explicit column → segment name → hash
```

### MAJOR

**C5.10 — Null handling architecture**
Utility classes stay null-unaware (push null handling to callers). Null-aware functions always handle nulls regardless of global flag. Guard null-handling code with feature flag checks.
- Trigger: Any PR modifying null handling in query execution or data processing

**C5.11 — Do not trust string column segment metadata**
Historical bugs mean metadata can be permanently wrong. Dictionary-based optimizations are safer. Some bugs require full segment re-creation.
- Trigger: Any PR using MINSTRING/MAXSTRING from segment metadata for optimization

**C5.12 — Centralize validation in Builder.build()**
Consumers trust builder invariants, do not re-check.
- Trigger: Any PR adding validation scattered across consumer code

**C5.13 — Understand blast radius**
Helix state transitions are shared paths. Changes affect all segment loading scenarios. ZK operations can have ambiguous outcomes.
- Trigger: Any PR modifying Helix state transition handlers or ZK write paths

**C5.14 — Use stored types (getStoredType()) not logical types for storage-layer concerns**
JSON is stored as STRING. Storage code must use storage types.
- Trigger: Any PR with type checks in storage/index layer

**C5.15 — Three-valued SQL logic**
SSE filters keep true-only records. IS_TRUE is redundant in filter context. UDF wrappers block dictionary optimizations.
- Trigger: Any PR modifying SQL filter logic or boolean expression handling

**C5.16 — Behavior consistency**
New implementations must produce identical results to the reference implementation. Feature parity mandatory across all data types.
- Trigger: Any PR reimplementing or adding an alternative implementation of existing logic

**C5.17 — Function return types must be deterministic per SQL standards**
No lazy runtime type resolution. Return types propagate throughout pipeline.
- Trigger: Any PR adding or modifying SQL function type resolution

**C5.18 — Sibling registries must stay in sync**
FunctionRegistry and TransformFunctionFactory must use identical canonicalization logic.
- Trigger: Any PR modifying function registration or canonicalization

**C5.19 — Recovery logic must itself be defensive**
If recovery accesses external systems, wrap in try-catch since original failure may still apply.
- Trigger: Any PR adding recovery/fallback logic

**C5.20 — Distinguish close (clean termination) from cancel (abort)**
These have different protocol semantics. Using onError() on close can discard valid data.
- Trigger: Any PR implementing close/shutdown logic in streaming or gRPC contexts

**C5.21 — Do not add redundant null/guard checks**
Trust class invariants enforced by constructors/setters. Use `assert` as documentation for invariant code paths.
- Trigger: Any PR adding null checks where constructor/builder guarantees non-null

**C5.22 — Caches MUST have explicit invalidation strategies**
Segment replacement must invalidate cached results. Evaluate hash collision risk. Estimate memory at production scale.
- Trigger: Any PR introducing in-memory caches

**C5.23 — Empty responses must have correct schema and data types**
Broker must produce correctly-typed empty responses matching what a server would return.
- Trigger: Any PR handling all-segments-pruned or empty result scenarios

**C5.24 — Cached config and schema must always be consistent**
Apply identical transformations in all code paths that cache these objects.
- Trigger: Any PR caching TableConfig or Schema objects

**C5.25 — Replace hard assertions with graceful handling for expected runtime invalid data**
Gate behind opt-in flag (OFF by default) when skipping index operations.
- Trigger: Any PR adding assertions in data processing paths that may encounter invalid data

### MINOR

**C5.26 — InterruptedException: don't restore interrupt flag when exception is already converted**
Preserving root cause takes priority.
- Trigger: Any PR handling InterruptedException

**C5.27 — Understand null semantics before adding guards**
Null validDocIds means "no valid docs" (skip segment), not "bitmap not yet initialized".
- Trigger: Any PR adding null guards around validDocIds or similar domain objects

**C5.28 — Refactoring must guarantee zero behavior change**
State this explicitly. When renaming types, update all corresponding variable names.
- Trigger: Any refactoring PR

---

## 6. Testing Strategies

### CRITICAL

**C6.1 — CI must be green before merge**
Backward compatibility tests are non-negotiable. Fix linter failures even when PR is approved.
- Trigger: Any PR with failing CI checks

```java
// BAD: "CI failure is unrelated to my changes" — merge anyway
// japicmp backward-compat test fails → merge → breaks downstream plugin builds
```
```java
// GOOD: Fix the compat issue or update the baseline with justification
// If genuinely unrelated: rebase on latest master, verify CI green, then merge
```

**C6.2 — Tests must not be more resilient than production code**
Don't add retries/sleeps that production doesn't have. Fix root causes of flakiness.
- Trigger: Any PR adding retries, Thread.sleep(), or try-catch in test code

```java
// BAD: Test retry hides a real race condition that production will also hit
@Test void testRebalance() {
  for (int retry = 0; retry < 3; retry++) {
    try { triggerRebalance(); verifyState(); return; }
    catch (AssertionError e) { Thread.sleep(1000); } // masks the bug
  }
}
```
```java
// GOOD: Fix the race condition — use TestHelper.waitForCondition with meaningful timeout
@Test void testRebalance() {
  triggerRebalance();
  TestHelper.waitForCondition(() -> verifyState(), 30_000, "Rebalance did not complete");
}
```

### MAJOR

**C6.3 — Bug fixes require regression tests**
Tests that fail without the fix and pass with it.
- Trigger: Any bug fix PR without a corresponding test

**C6.4 — Integration tests exercise the full pipeline**
Test user-facing queries through rewrite+optimization chain, not internal APIs.
- Trigger: Any PR adding tests for query behavior

**C6.5 — Tests must actually validate claimed behavior**
If tests pass with invalid credentials, the test suite has a gap.
- Trigger: Any PR where test assertions may be vacuously true

**C6.6 — Guard serialization format with tests**
Add round-trip tests for Jackson-annotated classes.
- Trigger: Any PR modifying JSON-serialized config or metadata classes

**C6.7 — Performance-sensitive changes require benchmark comparisons**
Share perf numbers comparing old vs new. Demand evidence before accepting degrading changes.
- Trigger: Any PR claiming performance improvement without benchmarks

**C6.8 — New tests must be verified as stable before merge**
Investigate CI failures in newly added tests before approval.
- Trigger: Any PR adding new tests that show intermittent failures

**C6.9 — Investigate unexpected test changes**
Don't blindly update expected values without understanding root cause.
- Trigger: Any PR modifying test expected values

**C6.10 — Reject unnecessary standalone integration-test clusters**
New Pinot integration tests should not spin up a fresh controller / broker / server / ZK cluster unless the test needs
special setup. Ordinary query/data behavior tests should reuse an existing `CustomDataQueryClusterIntegrationTest`
subclass or add a focused subclass under
`pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/custom/`, with
`@Test(suiteName = "CustomClusterIntegrationTest")`, so it is picked up by
`pinot-integration-tests/src/test/resources/custom-cluster-integration-test-suite.xml`. This keeps CI from paying
another cluster startup cost for tests that only need custom schema, data, and SQL assertions.
- Trigger: Any PR adding a new Pinot integration test class that uses `BaseClusterIntegrationTest`, starts Pinot
  components directly, or creates a dedicated cluster without a distinct topology/component setup requirement

```java
// BAD: New class pays a fresh cluster startup just to test schema/data/query behavior
public class JsonFunctionIntegrationTest extends BaseClusterIntegrationTest { ... }
```
```java
// GOOD: Reuse the shared custom-data cluster suite
@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonFunctionTest extends CustomDataQueryClusterIntegrationTest { ... }
```

### MINOR

**C6.11 — Core concurrent data structures require dedicated concurrent tests**
- Trigger: Any PR adding or modifying concurrent data structures

**C6.12 — Preserve test randomization**
Don't fix random seeds without justification.
- Trigger: Any PR fixing random seeds in tests

**C6.13 — Tests should mimic real-world data states**
Don't assume clean initial conditions.
- Trigger: Any PR with test setup that assumes pristine state

**C6.14 — Remove tests for invalid/unsupported request patterns**
- Trigger: Any PR maintaining tests for deprecated functionality

**C6.15 — Avoid expensive test setup changes**
Don't change @BeforeClass to @BeforeMethod without justification.
- Trigger: Any PR changing test lifecycle annotations

**C6.16 — Binary compatibility baselines should use PR base commit**
Not latest master, to avoid false positives.
- Trigger: Any PR modifying backward compatibility test configuration

**C6.17 — Place tests in the correct test file**
Tests for ClassB don't belong in ClassA's test file.
- Trigger: Any PR adding tests to an unrelated test class

**C6.18 — Maintain test scale unless explicitly justified**
Smaller tests may miss issues. Use assertions, not logging.
- Trigger: Any PR reducing test data size or adding logging to tests

---

## 7. Naming & API Design

### CRITICAL

**C7.1 — Read-only interfaces must not contain mutation methods**
Mutation goes in the builder. Builder pattern objects must be immutable after `build()`.
- Trigger: Any PR adding methods to interfaces — check if they break read-only contract

```java
// BAD: Read-only interface exposes setter — consumers can corrupt shared metadata
public interface ColumnMetadata {
  int getCardinality();
  void setCardinality(int cardinality); // mutation on a read-only interface!
}
```
```java
// GOOD: Mutation in builder only; interface is read-only after build()
public interface ColumnMetadata {
  int getCardinality();
}
public class ColumnMetadataBuilder {
  public ColumnMetadataBuilder setCardinality(int c) { ... }
  public ColumnMetadata build() { return new ColumnMetadataImpl(this); /* immutable */ }
}
```

### MAJOR

**C7.2 — @Nullable annotation discipline**
Annotate accurately on BOTH parameters and return values. Do not annotate params as @Nullable if callers guarantee non-null. Apply consistently across class hierarchies.
- Trigger: Any PR adding or modifying method signatures

**C7.3 — Precise naming**
Method names must match scope and use precise prepositions (`for` not `of`). Variables reflect contents (`tablesUpdated` not `tablesToUpdate`).
- Trigger: Any PR introducing new methods or variables with ambiguous names

**C7.4 — Method signatures reflect actual behavior**
Void for in-place mutation. Primitive types over wrappers when null has no distinct meaning.
- Trigger: Any PR with method signatures that don't match behavior

**C7.5 — Define explicit API contracts**
Document nullability, mutability in interface javadoc. Document whether passed-in collections may be modified.
- Trigger: Any PR adding new interfaces or public APIs

**C7.6 — Annotation conventions**
Use `javax.annotation.Nullable` (not JetBrains or Avro). Do NOT use `@NotNull` in Pinot codebase. `@Nullable` goes above `@Override`. Always use diamond operator. Never use raw types.
- Trigger: Any PR with annotation usage

**C7.7 — Use Predicate/lambda abstractions for existence checks**
Pass `Predicate<String>` rather than `List<String>` to decouple validation from data retrieval.
- Trigger: Any PR passing collections solely for membership testing

**C7.8 — Prefer builder pattern over multi-parameter constructors**
Use `@VisibleForTesting` for test-only access rather than widening visibility.
- Trigger: Any PR with constructors taking 4+ parameters

**C7.9 — Declare variables/parameters using the most general type**
Use interface/base class for declarations, not concrete implementations.
- Trigger: Any PR declaring variables with concrete types when interface would suffice

**C7.10 — Log exceptions exactly once**
Avoid double-logging. Enrich existing log statements rather than adding redundant new ones.
- Trigger: Any PR adding exception logging

### MINOR

**C7.11 — Boolean naming conventions**
Getters use `isXXXEnabled()` not `isEnableXXX()`. Use `==` for enum comparisons.
- Trigger: Any PR adding boolean getters or enum comparisons

**C7.12 — Modern Java idioms**
`List.of()` over `Collections.emptyList()`. Java std lib over Guava. `computeIfAbsent()` over manual check-and-put.
- Trigger: Any PR using older Java patterns

**C7.13 — No final on local variables**
Pinot convention. Effectively final is sufficient for lambda capture.
- Trigger: Any PR adding `final` to local variables

**C7.14 — Config constant names must match string values**
Inconsistency creates confusion and duplicates.
- Trigger: Any PR defining config constants

**C7.15 — Keep interfaces minimal**
No redundant methods. If `next()` returns null, separate `isNull()` is redundant.
- Trigger: Any PR expanding interfaces with convenience methods

**C7.16 — No duplicate enum values**
Use mapping layers for aliases instead.
- Trigger: Any PR adding enum values

**C7.17 — Pipeline stage ordering**
Samplers run after pre-selection (lineage), not before.
- Trigger: Any PR modifying pipeline stage order

**C7.18 — Provide opt-in debug parameters when filtering API response data**
- Trigger: Any PR filtering data in API responses

**C7.19 — No trailing periods in log messages**
State transition logs should capture both previous and new values.
- Trigger: Any PR adding log messages

**C7.20 — Use semantically accurate variable names**
"oldest/latest" for temporal ordering, not "smallest/largest". Plural names for collections.
- Trigger: Any PR with temporal or collection variable names

---

## 8. Process & Scope

### CRITICAL

**C8.1 — SPI changes must be flagged**
Explicitly tag all teams maintaining plugins for review. SPI blast radius extends beyond core.
- Trigger: Any PR modifying classes in `pinot-spi` module

```java
// BAD: Changed SPI interface method without notifying plugin teams
// pinot-spi/src/.../RecordReader.java — added required method
+ void seekToOffset(long offset); // all existing RecordReader plugins now fail to compile
```
```java
// GOOD: Add default method for backward compat; flag plugin teams for review
+ default void seekToOffset(long offset) {
+   throw new UnsupportedOperationException("Not implemented");
+ }
// PR description: "@kafka-plugin-team @kinesis-plugin-team — SPI change, please review"
```

**C8.2 — Do not modify deprecated features with known security implications**
OPTION keyword has injection risk. Respect deprecation boundaries.
- Trigger: Any PR touching deprecated features, especially query syntax

```java
// BAD: "Improving" the deprecated OPTION keyword — extends attack surface
// OPTION(timeColumn='ts') — the value is interpolated unsanitized into query context
+ OPTION(filterExpression='column > ' + userInput) // SQL injection via OPTION
```
```java
// GOOD: Use SET statement (the non-deprecated replacement) — properly validated
SET timeColumn = 'ts';
// Do not add new features to OPTION — it is deprecated specifically because of injection risk
```

### MAJOR

**C8.3 — Follow existing codebase patterns**
Reference how `segment.fetcher`, `ForwardIndexConfig`, etc. are handled. New variants must follow same approach.
- Trigger: Any PR adding new implementations of existing patterns

**C8.4 — Minimize PR scope**
Do not include unrelated changes. No accidental whitespace/formatting changes.
- Trigger: Any PR with changes outside its stated scope

**C8.5 — Separate reverts from improvements**
Revert PRs must be clean reverts. New logic belongs in follow-up PRs.
- Trigger: Any PR combining a revert with new changes

**C8.6 — Apply fixes consistently across all equivalent implementations**
Kafka 2.0 and 3.0 plugins, gRPC and in-memory mailboxes.
- Trigger: Any PR fixing a bug in one implementation that has siblings

**C8.7 — Resolve high-level design questions before implementation review**
Architectural fit before code-level review.
- Trigger: Any PR introducing new subsystems or major design changes

**C8.8 — Every new dependency/metric must be justified**
Dependency version overrides require explicit justification.
- Trigger: Any PR adding new dependencies or overriding versions in pom.xml

**C8.9 — Split large PRs into focused, independently reviewable units**
Critical path changes must be isolated. PRs must be complete — no interfaces without implementations.
- Trigger: Any PR exceeding ~500 lines or touching 3+ subsystems

**C8.10 — Default behavior changes require release notes and documentation updates**
Backward compatibility and performance impact must be explicitly assessed.
- Trigger: Any PR changing default behavior

**C8.11 — Demand evidence before accepting speculative fixes**
Require failing tests or concrete bugs before accepting performance-degrading changes.
- Trigger: Any PR claiming to fix a problem without evidence

**C8.12 — Require design documents for features with complex state management**
- Trigger: Any PR implementing features with distributed state or cross-table semantics

### MINOR

**C8.13 — PR descriptions must explain the problem**
Not just the solution. Include context for reviewers and future readers.
- Trigger: Any PR with insufficient description

**C8.14 — Explore alternative approaches in separate PRs**
Compare designs before committing.
- Trigger: Any PR where alternative designs were discussed but not prototyped

**C8.15 — Model features aligned with domain standards**
Text search follows Lucene/OpenSearch DSL. SQL functions follow SQL standard semantics.
- Trigger: Any PR adding query syntax or search features

**C8.16 — Remove System.out.println**
Use proper logging frameworks.
- Trigger: Any PR with System.out usage

**C8.17 — Don't defer trivial fixes**
Make the change directly instead of leaving TODOs.
- Trigger: Any PR adding TODO comments for trivial changes

**C8.18 — Leverage parser framework (Calcite) capabilities**
Before implementing custom SQL syntax.
- Trigger: Any PR adding custom SQL parsing

**C8.19 — Understand framework conventions before breaking them**
Empirically verify whether a feature is actually used before removing support.
- Trigger: Any PR modifying framework integration behavior

**C8.20 — Approve well-scoped fixes; defer broader redesign**
Approve the immediate fix while deferring broader design to separate threads.
- Trigger: Any PR that could be a focused fix but attempts broader changes

---

## Summary

| Domain | Critical | Major | Minor | Total |
|--------|----------|-------|-------|-------|
| 1. Configuration & Backward Compat | 5 | 7 | 4 | 16 |
| 2. State Management & Concurrency | 6 | 7 | 3 | 16 |
| 3. Code Architecture & Module Design | 5 | 11 | 8 | 24 |
| 4. Performance & Efficiency | 2 | 16 | 4 | 22 |
| 5. Correctness & Safety | 9 | 16 | 3 | 28 |
| 6. Testing Strategies | 2 | 8 | 8 | 18 |
| 7. Naming & API Design | 1 | 9 | 10 | 20 |
| 8. Process & Scope | 2 | 10 | 8 | 20 |
| **Total** | **32** | **84** | **48** | **164** |
