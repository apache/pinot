# Sparse Map Code Review: Master Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute each subplan.

**Goal:** Address all 9 code review findings on the SPARSE_MAP index implementation (commit c89092ab).

**Tech Stack:** Java 11, Apache Pinot, JUnit 5/TestNG, Maven (`./mvnw`)

**Working directory:** `/home/user/pinot`

**Key files:**
- `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/OnHeapSparseMapIndexCreator.java`
- `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/ImmutableSparseMapIndexReader.java`
- `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/MutableSparseMapIndexImpl.java`
- `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexHandler.java`
- `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexType.java`
- `pinot-core/src/main/java/org/apache/pinot/core/operator/filter/SparseMapFilterOperator.java`
- `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/SparseMapIndexConfig.java`
- Test files under `pinot-segment-local/src/test/...` and `pinot-core/src/test/...`

---

## Status

| # | Priority | Issue | Status | Subplan |
|---|----------|-------|--------|---------|
| 1 | CRITICAL | Byte order inconsistency in binary format | ✅ DONE (commit 9df4d6f114) | [subplan-1](#subplan-1-done) |
| 2 | HIGH | Silent key truncation without logging | 🔲 TODO | [subplan-2](#subplan-2-silent-key-truncation) |
| 3 | HIGH | Type coercion ClassCastException risk | 🔲 TODO | [subplan-3](#subplan-3-type-coercion-validation) |
| 4 | HIGH | Null vs. absent semantics unspecified | 🔲 TODO | [subplan-4](#subplan-4-null-vs-absent-semantics) |
| 5 | MEDIUM | Real-time memory unbounded | 🔲 TODO | [subplan-5](#subplan-5-real-time-memory-cap) |
| 6 | MEDIUM | No feature flag for rollout control | 🔲 TODO | [subplan-6](#subplan-6-feature-flag) |
| 7 | MEDIUM | NOT_EQ absent-key edge case untested | 🔲 TODO | [subplan-7](#subplan-7-not_eq-absent-key-test) |
| 8 | LOW | Segment merge unimplemented | 🔲 TODO | [subplan-8](#subplan-8-segment-merge) |
| 9 | LOW | Test coverage gaps | 🔲 TODO | [subplan-9](#subplan-9-test-coverage-gaps) |

---

## Subplan 1 (DONE): Byte Order Standardization {#subplan-1-done}

**Completed in commit `9df4d6f114`.** No further action required.

Changes: fixed `writeInt`/`writeLong` to BIG_ENDIAN; added `MAGIC=0x53504D58`; bumped `VERSION` to 2; reader validates magic + version; key metadata parsed as BIG_ENDIAN.

---

## Subplan 2: Silent Key Truncation {#subplan-2-silent-key-truncation}

**Files to modify:**
- `OnHeapSparseMapIndexCreator.java` (line 98–100)
- `MutableSparseMapIndexImpl.java` (line 101–103)

**Problem:** When `_maxKeys` is exceeded, new keys are silently dropped:
```java
if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
  continue;  // silent drop, no log
}
```

### Task 2.1: Add warn logging + dropped-key counter in `OnHeapSparseMapIndexCreator`

Add a `_droppedKeyCount` field and `LOGGER`. Replace the silent `continue` with a logged drop. Log one warning per unique dropped key (not per document, to avoid log spam).

**Current code to replace (line ~98):**
```java
if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
  continue;
}
```
**Replace with:**
```java
if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
  if (!_droppedKeys.contains(key)) {
    _droppedKeys.add(key);
    LOGGER.warn(
        "SparseMap index for column '{}' reached maxKeys limit ({}). Dropping key '{}'. "
            + "Total distinct dropped keys so far: {}.",
        _columnName, _maxKeys, key, _droppedKeys.size());
  }
  continue;
}
```

Add to class fields:
```java
private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapSparseMapIndexCreator.class);
private final Set<String> _droppedKeys = new HashSet<>();
```

Add necessary imports: `org.slf4j.Logger`, `org.slf4j.LoggerFactory`, `java.util.HashSet`.

### Task 2.2: Add warn logging + dropped-key counter in `MutableSparseMapIndexImpl`

Same approach. The existing class already has `private int _distinctKeyCount`. Add a `_droppedKeyCount` int field (simpler than a Set for the mutable case, since we can't easily deduplicate across concurrent threads without overhead).

**Current code to replace (line ~101):**
```java
if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
  continue;
}
```
**Replace with:**
```java
if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
  _droppedKeyCount++;
  if (_droppedKeyCount == 1 || _droppedKeyCount % 1000 == 0) {
    LOGGER.warn(
        "MutableSparseMapIndex reached maxKeys limit ({}). Key '{}' dropped. "
            + "Total drops: {}.",
        _maxKeys, key, _droppedKeyCount);
  }
  continue;
}
```

Add to class fields:
```java
private static final Logger LOGGER = LoggerFactory.getLogger(MutableSparseMapIndexImpl.class);
private int _droppedKeyCount;
```

The existing class already imports SLF4J indirectly via Pinot; add `org.slf4j.Logger` and `org.slf4j.LoggerFactory` if not present.

### Task 2.3: Add tests

In `SparseMapIndexTest.java`:
```java
@Test
public void testMaxKeysDropsExcessKeysWithLogging() throws Exception {
  // Config with maxKeys=2
  // Add doc with 3 keys
  // Assert only 2 keys present in sealed index
  // Assert the third key is absent (no bitmap, no value)
}
```

In a mutable test (or `SparseMapIndexTest`):
```java
@Test
public void testMutableMaxKeysDropsExcessKeys() {
  // Build MutableSparseMapIndexImpl with maxKeys=2
  // add() doc 0 with keys {a, b, c}
  // Assert getKeys().size() == 2
  // Assert key "c" has zero docs (presence bitmap empty)
}
```

### Task 2.4: Run tests + style + commit

```bash
cd /home/user/pinot
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapIndexTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
./mvnw spotless:apply checkstyle:check -pl pinot-segment-local -Dlicense.skip
git add <files>
git commit -m "fix(sparsemap): log warn when maxKeys exceeded, track dropped key count"
```

---

## Subplan 4: Null vs. Absent Semantics {#subplan-4-null-vs-absent-semantics}

**Files to modify:**
- `OnHeapSparseMapIndexCreator.java` — `add()` and `coerceValue()`
- `MutableSparseMapIndexImpl.java` — `add()`
- `ImmutableSparseMapIndexReader.java` — class-level Javadoc

**Problem (current inconsistency):**
- `MutableSparseMapIndexImpl.add()` line 98: `if (rawValue == null) { continue; }` → null means **absent** (key not recorded)
- `OnHeapSparseMapIndexCreator.add()`: null value calls `coerceValue(null, valueType)` → calls `coerceValueForType(dataType, null)` → returns a typed default → null means **present with default value**

These two behave differently for null values. Both should treat null as absent.

### Task 4.1: Fix `OnHeapSparseMapIndexCreator.add()` to skip null values

In `add(Map<String, Object> sparseMap)`, after extracting `entry.getValue()`, skip null:
```java
Object rawValue = entry.getValue();
if (rawValue == null) {
  // null value treated as absent: key not recorded for this document
  continue;
}
```
This mirrors the mutable implementation's behavior.

### Task 4.2: Add clarifying comments in both files

In `OnHeapSparseMapIndexCreator.add()` — above the null check:
```java
// Null values are treated as absent: the key is not recorded for this document.
// There is no distinction between "key absent" and "key present with null value".
```

In `MutableSparseMapIndexImpl.add()` — above the existing null check (line ~98):
```java
// Null values are treated as absent: the key is not recorded for this document.
// There is no distinction between "key absent" and "key present with null value".
```

In `ImmutableSparseMapIndexReader` class-level Javadoc, add a sentence:
```java
 * <p>Null-means-absent policy: keys with null values are not recorded during ingestion.
 * A key that does not appear in the presence bitmap is indistinguishable from one
 * that was explicitly set to null.
```

### Task 4.3: Add null-handling tests

In `SparseMapIndexTest.java`:
```java
@Test
public void testNullValueTreatedAsAbsent() throws Exception {
  // add() doc 0 with Map of("key", null)
  // Sealed index: presence bitmap for "key" must NOT contain docId 0
  // getInt(0, "key") returns 0 (default), getPresenceBitmap("key").contains(0) == false
}

@Test
public void testNullValueMutableTreatedAsAbsent() {
  // MutableSparseMapIndexImpl: add doc 0 with null value for "key"
  // getPresenceBitmap("key").contains(0) must be false
}
```

### Task 4.4: Run tests + style + commit

```bash
cd /home/user/pinot
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapIndexTest,SparseMapIndexEndToEndTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
./mvnw spotless:apply checkstyle:check -pl pinot-segment-local -Dlicense.skip
git add <files>
git commit -m "fix(sparsemap): standardize null-means-absent policy, add comments and tests"
```


---

## Subplan 7: NOT_EQ Absent-Key Edge Case Test {#subplan-7-not_eq-absent-key-test}

**Files to modify:**
- `SparseMapFilterOperator.java` — add comments
- `pinot-core/src/test/java/...` — find or create `SparseMapFilterOperatorTest.java`

**Problem:** The `NOT_EQ` and `NOT_IN` implementations are correct (absence = excluded from results) but this semantic is undocumented and untested.

Current code in `SparseMapFilterOperator.java` (line ~105):
```java
case NOT_EQ: {
  String value = ((NotEqPredicate) _predicate).getValue();
  ImmutableRoaringBitmap matching = _sparseMapReader.getDocsWithKeyValue(_keyName, value);
  ImmutableRoaringBitmap presence = _sparseMapReader.getPresenceBitmap(_keyName);
  if (matching == null) {
    return presence;
  }
  return ImmutableRoaringBitmap.andNot(presence, matching);
}
```

### Task 7.1: Add explanatory comment to `NOT_EQ` and `NOT_IN` cases

In `SparseMapFilterOperator.java`, add above the `NOT_EQ` case:
```java
// NOT_EQ semantics: docs where the key is ABSENT are excluded from results.
// Only docs where the key is PRESENT and its value != the predicate value are returned.
// This matches SQL NULL semantics: NULL != X is unknown, not true.
```

And above `NOT_IN`:
```java
// NOT_IN semantics: same absence-exclusion policy as NOT_EQ.
// Only docs where the key is present and value not in the set are returned.
```

### Task 7.2: Find or create `SparseMapFilterOperatorTest`

Search for the test file:
```bash
find /home/user/pinot -name "SparseMapFilterOperatorTest.java"
```

If not found, create it at:
`pinot-core/src/test/java/org/apache/pinot/core/operator/filter/SparseMapFilterOperatorTest.java`

### Task 7.3: Add tests

```java
@Test
public void testNotEqExcludesDocsWhereKeyIsAbsent() {
  // Setup: 4 docs
  //   doc 0: key "color" = "red"
  //   doc 1: key "color" = "blue"
  //   doc 2: NO "color" key (absent)
  //   doc 3: key "color" = "green"
  // Predicate: color != "red"
  // Expected: {1, 3} — doc 2 (absent) is NOT included

  // Use a mock/stub SparseMapIndexReader or build a real index
  // Assert result bitmap == {1, 3}
}

@Test
public void testNotInExcludesDocsWhereKeyIsAbsent() {
  // Setup: 4 docs, doc 2 has no "color" key
  // Predicate: color NOT IN ("red")
  // Expected: {1, 3} — doc 2 excluded
}

@Test
public void testNotEqAllDocsAbsent() {
  // Setup: 3 docs, none have "color" key
  // Predicate: color != "red"
  // Expected: empty bitmap
}
```

### Task 7.4: Run tests + style + commit

```bash
cd /home/user/pinot
./mvnw test -pl pinot-core \
  -Dtest=SparseMapFilterOperatorTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
./mvnw spotless:apply checkstyle:check -pl pinot-core,pinot-segment-local -Dlicense.skip
git add <files>
git commit -m "test(sparsemap): add NOT_EQ/NOT_IN absent-key edge case tests, add semantic comments"
```

---

## Subplan 8: Segment Merge Strategy {#subplan-8-segment-merge}

**Files to modify:**
- `SparseMapIndexHandler.java` — add merge logic
- Test file for merge scenario

**Problem:** When two segments with disjoint or overlapping SPARSE_MAP key sets are merged, there is no defined behavior for merging the indexes.

**Approach:** Implement a union-of-key-sets merge strategy in `SparseMapIndexHandler`. Since Pinot segment merge typically runs through the `MergeTask` minion, the merge is handled by re-ingesting documents via the creator. The handler's job is to document this.

### Task 8.1: Document merge behavior in `SparseMapIndexHandler`

Add class-level Javadoc explaining merge behavior:
```java
/**
 * ...existing Javadoc...
 *
 * <p>Segment merge: when segments are merged (e.g., via Minion merge task), the SPARSE_MAP
 * index is rebuilt from scratch by re-ingesting all documents through
 * {@link OnHeapSparseMapIndexCreator}. The resulting merged index contains the union of all
 * keys observed across the merged segments, subject to the {@code maxKeys} limit of the
 * merged segment's config. No explicit merge handler is needed: the creator handles
 * key-set union naturally as documents are added in order.
 */
```

### Task 8.2: Add a merge integration test

In `SparseMapIndexEndToEndTest.java` (or a new `SparseMapMergeTest.java`):
```java
@Test
public void testKeyUnionAcrossSegments() throws Exception {
  // Segment A: docs with keys {price, quantity}
  // Segment B: docs with keys {color, quantity}  <- overlapping "quantity"
  // Merged (by re-ingesting both through a single creator):
  //   keys = {price, quantity, color}
  //   "quantity" has values from both original segments
  // Assert merged index has 3 keys
  // Assert "quantity" presence bitmap covers all docs
  // Assert "price" presence bitmap covers only segment A docs
  // Assert "color" presence bitmap covers only segment B docs
}
```

### Task 8.3: Run tests + style + commit

```bash
cd /home/user/pinot
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapIndexEndToEndTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
./mvnw spotless:apply checkstyle:check -pl pinot-segment-local -Dlicense.skip
git add <files>
git commit -m "docs(sparsemap): document segment merge behavior, add key-union merge test"
```

---

## Subplan 9: Test Coverage Gaps {#subplan-9-test-coverage-gaps}

**Files to modify:**
- `SparseMapIndexTest.java` or `MutableSparseMapIndexImplTest.java` (create if needed)

**Missing test scenarios:**

### Task 9.1: Concurrent `add()` + `getMap()` under write-heavy load

```java
@Test
public void testConcurrentAddAndReadDoesNotThrow() throws Exception {
  MutableSparseMapIndexImpl idx = buildMutableIndex(maxKeys=100);
  ExecutorService pool = Executors.newFixedThreadPool(4);
  AtomicBoolean failed = new AtomicBoolean(false);

  // 2 writer threads
  for (int t = 0; t < 2; t++) {
    pool.submit(() -> {
      for (int i = 0; i < 500; i++) {
        try { idx.add(Map.of("k" + i, i), -1, i); }
        catch (Exception e) { failed.set(true); }
      }
    });
  }
  // 2 reader threads
  for (int t = 0; t < 2; t++) {
    pool.submit(() -> {
      for (int i = 0; i < 500; i++) {
        try { idx.getMap(i); }
        catch (Exception e) { failed.set(true); }
      }
    });
  }
  pool.shutdown();
  pool.awaitTermination(10, TimeUnit.SECONDS);
  assertFalse(failed.get(), "Concurrent access caused an exception");
}
```

### Task 9.2: `_maxKeys` overflow — assert which keys are retained

```java
@Test
public void testMaxKeysRetainsFirstKeysEncountered() throws Exception {
  // Add doc 0 with keys {a:1, b:2, c:3} but maxKeys=2
  // Keys are iterated from a HashMap so insertion order is not guaranteed.
  // Assert: exactly 2 keys in sealed index (any 2 of {a, b, c})
  // Assert: the 3rd key has an empty presence bitmap
}
```

### Task 9.3: Round-trip null handling (write null → read → expect absent)

(Already covered in subplan 4, reference here for completeness.)

### Task 9.4: Segment seal during concurrent ingestion

```java
@Test
public void testSealDuringConcurrentAddIsThreadSafe() throws Exception {
  OnHeapSparseMapIndexCreator creator = buildCreator();
  // add 1000 docs on a background thread
  // seal() on main thread after 500ms
  // assert no ConcurrentModificationException
  // assert sealed file is valid and readable
}
```

Note: `OnHeapSparseMapIndexCreator` is NOT designed to be thread-safe (add is called by one thread, seal by the framework after ingestion). Document this in Javadoc and verify the test confirms the expected single-threaded usage. If the test shows thread-safety is needed, add synchronization.

### Task 9.5: Run all new tests + style + commit

```bash
cd /home/user/pinot
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapIndexTest,SparseMapIndexEndToEndTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
./mvnw spotless:apply checkstyle:check -pl pinot-segment-local -Dlicense.skip
git add <files>
git commit -m "test(sparsemap): add concurrent access, maxKeys overflow, and seal tests"
```

---

## Execution Order

Execute subplans in priority order. Each subplan is independent and can be committed separately.

1. **Subplan 2** — `HIGH`: Logging for silent key drops (smallest change, highest operator value)
3. **Subplan 4** — `HIGH`: Null semantics alignment (correctness fix + tests)
4. **Subplan 7** — `MEDIUM`: NOT_EQ absent-key test (pure test + comment, very low risk)
7. **Subplan 8** — `LOW`: Merge documentation + test
8. **Subplan 9** — `LOW`: Remaining test coverage gaps

**Run full suite at the end:**
```bash
./mvnw test -pl pinot-spi,pinot-segment-local,pinot-core \
  -Dtest="SparseMap*" \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
```
