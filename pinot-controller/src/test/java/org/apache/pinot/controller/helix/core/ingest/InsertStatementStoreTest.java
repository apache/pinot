/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.ingest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Unit tests for {@link InsertStatementStore} using an in-memory property store.
///
/// Covers CRUD operations for statement manifests, requestId reservation/release/rebind,
/// version-checked updates, and cross-table search.
public class InsertStatementStoreTest {

  private static final String TABLE = "testTable_OFFLINE";
  private static final String TABLE2 = "otherTable_OFFLINE";

  private InsertStatementStore _store;
  private InMemoryPropertyStore _propertyStore;

  @BeforeMethod
  public void setUp() {
    _propertyStore = new InMemoryPropertyStore();
    _store = new InsertStatementStore(_propertyStore);
  }

  /// -------------------------------------------------------------------------
  /// createStatement / getStatement
  /// -------------------------------------------------------------------------

  @Test
  public void testCreateAndGetStatement() {
    InsertStatementManifest manifest = makeManifest("stmt-1", "req-1", TABLE, InsertStatementState.ACCEPTED);

    assertTrue(_store.createStatement(manifest), "first create should succeed");

    InsertStatementManifest loaded = _store.getStatement(TABLE, "stmt-1");
    assertNotNull(loaded);
    assertEquals(loaded.getStatementId(), "stmt-1");
    assertEquals(loaded.getRequestId(), "req-1");
    assertEquals(loaded.getTableNameWithType(), TABLE);
    assertEquals(loaded.getState(), InsertStatementState.ACCEPTED);
    assertEquals(loaded.getInsertType(), InsertType.ROW);
  }

  @Test
  public void testCreateStatementIdempotent() {
    InsertStatementManifest manifest = makeManifest("stmt-2", "req-2", TABLE, InsertStatementState.ACCEPTED);

    assertTrue(_store.createStatement(manifest));
    /// Second create for same statementId must return false (ZK create is atomic)
    assertFalse(_store.createStatement(manifest), "duplicate create must return false");
  }

  @Test
  public void testGetStatementNotFound() {
    assertNull(_store.getStatement(TABLE, "nonexistent"));
  }

  /// -------------------------------------------------------------------------
  /// updateStatement
  /// -------------------------------------------------------------------------

  @Test
  public void testUpdateStatement() {
    InsertStatementManifest manifest = makeManifest("stmt-3", "req-3", TABLE, InsertStatementState.ACCEPTED);
    assertTrue(_store.createStatement(manifest));

    manifest.setState(InsertStatementState.VISIBLE);
    assertTrue(_store.updateStatement(manifest), "update of existing statement should succeed");

    InsertStatementManifest reloaded = _store.getStatement(TABLE, "stmt-3");
    assertNotNull(reloaded);
    assertEquals(reloaded.getState(), InsertStatementState.VISIBLE);
  }

  @Test
  public void testUpdateStatementNotFound() {
    InsertStatementManifest manifest = makeManifest("stmt-ghost", "req-ghost", TABLE, InsertStatementState.ACCEPTED);
    /// Node was never created — update must return false
    assertFalse(_store.updateStatement(manifest));
  }

  @Test
  public void testUpdateStatementVersionConflictReturnsFalse() {
    /// updateStatement always does a fresh get+set inside a single call, so a real version
    /// conflict can only arise when a concurrent writer bumps the ZK version between the
    /// get and the set. We simulate that here by injecting a fault into the property store
    /// that causes the next version-checked set to return false, and verify that
    /// updateStatement propagates false rather than throwing or silently succeeding.
    AtomicBoolean failNextVersionedSet = new AtomicBoolean(false);
    InMemoryPropertyStore faultingStore = new InMemoryPropertyStore() {
      @Override
      public synchronized boolean set(String path, ZNRecord record, int expectedVersion, int options) {
        if (failNextVersionedSet.getAndSet(false)) {
          return false; /// simulate version conflict
        }
        return super.set(path, record, expectedVersion, options);
      }
    };
    InsertStatementStore store = new InsertStatementStore(faultingStore);

    InsertStatementManifest manifest = makeManifest("stmt-vc", "req-vc", TABLE, InsertStatementState.ACCEPTED);
    assertTrue(store.createStatement(manifest));

    /// Arm the fault so the next version-checked set returns false
    failNextVersionedSet.set(true);
    manifest.setState(InsertStatementState.VISIBLE);
    assertFalse(store.updateStatement(manifest),
        "updateStatement must return false when the underlying version-checked set fails");
  }

  /// -------------------------------------------------------------------------
  /// listStatements / deleteStatement
  /// -------------------------------------------------------------------------

  @Test
  public void testListStatements() {
    _store.createStatement(makeManifest("ls-1", "r1", TABLE, InsertStatementState.ACCEPTED));
    _store.createStatement(makeManifest("ls-2", "r2", TABLE, InsertStatementState.VISIBLE));
    _store.createStatement(makeManifest("ls-3", "r3", TABLE2, InsertStatementState.VISIBLE));

    List<InsertStatementManifest> tableResult = _store.listStatements(TABLE);
    assertEquals(tableResult.size(), 2);
    List<String> ids = tableResult.stream().map(InsertStatementManifest::getStatementId)
        .sorted().collect(Collectors.toList());
    assertEquals(ids, Arrays.asList("ls-1", "ls-2"));
  }

  @Test
  public void testListStatementsEmpty() {
    List<InsertStatementManifest> result = _store.listStatements("noSuchTable_OFFLINE");
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  /// Forward-compat regression test: a manifest envelope written by a future controller with a
  /// higher schemaVersion than this controller supports MUST be skipped (not crash) by
  /// listStatements. The cleanup sweep relies on this — one bad record should not stall cleanup
  /// for healthy peers.
  @Test
  public void testListStatementsSkipsForwardIncompatEnvelopeSchemaVersion() {
    /// Write one healthy manifest the conventional way.
    _store.createStatement(makeManifest("good-1", "r-good", TABLE, InsertStatementState.ACCEPTED));

    /// Write a poisoned ZNRecord directly: envelope schemaVersion=99 (way higher than supported).
    String poisonStmtId = "poison-1";
    String poisonPath = "/INSERT_STATEMENTS/" + TABLE + "/" + poisonStmtId;
    ZNRecord poisoned = new ZNRecord(poisonStmtId);
    poisoned.setSimpleField("manifest", "{\"schemaVersion\":1,\"statementId\":\"" + poisonStmtId
        + "\",\"tableNameWithType\":\"" + TABLE + "\",\"insertType\":\"ROW\",\"state\":\"ACCEPTED\","
        + "\"createdTimeMs\":0,\"lastUpdatedTimeMs\":0}");
    poisoned.setSimpleField("envelopeSchemaVersion", "99");
    _propertyStore.create(poisonPath, poisoned, org.apache.helix.AccessOption.PERSISTENT);

    /// listStatements must return the healthy record and skip the poisoned one without throwing.
    List<InsertStatementManifest> result = _store.listStatements(TABLE);
    assertEquals(result.size(), 1, "Forward-incompat envelope must be skipped, healthy record kept");
    assertEquals(result.get(0).getStatementId(), "good-1");
  }

  @Test
  public void testDeleteStatement() {
    InsertStatementManifest manifest = makeManifest("del-1", "req-d", TABLE, InsertStatementState.ABORTED);
    assertTrue(_store.createStatement(manifest));
    assertNotNull(_store.getStatement(TABLE, "del-1"));

    assertTrue(_store.deleteStatement(TABLE, "del-1"));
    assertNull(_store.getStatement(TABLE, "del-1"));
  }

  /// -------------------------------------------------------------------------
  /// reserveRequestId
  /// -------------------------------------------------------------------------

  @Test
  public void testReserveRequestIdFirstCallReturnsNull() {
    String existing = _store.reserveRequestId(TABLE, "req-new", "stmt-new");
    assertNull(existing, "first reservation should return null (this caller wins)");
  }

  @Test
  public void testReserveRequestIdDuplicateReturnsExisting() {
    _store.reserveRequestId(TABLE, "req-dup", "stmt-original");
    String existing = _store.reserveRequestId(TABLE, "req-dup", "stmt-retry");
    assertEquals(existing, "stmt-original", "duplicate reservation must return original statementId");
  }

  @Test
  public void testReserveRequestIdIsolatedByTable() {
    _store.reserveRequestId(TABLE, "req-iso", "stmt-table1");
    /// Same requestId on a different table must be a fresh reservation
    String existing = _store.reserveRequestId(TABLE2, "req-iso", "stmt-table2");
    assertNull(existing, "requestId on a different table must not conflict");
  }

  /// -------------------------------------------------------------------------
  /// releaseRequestId
  /// -------------------------------------------------------------------------

  @Test
  public void testReleaseRequestId() {
    _store.reserveRequestId(TABLE, "req-rel", "stmt-rel");
    _store.releaseRequestId(TABLE, "req-rel");

    /// After release, a new reservation must succeed (returns null)
    String existing = _store.reserveRequestId(TABLE, "req-rel", "stmt-new");
    assertNull(existing, "reservation should succeed after release");
  }

  @Test
  public void testReleaseRequestIdNullIsNoOp() {
    /// Must not throw
    _store.releaseRequestId(TABLE, null);
  }

  /// -------------------------------------------------------------------------
  /// rebindRequestId
  /// -------------------------------------------------------------------------

  @Test
  public void testRebindRequestId() {
    _store.reserveRequestId(TABLE, "req-rebind", "stmt-old");

    boolean rebound = _store.rebindRequestIdIfEquals(TABLE, "req-rebind", "stmt-old", "stmt-new");
    assertTrue(rebound, "rebind of existing reservation should succeed");

    /// A subsequent reservation attempt must see the new statementId
    String existing = _store.reserveRequestId(TABLE, "req-rebind", "stmt-irrelevant");
    assertEquals(existing, "stmt-new", "after rebind the reservation should point to the new statementId");
  }

  @Test
  public void testRebindRequestIdLosesRaceWhenExpectedMismatches() {
    /// Another caller has already rebound the reservation to stmt-winner. A second caller that
    /// still sees the original stale statementId must lose the content-checked CAS.
    _store.reserveRequestId(TABLE, "req-rebind-race", "stmt-original");
    assertTrue(_store.rebindRequestIdIfEquals(TABLE, "req-rebind-race", "stmt-original", "stmt-winner"));

    /// Now the second caller, still thinking the reservation points at "stmt-original", should fail.
    boolean second = _store.rebindRequestIdIfEquals(TABLE, "req-rebind-race", "stmt-original", "stmt-loser");
    assertFalse(second, "second rebind must lose — the reservation no longer equals the expected id");
  }

  @Test
  public void testRebindRequestIdNotFound() {
    /// Node was never created
    boolean rebound = _store.rebindRequestIdIfEquals(TABLE, "req-ghost", "stmt-old", "stmt-new");
    assertFalse(rebound, "rebind of non-existent reservation should return false");
  }

  @Test
  public void testRebindRequestIdNullIsNoOp() {
    assertFalse(_store.rebindRequestIdIfEquals(TABLE, null, "stmt-old", "stmt-x"));
    assertFalse(_store.rebindRequestIdIfEquals(TABLE, "req-x", null, "stmt-x"));
  }

  /// -------------------------------------------------------------------------
  /// findByRequestId
  /// -------------------------------------------------------------------------

  @Test
  public void testFindByRequestIdViaReservationIndex() {
    InsertStatementManifest manifest = makeManifest("stmt-find", "req-find", TABLE, InsertStatementState.ACCEPTED);
    _store.createStatement(manifest);
    _store.reserveRequestId(TABLE, "req-find", "stmt-find");

    InsertStatementManifest found = _store.findByRequestId(TABLE, "req-find");
    assertNotNull(found);
    assertEquals(found.getStatementId(), "stmt-find");
  }

  @Test
  public void testFindByRequestIdFallbackScan() {
    /// Create manifest without a reservation znode — exercises the fallback scan path.
    /// No call to reserveRequestId, so the /INSERT_REQUEST_IDS path is absent.
    InsertStatementManifest manifest = makeManifest("stmt-scan", "req-scan", TABLE, InsertStatementState.VISIBLE);
    _store.createStatement(manifest);

    InsertStatementManifest found = _store.findByRequestId(TABLE, "req-scan");
    assertNotNull(found);
    assertEquals(found.getStatementId(), "stmt-scan");
    assertEquals(found.getRequestId(), "req-scan",
        "fallback scan must return the manifest with the matching requestId");
  }

  @Test
  public void testFindByRequestIdNotFound() {
    assertNull(_store.findByRequestId(TABLE, "req-missing"));
  }

  /// -------------------------------------------------------------------------
  /// listTablesWithStatements / findStatementAcrossTables
  /// -------------------------------------------------------------------------

  @Test
  public void testListTablesWithStatements() {
    _store.createStatement(makeManifest("t1s1", "r1", TABLE, InsertStatementState.ACCEPTED));
    _store.createStatement(makeManifest("t2s1", "r2", TABLE2, InsertStatementState.VISIBLE));

    List<String> tables = _store.listTablesWithStatements();
    assertTrue(tables.contains(TABLE), "should include " + TABLE);
    assertTrue(tables.contains(TABLE2), "should include " + TABLE2);
  }

  @Test
  public void testListTablesWithStatementsEmpty() {
    assertTrue(_store.listTablesWithStatements().isEmpty());
  }

  @Test
  public void testFindStatementAcrossTables() {
    _store.createStatement(makeManifest("cross-1", "rc1", TABLE, InsertStatementState.VISIBLE));
    _store.createStatement(makeManifest("cross-2", "rc2", TABLE2, InsertStatementState.VISIBLE));

    InsertStatementManifest found = _store.findStatementAcrossTables("cross-2");
    assertNotNull(found);
    assertEquals(found.getStatementId(), "cross-2");
    assertEquals(found.getTableNameWithType(), TABLE2);
  }

  @Test
  public void testFindStatementAcrossTablesNotFound() {
    assertNull(_store.findStatementAcrossTables("nonexistent"));
  }

  /// -------------------------------------------------------------------------
  /// pruneStaleReservationTombstones / GC_PENDING handling
  /// -------------------------------------------------------------------------

  @Test
  public void testPruneStaleReservationTombstonesSkipsActive() {
    /// Active reservation must NOT be pruned regardless of retention argument.
    _store.reserveRequestId(TABLE, "req-active", "stmt-active");
    int pruned = _store.pruneStaleReservationTombstones(TABLE, 0L);
    assertEquals(pruned, 0);
    String active = _store.reserveRequestId(TABLE, "req-active", "stmt-other");
    assertEquals(active, "stmt-active");
  }

  @Test
  public void testPruneStaleReservationTombstonesReapsTombstone() {
    /// A tombstoned reservation older than retention should be reaped. (InMemoryPropertyStore
    /// returns mtime=0 so any positive retention treats every record as "old"; we exploit that
    /// by passing 0 so the comparison `mtime > cutoffMs` reads `0 > now` = false, age-eligible.)
    _store.reserveRequestId(TABLE, "req-tombstoned", "stmt-tombstoned");
    boolean released = _store.releaseRequestIdIfEquals(TABLE, "req-tombstoned", "stmt-tombstoned");
    assertTrue(released);

    int pruned = _store.pruneStaleReservationTombstones(TABLE, 0L);
    assertEquals(pruned, 1);

    /// After prune the path is gone — fresh reservation succeeds.
    assertNull(_store.reserveRequestId(TABLE, "req-tombstoned", "stmt-fresh"));
  }

  @Test
  public void testPruneStaleReservationTombstonesReapsGcPendingOrphan() {
    /// Simulate a prior sweep that CAS-set GC_PENDING but crashed before remove. The current
    /// sweep reaps GC_PENDING regardless of mtime so orphans are not stuck forever.
    org.apache.helix.zookeeper.datamodel.ZNRecord orphan =
        new org.apache.helix.zookeeper.datamodel.ZNRecord("req-orphan");
    orphan.setSimpleField("statementId", "__GC_PENDING_stmt-orphan");
    String path = "/INSERT_REQUEST_IDS/" + TABLE + "/req-orphan";
    assertTrue(_propertyStore.create(path, orphan, org.apache.helix.AccessOption.PERSISTENT));

    int pruned = _store.pruneStaleReservationTombstones(TABLE, 0L);
    assertEquals(pruned, 1);

    /// After prune the path is gone — fresh reservation succeeds.
    assertNull(_store.reserveRequestId(TABLE, "req-orphan", "stmt-fresh"));
  }

  @Test
  public void testRebindRefusesGcPendingPrefix() {
    /// Place a GC_PENDING record directly. The rebind path must refuse to overwrite it (the
    /// sweep's unconditional remove follows immediately and would wipe a fresh reservation).
    org.apache.helix.zookeeper.datamodel.ZNRecord pending =
        new org.apache.helix.zookeeper.datamodel.ZNRecord("req-pending-gc");
    pending.setSimpleField("statementId", "__GC_PENDING_stmt-pending");
    String path = "/INSERT_REQUEST_IDS/" + TABLE + "/req-pending-gc";
    assertTrue(_propertyStore.create(path, pending, org.apache.helix.AccessOption.PERSISTENT));

    boolean rebound = _store.rebindRequestIdIfEquals(TABLE, "req-pending-gc",
        "__GC_PENDING_stmt-pending", "stmt-new");
    assertFalse(rebound, "rebind must refuse GC_PENDING records");
  }

  /// -------------------------------------------------------------------------
  /// helpers
  /// -------------------------------------------------------------------------

  private static InsertStatementManifest makeManifest(String statementId, String requestId,
      String table, InsertStatementState state) {
    return new InsertStatementManifest(
        statementId, requestId, "hash-" + statementId, table,
        InsertType.ROW, state,
        1000L, 1000L, List.of(), null, null, null);
  }

  /// -------------------------------------------------------------------------
  /// In-memory ZkHelixPropertyStore substitute
  ///
  /// Overrides every method called by InsertStatementStore:
  ///   create(path, record, options)              — atomic; returns false if exists
  ///   get(path, stat, options)                   — populates stat.version
  ///   set(path, record, expectedVersion, options) — version-checked write
  ///   set(path, record, options)                 — unconditional write
  ///   remove(path, options)                      — delete node and children
  ///   exists(path, options)                      — presence check
  ///   getChildNames(parentPath, options)         — list one level of children
  ///
  /// Safety note: the super constructor is called with a null ZkBaseDataAccessor.
  /// This is safe because every method that InsertStatementStore uses is overridden here,
  /// so no call will reach the ZkCacheBaseDataAccessor delegate. The same pattern is used
  /// by FakePropertyStore in pinot-common, which is the established Pinot convention for
  /// unit-testing ZK-backed stores without a live ZK server.
  /// -------------------------------------------------------------------------

  private static class InMemoryPropertyStore extends ZkHelixPropertyStore<ZNRecord> {
    private final Map<String, ZNRecord> _data = new HashMap<>();
    private final Map<String, Integer> _versions = new HashMap<>();

    InMemoryPropertyStore() {
      super((ZkBaseDataAccessor<ZNRecord>) null, null, null);
    }

    @Override
    public synchronized boolean create(String path, ZNRecord record, int options) {
      if (_data.containsKey(path)) {
        return false;
      }
      _data.put(path, record);
      _versions.put(path, 0);
      return true;
    }

    @Override
    public synchronized ZNRecord get(String path, Stat stat, int options) {
      ZNRecord record = _data.get(path);
      if (stat != null && record != null) {
        stat.setVersion(_versions.getOrDefault(path, 0));
      }
      return record;
    }

    /// Version-checked set: fails when expectedVersion does not match.
    @Override
    public synchronized boolean set(String path, ZNRecord record, int expectedVersion, int options) {
      if (!_data.containsKey(path)) {
        return false;
      }
      int current = _versions.getOrDefault(path, 0);
      if (expectedVersion != current) {
        return false;
      }
      _data.put(path, record);
      _versions.put(path, current + 1);
      return true;
    }

    /// Unconditional set (upsert). New keys start at version 0 to match get()'s default.
    @Override
    public synchronized boolean set(String path, ZNRecord record, int options) {
      _data.put(path, record);
      /// getOrDefault(..., -1) + 1 means: a previously-unseen path starts at 0 (consistent
      /// with what get() reports via getOrDefault(path, 0)), and an existing path advances by 1.
      _versions.put(path, _versions.getOrDefault(path, -1) + 1);
      return true;
    }

    @Override
    public synchronized boolean remove(String path, int options) {
      _data.remove(path);
      _versions.remove(path);
      return true;
    }

    @Override
    public synchronized boolean exists(String path, int options) {
      return _data.containsKey(path);
    }

    @Override
    public synchronized List<String> getChildNames(String parentPath, int options) {
      String prefix = parentPath + "/";
      return _data.keySet().stream()
          .filter(k -> k.startsWith(prefix))
          .map(k -> k.substring(prefix.length()).split("/")[0])
          .distinct()
          .collect(Collectors.toList());
    }

    /// Batch-read all immediate children of `parentPath`. Mirrors the behavior of
    /// `ZkHelixPropertyStore.getChildren` which returns a list of ZNRecords (one per child)
    /// in a single ZK round-trip. Used by InsertStatementStore.listStatements to avoid N+1.
    @Override
    public synchronized List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
      List<String> childNames = getChildNames(parentPath, options);
      List<ZNRecord> result = new ArrayList<ZNRecord>(childNames.size());
      for (String name : childNames) {
        result.add(_data.get(parentPath + "/" + name));
      }
      return result;
    }

    @Override
    public synchronized List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options,
        int retryCount, int retryInterval) {
      return getChildren(parentPath, stats, options);
    }

    @Override
    public void start() {
      /// No-op: no ZK connection to establish
    }
  }
}
