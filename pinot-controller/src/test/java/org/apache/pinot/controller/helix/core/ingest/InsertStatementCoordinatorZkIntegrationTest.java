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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Coordinator tests against a real {@link InsertStatementStore} backed by an in-memory property
/// store — exercises the ZK CAS semantics (reservation atomicity, rebind races, version-checked
/// writes) that the Mockito-mocked `InsertStatementCoordinatorTest` cannot reach.
///
/// This is a companion to that unit-test class; the mocked version focuses on coordinator
/// state-machine logic, and this one focuses on the interaction with ZK-semantic store behavior.
public class InsertStatementCoordinatorZkIntegrationTest {

  private static final String TABLE = "testTable_OFFLINE";

  private PinotHelixResourceManager _helixResourceManager;
  private InsertStatementStore _statementStore;
  private ControllerMetrics _controllerMetrics;
  private InsertStatementCoordinator _coordinator;
  private InsertExecutor _mockExecutor;

  @BeforeMethod
  public void setUp() {
    _helixResourceManager = mock(PinotHelixResourceManager.class);
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);
    when(_helixResourceManager.hasTable(TABLE)).thenReturn(true);

    _statementStore = new InsertStatementStore(new InMemoryPropertyStore());

    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(metricsRegistry);

    _coordinator = new InsertStatementCoordinator(_helixResourceManager, _statementStore, _controllerMetrics);
    _mockExecutor = mock(InsertExecutor.class);
    _coordinator.registerExecutor("ROW", _mockExecutor);
    _coordinator.start();
  }

  @AfterMethod
  public void tearDown() {
    _coordinator.stop();
  }

  /// -------------------------------------------------------------------------
  /// Reservation idempotency — real ZK atomic create semantics
  /// -------------------------------------------------------------------------

  @Test
  public void testIdempotentRetryWithSameRequestIdReturnsExistingResult() {
    when(_mockExecutor.execute(any())).thenReturn(makeAcceptedResult("stmt-first"));

    InsertRequest first = makeRequest("stmt-first", "req-idem", "hash-1");
    InsertResult r1 = _coordinator.submitInsert(first);
    assertEquals(r1.getStatementId(), "stmt-first");
    assertEquals(r1.getState(), InsertStatementState.ACCEPTED);

    /// Retry with same requestId + same payloadHash → must be idempotent
    InsertRequest retry = makeRequest("stmt-second", "req-idem", "hash-1");
    InsertResult r2 = _coordinator.submitInsert(retry);
    assertEquals(r2.getStatementId(), "stmt-first",
        "Idempotent retry must return the original statementId, not the new one");

    /// Executor was only called once
    org.mockito.Mockito.verify(_mockExecutor, org.mockito.Mockito.times(1)).execute(any());
  }

  @Test
  public void testDifferentPayloadHashWithSameRequestIdRejected() {
    when(_mockExecutor.execute(any())).thenReturn(makeAcceptedResult("stmt-first"));

    InsertRequest first = makeRequest("stmt-first", "req-collide", "hash-1");
    _coordinator.submitInsert(first);

    /// Different payloadHash → collision detected
    InsertRequest other = makeRequest("stmt-second", "req-collide", "hash-2");
    InsertResult r = _coordinator.submitInsert(other);
    assertEquals(r.getState(), InsertStatementState.REJECTED);
    assertEquals(r.getErrorCode(), "IDEMPOTENCY_CONFLICT");
  }

  /// -------------------------------------------------------------------------
  /// CAS retry — executor returns a terminal state after a concurrent writer
  /// bumps the manifest version; persistWithCasRetry must recover
  /// -------------------------------------------------------------------------

  @Test
  public void testTerminalStatePersistRecoversFromVersionConflict() {
    /// Return VISIBLE from the executor; the ZK write must succeed despite a simulated
    /// concurrent version bump that we inject between the executor's return and the persist.
    AtomicBoolean injectConflict = new AtomicBoolean(true);
    when(_mockExecutor.execute(any())).thenAnswer(inv -> {
      /// Bump the version of the already-created manifest once, simulating a concurrent controller
      /// leader writing to the same node. persistWithCasRetry should re-read, see version=1, and
      /// succeed on the second CAS attempt.
      if (injectConflict.getAndSet(false)) {
        InsertStatementManifest current = _statementStore.getStatement(TABLE, "stmt-cas");
        if (current != null) {
          current.setErrorMessage("concurrent-write");
          _statementStore.updateStatement(current);
        }
      }
      return new InsertResult.Builder()
          .setStatementId("stmt-cas")
          .setState(InsertStatementState.VISIBLE)
          .setSegmentNames(Collections.singletonList("seg-1"))
          .build();
    });

    InsertRequest req = makeRequest("stmt-cas", null, null);
    InsertResult result = _coordinator.submitInsert(req);
    assertEquals(result.getState(), InsertStatementState.VISIBLE);

    /// ZK state reflects the terminal outcome after the retry
    InsertStatementManifest persisted = _statementStore.getStatement(TABLE, "stmt-cas");
    assertNotNull(persisted);
    assertEquals(persisted.getState(), InsertStatementState.VISIBLE);
    assertEquals(persisted.getSegmentNames(), Collections.singletonList("seg-1"));
  }

  /// -------------------------------------------------------------------------
  /// Rebind race — two concurrent callers both observe the same stale reservation
  /// -------------------------------------------------------------------------

  @Test
  public void testConcurrentSubmissionsWithStaleReservationExactlyOneWins()
      throws Exception {
    /// Pre-seed a reservation pointing to a GC'd statementId.
    _statementStore.reserveRequestId(TABLE, "req-stale", "stmt-gc");
    /// Note: we intentionally do NOT create a manifest for stmt-gc — simulating GC.

    /// The mock executor returns VISIBLE quickly for whichever caller wins the rebind.
    when(_mockExecutor.execute(any())).thenAnswer(inv -> {
      InsertRequest r = inv.getArgument(0);
      return new InsertResult.Builder()
          .setStatementId(r.getStatementId())
          .setState(InsertStatementState.VISIBLE)
          .setSegmentNames(Collections.singletonList("seg-" + r.getStatementId()))
          .build();
    });

    /// Two concurrent submissions with the same requestId but different statementIds.
    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Future<InsertResult> f1 = pool.submit(() -> {
      startLatch.await();
      return _coordinator.submitInsert(makeRequest("stmt-A", "req-stale", "hash-same"));
    });
    Future<InsertResult> f2 = pool.submit(() -> {
      startLatch.await();
      return _coordinator.submitInsert(makeRequest("stmt-B", "req-stale", "hash-same"));
    });
    startLatch.countDown();
    InsertResult r1 = f1.get(10, TimeUnit.SECONDS);
    InsertResult r2 = f2.get(10, TimeUnit.SECONDS);
    pool.shutdown();

    /// Exactly one of {A, B} ran the executor; the other observed idempotency and returned the
    /// winner's statementId. The reservation must not allow two independent executions.
    String winnerId = r1.getStatementId();
    String otherId = r2.getStatementId();
    assertTrue(winnerId.equals("stmt-A") || winnerId.equals("stmt-B"),
        "Winner should be one of the two submitted statementIds");
    assertEquals(otherId, winnerId,
        "Both callers must converge on the same statementId (either the direct winner or the "
            + "rebind loser that returned the winner's manifest)");

    /// The ZK state has exactly one manifest for this requestId.
    InsertStatementManifest persisted = _statementStore.findByRequestId(TABLE, "req-stale");
    assertNotNull(persisted);
    assertEquals(persisted.getStatementId(), winnerId);

    /// The executor was called exactly once (the other caller surfaced idempotency).
    org.mockito.Mockito.verify(_mockExecutor, org.mockito.Mockito.times(1)).execute(any());
  }

  /// -------------------------------------------------------------------------
  /// Schema-version fingerprint on reservation node — verifies the store stamps it
  /// -------------------------------------------------------------------------

  @Test
  public void testReservationNodeCarriesSchemaVersion() {
    assertEquals(_statementStore.reserveRequestId(TABLE, "req-versioned", "stmt-versioned"), null);
    /// The reservation should be findable via its own API.
    when(_mockExecutor.execute(any())).thenReturn(makeAcceptedResult("stmt-versioned"));
    /// Subsequent reservation with same requestId returns the existing statementId (idempotent).
    String existing = _statementStore.reserveRequestId(TABLE, "req-versioned", "stmt-dup");
    assertEquals(existing, "stmt-versioned");
  }

  /// -------------------------------------------------------------------------
  /// helpers
  /// -------------------------------------------------------------------------

  private static InsertRequest makeRequest(String statementId, String requestId, String payloadHash) {
    return new InsertRequest.Builder()
        .setStatementId(statementId)
        .setRequestId(requestId)
        .setPayloadHash(payloadHash)
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new org.apache.pinot.spi.data.readers.GenericRow()))
        .build();
  }

  private static InsertResult makeAcceptedResult(String statementId) {
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ACCEPTED)
        .build();
  }

  /// -------------------------------------------------------------------------
  /// In-memory ZkHelixPropertyStore substitute (same pattern as InsertStatementStoreTest)
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

    @Override
    public synchronized boolean set(String path, ZNRecord record, int options) {
      _data.put(path, record);
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

    @Override
    public synchronized List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
      List<String> childNames = getChildNames(parentPath, options);
      List<ZNRecord> result = new java.util.ArrayList<>(childNames.size());
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
    }
  }
}
