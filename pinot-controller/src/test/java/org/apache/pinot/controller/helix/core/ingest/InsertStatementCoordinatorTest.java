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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.InsertErrorCode;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Unit tests for {@link InsertStatementCoordinator}.
///
/// Tests idempotency, state transitions, hybrid table validation, and statement timeout/cleanup.
/// Uses mocks for PinotHelixResourceManager, InsertStatementStore, and InsertExecutor.
public class InsertStatementCoordinatorTest {

  private PinotHelixResourceManager _helixResourceManager;
  private InsertStatementStore _statementStore;
  private ControllerMetrics _controllerMetrics;
  private InsertStatementCoordinator _coordinator;
  private InsertExecutor _mockExecutor;

  @BeforeMethod
  public void setUp() {
    _helixResourceManager = mock(PinotHelixResourceManager.class);
    _statementStore = mock(InsertStatementStore.class);

    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(metricsRegistry);

    _coordinator = new InsertStatementCoordinator(_helixResourceManager, _statementStore, _controllerMetrics);

    _mockExecutor = mock(InsertExecutor.class);
    _coordinator.registerExecutor("ROW", _mockExecutor);
    /// submitInsert/abortStatement/etc. fail closed when not started; tests need a started coordinator.
    _coordinator.start();
  }

  @AfterMethod
  public void tearDown() {
    _coordinator.stop();
  }

  @Test
  public void testSubmitInsertSuccess() {
    /// Setup: table exists as OFFLINE only
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);
    when(_helixResourceManager.hasTable("testTable_OFFLINE")).thenReturn(true);
    when(_statementStore.createStatement(any())).thenReturn(true);

    InsertResult executorResult = new InsertResult.Builder()
        .setStatementId("stmt-1")
        .setState(InsertStatementState.ACCEPTED)
        .setMessage("Accepted")
        .build();
    when(_mockExecutor.execute(any())).thenReturn(executorResult);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-1")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getStatementId(), "stmt-1");
    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
  }

  @Test
  public void testIdempotencySamePayloadHash() {
    /// Setup: table exists as OFFLINE only
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    /// Existing manifest with same requestId and payloadHash
    InsertStatementManifest existing = new InsertStatementManifest(
        "stmt-existing", "req-1", "hash-abc", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), List.of(), null, null, null);
    /// Atomic reservation returns existing statementId (someone already reserved this requestId)
    when(_statementStore.reserveRequestId("testTable_OFFLINE", "req-1", "stmt-2"))
        .thenReturn("stmt-existing");
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-existing")).thenReturn(existing);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-2")
        .setRequestId("req-1")
        .setPayloadHash("hash-abc")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getStatementId(), "stmt-existing");
    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
  }

  @Test
  public void testIdempotencyDifferentPayloadHash() {
    /// Setup: table exists as OFFLINE only
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    InsertStatementManifest existing = new InsertStatementManifest(
        "stmt-existing", "req-1", "hash-abc", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), List.of(), null, null, null);
    /// Atomic reservation returns existing statementId
    when(_statementStore.reserveRequestId("testTable_OFFLINE", "req-1", "stmt-2"))
        .thenReturn("stmt-existing");
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-existing")).thenReturn(existing);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-2")
        .setRequestId("req-1")
        .setPayloadHash("hash-DIFFERENT")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.REJECTED);
    assertEquals(result.getErrorCode(), "IDEMPOTENCY_CONFLICT");
  }

  @Test
  public void testHybridTableRequiresExplicitType() {
    /// Setup: hybrid table (both OFFLINE and REALTIME exist)
    when(_helixResourceManager.hasOfflineTable("hybridTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("hybridTable")).thenReturn(true);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-3")
        .setTableName("hybridTable")
        .setInsertType(InsertType.ROW)
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.REJECTED);
    assertEquals(result.getErrorCode(), "TABLE_RESOLUTION_ERROR");
  }

  @Test
  public void testHybridTableWithExplicitType() {
    /// Setup: hybrid table with explicit REALTIME type
    when(_helixResourceManager.hasOfflineTable("hybridTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("hybridTable")).thenReturn(true);
    when(_helixResourceManager.hasTable("hybridTable_REALTIME")).thenReturn(true);
    when(_statementStore.createStatement(any())).thenReturn(true);

    InsertResult executorResult = new InsertResult.Builder()
        .setStatementId("stmt-4")
        .setState(InsertStatementState.ACCEPTED)
        .build();
    when(_mockExecutor.execute(any())).thenReturn(executorResult);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-4")
        .setTableName("hybridTable")
        .setTableType(TableType.REALTIME)
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
  }

  @Test
  public void testTableDoesNotExist() {
    when(_helixResourceManager.hasOfflineTable("noSuchTable")).thenReturn(false);
    when(_helixResourceManager.hasRealtimeTable("noSuchTable")).thenReturn(false);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-5")
        .setTableName("noSuchTable")
        .setInsertType(InsertType.ROW)
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertEquals(result.getState(), InsertStatementState.REJECTED);
    assertEquals(result.getErrorCode(), "TABLE_RESOLUTION_ERROR");
  }

  @Test
  public void testAbortStatement() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-8", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), List.of(), null, null, null);
    when(_statementStore.getStatement(tableNameWithType, "stmt-8")).thenReturn(manifest);
    when(_statementStore.updateStatement(any())).thenReturn(true);

    InsertResult result = _coordinator.abortStatement("stmt-8", tableNameWithType);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    verify(_mockExecutor).abort("stmt-8");
  }

  @Test
  public void testStatementTimeout() {
    /// FILE-typed manifests (not ROW) — ROW is now intentionally skipped from the timeout-abort
    /// path because its executor's success path uploads/registers segments before the coordinator
    /// CAS-flips the manifest to VISIBLE; aborting from the sweep would mis-report ABORTED for
    /// queryable data. The timeout-abort behavior is still exercised here on the FILE path.
    String tableNameWithType = "testTable_OFFLINE";
    long now = System.currentTimeMillis();
    long oldTime = now - 60 * 60 * 1000; /// 1 hour ago

    /// Create coordinator with short timeout for testing. FILE-type registration requires a
    /// FileInsertExecutor (or subclass) — see InsertStatementCoordinator.registerExecutor's v1
    /// guard. Use a Mockito mock of the concrete class so the cleanup-sweep abort path can invoke
    /// its hooks without a real Minion.
    FileInsertExecutor mockFileExecutor = mock(FileInsertExecutor.class);
    InsertStatementCoordinator coordinator =
        new InsertStatementCoordinator(_helixResourceManager, _statementStore, _controllerMetrics, 1000, 1000);
    coordinator.registerExecutor("FILE", mockFileExecutor);

    InsertStatementManifest stuckManifest = new InsertStatementManifest(
        "stmt-stuck", null, null, tableNameWithType,
        InsertType.FILE, InsertStatementState.ACCEPTED, oldTime, oldTime,
        List.of(), null, null, null);

    InsertStatementManifest activeManifest = new InsertStatementManifest(
        "stmt-active", null, null, tableNameWithType,
        InsertType.FILE, InsertStatementState.ACCEPTED, now, now,
        List.of(), null, null, null);

    when(_statementStore.listStatements(tableNameWithType))
        .thenReturn(Arrays.asList(stuckManifest, activeManifest))
        .thenReturn(List.of());  /// post-prune call sees empty list
    /// persistWithCasRetry re-reads via getStatement and writes via updateStatement
    when(_statementStore.getStatement(tableNameWithType, "stmt-stuck")).thenReturn(stuckManifest);
    when(_statementStore.getStatement(tableNameWithType, "stmt-active")).thenReturn(activeManifest);
    when(_statementStore.updateStatement(any())).thenReturn(true);

    int abortedCount = coordinator.cleanupStatementsForTable(tableNameWithType);

    assertEquals(abortedCount, 1);
    assertEquals(stuckManifest.getState(), InsertStatementState.ABORTED);
    assertEquals(activeManifest.getState(), InsertStatementState.ACCEPTED);

    coordinator.stop();
  }

  /// Verifies that ROW-typed ACCEPTED manifests are deliberately skipped from the timeout-abort
  /// cleanup. The ROW executor's success path uploads and registers segments before the
  /// coordinator's CAS persists VISIBLE; aborting from the sweep would mis-report ABORTED for
  /// data that is already queryable.
  @Test
  public void testRowAcceptedTimeoutIsNotAbortedBySweep() {
    String tableNameWithType = "testTable_OFFLINE";
    long now = System.currentTimeMillis();
    long oldTime = now - 60 * 60 * 1000;

    InsertStatementCoordinator coordinator =
        new InsertStatementCoordinator(_helixResourceManager, _statementStore, _controllerMetrics, 1000, 1000);
    coordinator.registerExecutor("ROW", _mockExecutor);

    InsertStatementManifest stuckRow = new InsertStatementManifest(
        "stmt-stuck-row", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, oldTime, oldTime,
        List.of(), null, null, null);

    when(_statementStore.listStatements(tableNameWithType))
        .thenReturn(Collections.singletonList(stuckRow))
        .thenReturn(Collections.singletonList(stuckRow));
    when(_statementStore.getStatement(tableNameWithType, "stmt-stuck-row")).thenReturn(stuckRow);

    int abortedCount = coordinator.cleanupStatementsForTable(tableNameWithType);

    assertEquals(abortedCount, 0);
    assertEquals(stuckRow.getState(), InsertStatementState.ACCEPTED);
    coordinator.stop();
  }

  @Test
  public void testGetStatus() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-9", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.VISIBLE, System.currentTimeMillis(),
        System.currentTimeMillis(), Arrays.asList("seg-a"), null, null, null);
    when(_statementStore.getStatement(tableNameWithType, "stmt-9")).thenReturn(manifest);

    InsertResult result = _coordinator.getStatus("stmt-9", tableNameWithType);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getSegmentNames(), Arrays.asList("seg-a"));
  }

  /// Regression test for the auto-complete-sweep advisory: a VISIBLE manifest whose
  /// `informationalMessage` is set (e.g., by the cleanup sweep's auto-complete path) must
  /// surface that field through `/insert/status`. Without forwarding through the
  /// {@link InsertResult} builder, operators would see `informationalMessage=null` on the wire
  /// even though the manifest carries the advisory. This test pins the forwarding contract.
  @Test
  public void testGetStatusSurfacesInformationalMessage() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-info", null, null, tableNameWithType,
        InsertType.FILE, InsertStatementState.VISIBLE, System.currentTimeMillis(),
        System.currentTimeMillis(), List.of(), null, null, null);
    manifest.setInformationalMessage("Auto-completed via cleanup sweep");
    when(_statementStore.getStatement(tableNameWithType, "stmt-info")).thenReturn(manifest);

    InsertResult result = _coordinator.getStatus("stmt-info", tableNameWithType);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getInformationalMessage(), "Auto-completed via cleanup sweep");
    /// No errorMessage was set on the manifest, so message is null.
    assertNull(result.getMessage());
  }

  /// Dual-channel forwarding: the {@link InsertResult} builder must surface both `errorMessage`
  /// and `informationalMessage` from the manifest independently. This scenario is
  /// synthetic — the auto-complete sweep currently clears `errorMessage` on every successful
  /// VISIBLE transition, so a real VISIBLE manifest carrying both fields shouldn't arise today. The
  /// test pins the forwarding contract for any future state-machine path that emits both, so a
  /// refactor that drops one of the two from the builder is caught.
  @Test
  public void testGetStatusSurfacesBothErrorAndInformationalMessage() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-dual", null, null, tableNameWithType,
        InsertType.FILE, InsertStatementState.VISIBLE, System.currentTimeMillis(),
        System.currentTimeMillis(), List.of(), "earlier transient error", null, null);
    manifest.setInformationalMessage("Auto-completed via cleanup sweep");
    when(_statementStore.getStatement(tableNameWithType, "stmt-dual")).thenReturn(manifest);

    InsertResult result = _coordinator.getStatus("stmt-dual", tableNameWithType);

    assertEquals(result.getMessage(), "earlier transient error");
    assertEquals(result.getInformationalMessage(), "Auto-completed via cleanup sweep");
  }

  @Test
  public void testGetStatusNotFound() {
    when(_statementStore.getStatement("testTable_OFFLINE", "no-such-stmt")).thenReturn(null);

    InsertResult result = _coordinator.getStatus("no-such-stmt", "testTable_OFFLINE");

    assertEquals(result.getState(), InsertStatementState.REJECTED);
    assertEquals(result.getErrorCode(), "NOT_FOUND");
  }

  @Test
  public void testListStatements() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest m1 = new InsertStatementManifest(
        "stmt-a", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.VISIBLE, 1000L, 2000L,
        Arrays.asList("seg-1"), null, null, null);
    InsertStatementManifest m2 = new InsertStatementManifest(
        "stmt-b", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, 3000L, 4000L,
        List.of(), null, null, null);
    when(_statementStore.listStatements(tableNameWithType)).thenReturn(Arrays.asList(m1, m2));

    List<InsertResult> results = _coordinator.listStatements(tableNameWithType);

    assertEquals(results.size(), 2);
    assertEquals(results.get(0).getStatementId(), "stmt-a");
    assertEquals(results.get(1).getStatementId(), "stmt-b");
  }

  @Test
  public void testNoExecutorRegistered() {
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-10")
        .setTableName("testTable")
        .setInsertType(InsertType.FILE)
        .setFileUri("s3://bucket/data.json")
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertEquals(result.getState(), InsertStatementState.REJECTED);
    assertEquals(result.getErrorCode(), "NO_EXECUTOR");
  }

  @Test
  public void testResolveTableNameWithTypeSuffix() {
    when(_helixResourceManager.hasTable("myTable_OFFLINE")).thenReturn(true);

    String resolved = _coordinator.resolveTableName("myTable_OFFLINE", null);
    assertEquals(resolved, "myTable_OFFLINE");
  }

  @Test
  public void testExecutorReceivesResolvedTableName() {
    /// Setup: only REALTIME exists, caller passes raw table name without type
    when(_helixResourceManager.hasOfflineTable("rtOnly")).thenReturn(false);
    when(_helixResourceManager.hasRealtimeTable("rtOnly")).thenReturn(true);
    when(_statementStore.createStatement(any())).thenReturn(true);

    InsertResult executorResult = new InsertResult.Builder()
        .setStatementId("stmt-rt")
        .setState(InsertStatementState.ACCEPTED)
        .build();
    when(_mockExecutor.execute(any())).thenReturn(executorResult);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-rt")
        .setTableName("rtOnly")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    _coordinator.submitInsert(request);

    /// Verify the executor received the resolved table name, not the raw one
    verify(_mockExecutor).execute(argThat(r ->
        "rtOnly_REALTIME".equals(r.getTableName()) && r.getTableType() == TableType.REALTIME));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testResolveTableNameWithTypeSuffixNotExists() {
    when(_helixResourceManager.hasTable("myTable_OFFLINE")).thenReturn(false);

    _coordinator.resolveTableName("myTable_OFFLINE", null);
  }

  /// Verifies that when execution completes as VISIBLE but ZK state persist fails, the requestId
  /// reservation is NOT released. Releasing it would allow a retry to create a fresh statement
  /// and insert duplicate data when segments may already be queryable.
  @Test
  public void testStatePersistErrorDoesNotReleaseRequestIdWhenVisible() {
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-vis"), anyString()))
        .thenReturn(null);  /// Reservation succeeds
    when(_statementStore.createStatement(any())).thenReturn(true);
    /// Both the initial update and the retry-with-fresh-read fail
    when(_statementStore.updateStatement(any())).thenReturn(false);
    when(_statementStore.getStatement(eq("testTable_OFFLINE"), anyString())).thenReturn(null);

    InsertResult executorResult = new InsertResult.Builder()
        .setStatementId("stmt-vis")
        .setState(InsertStatementState.VISIBLE)
        .setSegmentNames(Collections.singletonList("seg-1"))
        .build();
    when(_mockExecutor.execute(any())).thenReturn(executorResult);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-vis")
        .setRequestId("req-vis")
        .setPayloadHash("hash-vis")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    /// Actual executor state was VISIBLE — surfacing ABORTED would mislead callers since the data
    /// may be queryable. errorCode communicates the persist failure.
    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getErrorCode(), "STATE_PERSIST_ERROR");

    /// Critical: requestId must NOT be released when execution already succeeded.
    /// Releasing it would allow a retry to create a new statement, causing duplicate data.
    verify(_statementStore, never()).releaseRequestId(anyString(), eq("req-vis"));
  }

  /// Verifies that getStatus works without a table parameter by searching across all tables,
  /// consistent with how abortStatement behaves.
  @Test
  public void testGetStatusWithoutTableSearchesAcrossTables() {
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-cross", null, null, "someTable_OFFLINE",
        InsertType.ROW, InsertStatementState.VISIBLE, System.currentTimeMillis(),
        System.currentTimeMillis(), Collections.singletonList("seg-x"), null, null, null);
    when(_statementStore.findStatementAcrossTables("stmt-cross")).thenReturn(manifest);

    InsertResult result = _coordinator.getStatus("stmt-cross", null);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getSegmentNames(), Collections.singletonList("seg-x"));
  }

  /// Verifies that concurrent retries with the same requestId result in exactly one execution.
  /// The atomic reservation in ZK ensures only the first caller proceeds; all others get the
  /// idempotent result.
  @Test
  public void testConcurrentRetriesOnlyOneExecutes()
      throws Exception {
    /// Setup: table exists
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    /// Track how many times the executor is actually called
    AtomicInteger executorCallCount = new AtomicInteger(0);

    /// The existing manifest that the idempotent path returns
    InsertStatementManifest existingManifest = new InsertStatementManifest(
        "stmt-winner", "req-concurrent", "hash-1", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), List.of(), null, null, null);

    /// Simulate atomic reservation: first call wins (returns null), subsequent calls lose
    /// (return the winning statementId)
    AtomicInteger reserveCallCount = new AtomicInteger(0);
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-concurrent"), anyString()))
        .thenAnswer(inv -> {
          int callNum = reserveCallCount.incrementAndGet();
          if (callNum == 1) {
            return null;  /// First caller wins
          }
          return "stmt-winner";  /// All others see the existing reservation
        });
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-winner")).thenReturn(existingManifest);
    when(_statementStore.createStatement(any())).thenReturn(true);

    InsertResult executorResult = new InsertResult.Builder()
        .setStatementId("stmt-winner")
        .setState(InsertStatementState.ACCEPTED)
        .build();
    when(_mockExecutor.execute(any())).thenAnswer(inv -> {
      executorCallCount.incrementAndGet();
      return executorResult;
    });

    /// Launch concurrent submissions
    int numThreads = 5;
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    CyclicBarrier barrier = new CyclicBarrier(numThreads);

    List<Future<InsertResult>> futures = new java.util.ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      futures.add(pool.submit(() -> {
        barrier.await();
        InsertRequest request = new InsertRequest.Builder()
            .setStatementId("stmt-" + idx)
            .setRequestId("req-concurrent")
            .setPayloadHash("hash-1")
            .setTableName("testTable")
            .setInsertType(InsertType.ROW)
            .setRows(Collections.singletonList(new GenericRow()))
            .build();
        return _coordinator.submitInsert(request);
      }));
    }

    /// Collect results
    int acceptedCount = 0;
    for (Future<InsertResult> future : futures) {
      InsertResult result = future.get();
      assertNotNull(result);
      /// All results should be ACCEPTED (either the winner's result or the idempotent echo)
      assertEquals(result.getState(), InsertStatementState.ACCEPTED);
      acceptedCount++;
    }

    pool.shutdown();

    assertEquals(acceptedCount, numThreads);
    /// Only one thread should have reached the executor
    assertEquals(executorCallCount.get(), 1, "Exactly one execution should occur for concurrent retries");
  }

  /// Regression test for P2 stale requestId rebinding.
  /// When a requestId reservation exists but the associated manifest was GC'd, the coordinator
  /// must rebind the reservation to the new statementId AFTER persisting the new manifest.
  /// This prevents concurrent retries from each seeing the stale reservation and both creating
  /// independent fresh statements.
  @Test
  public void testStaleReservationIsReboundAfterManifestPersisted() {
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    /// Reservation returns a stale statementId (old statement was GC'd)
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-stale"), anyString()))
        .thenReturn("stmt-gc");
    /// The stale statement's manifest is gone
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-gc")).thenReturn(null);
    when(_statementStore.createStatement(any())).thenReturn(true);
    when(_statementStore.rebindRequestIdIfEquals(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(true);
    /// Post-create reservation re-read confirms our statementId still owns the reservation (no
    /// concurrent retry stole it after our rebind).
    when(_statementStore.peekReservedStatementId("testTable_OFFLINE", "req-stale")).thenReturn("stmt-new");

    InsertResult executorResult = new InsertResult.Builder()
        .setStatementId("stmt-new")
        .setState(InsertStatementState.ACCEPTED)
        .build();
    when(_mockExecutor.execute(any())).thenReturn(executorResult);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-new")
        .setRequestId("req-stale")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    /// The new statement should be accepted
    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
    /// rebindRequestIdIfEquals must be called with the stale expected id + new statementId
    verify(_statementStore).rebindRequestIdIfEquals("testTable_OFFLINE", "req-stale", "stmt-gc", "stmt-new");
    /// The executor must have been called (new statement was executed)
    verify(_mockExecutor).execute(any());
    /// CRITICAL: the pre-existing reservation node must NOT be deleted — we don't own it
    verify(_statementStore, never()).releaseRequestId(anyString(), anyString());
  }

  /// Regression test for the rebind race: if two concurrent callers both observe the same stale
  /// reservation, `rebindRequestIdIfEquals` fails for the loser. The loser must clean up its
  /// orphan manifest and return the winner's idempotency result, not proceed with a duplicate
  /// execution.
  @Test
  public void testSubmitInsertRebindRaceLoserReturnsWinnerResult() {
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);
    when(_helixResourceManager.hasTable("testTable_OFFLINE")).thenReturn(true);

    /// Both callers saw the same stale statementId
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-race"), anyString()))
        .thenReturn("stmt-gc");
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-gc")).thenReturn(null);
    when(_statementStore.createStatement(any())).thenReturn(true);
    /// Our rebind loses the race
    when(_statementStore.rebindRequestIdIfEquals(eq("testTable_OFFLINE"), eq("req-race"), eq("stmt-gc"),
        anyString())).thenReturn(false);
    /// The winner's manifest under a different statementId
    InsertStatementManifest winner = new InsertStatementManifest("stmt-winner", "req-race", "payload-hash",
        "testTable_OFFLINE", InsertType.ROW, InsertStatementState.ACCEPTED, 1L, 1L, List.of(),
        null, null, null);
    when(_statementStore.findByRequestId("testTable_OFFLINE", "req-race")).thenReturn(winner);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-loser")
        .setRequestId("req-race")
        .setPayloadHash("payload-hash")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    /// Must return the winner's result (idempotent)
    assertEquals(result.getStatementId(), "stmt-winner");
    /// No orphan manifest was ever created — the rebind happens BEFORE createStatement, so the
    /// loser yields without publishing any manifest. createStatement must NOT have been called.
    verify(_statementStore, never()).createStatement(any());
    verify(_statementStore, never()).deleteStatement(anyString(), anyString());
    /// Executor must NOT have been called — no duplicate execution
    verify(_mockExecutor, never()).execute(any());
  }

  /// Regression test for the rebind-vs-create residual race. After a successful rebind but before
  /// createStatement, a concurrent retry can observe our reservation, see no manifest, treat our
  /// statementId as stale, and rebind to itself. The post-create reservation re-read must detect
  /// that theft and self-rollback (delete the orphan manifest + return REBIND_RACE_LOST).
  @Test
  public void testRebindVsCreateRaceLoserSelfRollsBack() {
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);
    when(_helixResourceManager.hasTable("testTable_OFFLINE")).thenReturn(true);

    /// Reservation observed as stale, manifest GC'd
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-stale"), anyString()))
        .thenReturn("stmt-gc");
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-gc")).thenReturn(null);
    when(_statementStore.createStatement(any())).thenReturn(true);
    /// We win the rebind...
    when(_statementStore.rebindRequestIdIfEquals(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(true);
    /// ...but a concurrent retry stole the reservation between our rebind and our createStatement.
    /// The post-create peek surfaces that theft.
    when(_statementStore.peekReservedStatementId("testTable_OFFLINE", "req-stale"))
        .thenReturn("stmt-thief");

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-mine")
        .setRequestId("req-stale")
        .setPayloadHash("payload-hash")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    /// Self-rollback: REJECTED with REBIND_RACE_LOST, our orphan manifest deleted.
    assertEquals(result.getState(), InsertStatementState.REJECTED);
    assertEquals(result.getErrorCode(), InsertErrorCode.REBIND_RACE_LOST);
    verify(_statementStore).deleteStatement("testTable_OFFLINE", "stmt-mine");
    /// Executor must NOT have been called — we self-rolled back before delegation.
    verify(_mockExecutor, never()).execute(any());
    /// Reservation owned by the thief must NOT have been released by us.
    verify(_statementStore, never()).releaseRequestIdIfEquals(anyString(), anyString(), anyString());
  }

  /// Regression test for the transient-ZK-failure branch in the post-create reservation re-read.
  /// If peekReservedStatementId throws (e.g., ZK session expired), the coordinator must NOT delete
  /// the manifest — that would destroy a valid statement on a flake. The response must surface
  /// STATE_PERSIST_ERROR with the durable state (ACCEPTED) so a getStatus call from the caller
  /// agrees with the synchronous response.
  @Test
  public void testRebindVsCreatePeekTransientZkErrorLeavesManifestInPlace() {
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);
    when(_helixResourceManager.hasTable("testTable_OFFLINE")).thenReturn(true);

    /// Stale-reservation path so the post-create re-read fires.
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-stale"), anyString()))
        .thenReturn("stmt-gc");
    when(_statementStore.getStatement("testTable_OFFLINE", "stmt-gc")).thenReturn(null);
    when(_statementStore.createStatement(any())).thenReturn(true);
    when(_statementStore.rebindRequestIdIfEquals(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(true);
    /// The post-create peek throws — simulate ZK session expired / connection loss.
    when(_statementStore.peekReservedStatementId("testTable_OFFLINE", "req-stale"))
        .thenThrow(new RuntimeException("ZK session expired"));

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-mine")
        .setRequestId("req-stale")
        .setPayloadHash("payload-hash")
        .setTableName("testTable")
        .setInsertType(InsertType.ROW)
        .setRows(Collections.singletonList(new GenericRow()))
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    /// Surface durable manifest state, NOT a defaulted ABORTED.
    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
    assertEquals(result.getErrorCode(), InsertErrorCode.STATE_PERSIST_ERROR);
    /// The manifest must NOT have been deleted on a flake.
    verify(_statementStore, never()).deleteStatement(anyString(), anyString());
    /// We must not delegate to the executor in this branch — we don't know if the reservation was
    /// actually rebound, so executing would be unsafe.
    verify(_mockExecutor, never()).execute(any());
  }
}
