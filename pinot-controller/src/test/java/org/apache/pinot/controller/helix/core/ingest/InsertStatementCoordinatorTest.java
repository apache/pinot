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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Unit tests for {@link InsertStatementCoordinator}.
 *
 * <p>Tests idempotency, state transitions, hybrid table validation, and statement timeout/cleanup.
 * Uses mocks for PinotHelixResourceManager, InsertStatementStore, and InsertExecutor.
 */
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
  }

  @AfterMethod
  public void tearDown() {
    _coordinator.stop();
  }

  @Test
  public void testSubmitInsertSuccess() {
    // Setup: table exists as OFFLINE only
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
        .setRows(Collections.emptyList())
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getStatementId(), "stmt-1");
    assertEquals(result.getState(), InsertStatementState.ACCEPTED);
  }

  @Test
  public void testIdempotencySamePayloadHash() {
    // Setup: table exists as OFFLINE only
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    // Existing manifest with same requestId and payloadHash
    InsertStatementManifest existing = new InsertStatementManifest(
        "stmt-existing", "req-1", "hash-abc", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), Collections.emptyList(), null, null);
    // Atomic reservation returns existing statementId (someone already reserved this requestId)
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
    // Setup: table exists as OFFLINE only
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    InsertStatementManifest existing = new InsertStatementManifest(
        "stmt-existing", "req-1", "hash-abc", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), Collections.emptyList(), null, null);
    // Atomic reservation returns existing statementId
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
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "IDEMPOTENCY_CONFLICT");
  }

  @Test
  public void testHybridTableRequiresExplicitType() {
    // Setup: hybrid table (both OFFLINE and REALTIME exist)
    when(_helixResourceManager.hasOfflineTable("hybridTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("hybridTable")).thenReturn(true);

    InsertRequest request = new InsertRequest.Builder()
        .setStatementId("stmt-3")
        .setTableName("hybridTable")
        .setInsertType(InsertType.ROW)
        .build();

    InsertResult result = _coordinator.submitInsert(request);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TABLE_RESOLUTION_ERROR");
  }

  @Test
  public void testHybridTableWithExplicitType() {
    // Setup: hybrid table with explicit REALTIME type
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

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "TABLE_RESOLUTION_ERROR");
  }

  @Test
  public void testStateTransitionCommit() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-6", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.PREPARED, System.currentTimeMillis(),
        System.currentTimeMillis(), Arrays.asList("seg-1", "seg-2"), null, null);
    when(_statementStore.getStatement(tableNameWithType, "stmt-6")).thenReturn(manifest);
    when(_statementStore.updateStatement(any())).thenReturn(true);

    InsertResult result = _coordinator.commitStatement("stmt-6", tableNameWithType);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.COMMITTED);
    assertEquals(result.getSegmentNames(), Arrays.asList("seg-1", "seg-2"));
  }

  @Test
  public void testCannotCommitFromWrongState() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-7", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), Collections.emptyList(), null, null);
    when(_statementStore.getStatement(tableNameWithType, "stmt-7")).thenReturn(manifest);

    InsertResult result = _coordinator.commitStatement("stmt-7", tableNameWithType);

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "INVALID_STATE");
  }

  @Test
  public void testAbortStatement() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-8", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), Collections.emptyList(), null, null);
    when(_statementStore.getStatement(tableNameWithType, "stmt-8")).thenReturn(manifest);
    when(_statementStore.updateStatement(any())).thenReturn(true);

    InsertResult result = _coordinator.abortStatement("stmt-8", tableNameWithType);

    assertNotNull(result);
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    verify(_mockExecutor).abort("stmt-8");
  }

  @Test
  public void testStatementTimeout() {
    String tableNameWithType = "testTable_OFFLINE";
    long now = System.currentTimeMillis();
    long oldTime = now - 60 * 60 * 1000; // 1 hour ago

    // Create coordinator with short timeout for testing
    InsertStatementCoordinator coordinator =
        new InsertStatementCoordinator(_helixResourceManager, _statementStore, _controllerMetrics, 1000, 1000, 1000);
    coordinator.registerExecutor("ROW", _mockExecutor);

    InsertStatementManifest stuckManifest = new InsertStatementManifest(
        "stmt-stuck", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, oldTime, oldTime,
        Collections.emptyList(), null, null);

    InsertStatementManifest activeManifest = new InsertStatementManifest(
        "stmt-active", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, now, now,
        Collections.emptyList(), null, null);

    when(_statementStore.listStatements(tableNameWithType))
        .thenReturn(Arrays.asList(stuckManifest, activeManifest));
    when(_statementStore.updateStatement(any())).thenReturn(true);

    int abortedCount = coordinator.cleanupStatementsForTable(tableNameWithType);

    assertEquals(abortedCount, 1);
    assertEquals(stuckManifest.getState(), InsertStatementState.ABORTED);
    assertEquals(activeManifest.getState(), InsertStatementState.ACCEPTED);

    coordinator.stop();
  }

  @Test
  public void testGetStatus() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-9", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.VISIBLE, System.currentTimeMillis(),
        System.currentTimeMillis(), Arrays.asList("seg-a"), null, null);
    when(_statementStore.getStatement(tableNameWithType, "stmt-9")).thenReturn(manifest);

    InsertResult result = _coordinator.getStatus("stmt-9", tableNameWithType);

    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getSegmentNames(), Arrays.asList("seg-a"));
  }

  @Test
  public void testGetStatusNotFound() {
    when(_statementStore.getStatement("testTable_OFFLINE", "no-such-stmt")).thenReturn(null);

    InsertResult result = _coordinator.getStatus("no-such-stmt", "testTable_OFFLINE");

    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), "NOT_FOUND");
  }

  @Test
  public void testListStatements() {
    String tableNameWithType = "testTable_OFFLINE";
    InsertStatementManifest m1 = new InsertStatementManifest(
        "stmt-a", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.VISIBLE, 1000L, 2000L,
        Arrays.asList("seg-1"), null, null);
    InsertStatementManifest m2 = new InsertStatementManifest(
        "stmt-b", null, null, tableNameWithType,
        InsertType.ROW, InsertStatementState.ACCEPTED, 3000L, 4000L,
        Collections.emptyList(), null, null);
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

    assertEquals(result.getState(), InsertStatementState.ABORTED);
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
    // Setup: only REALTIME exists, caller passes raw table name without type
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
        .setRows(Collections.emptyList())
        .build();

    _coordinator.submitInsert(request);

    // Verify the executor received the resolved table name, not the raw one
    verify(_mockExecutor).execute(argThat(r ->
        "rtOnly_REALTIME".equals(r.getTableName()) && r.getTableType() == TableType.REALTIME));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testResolveTableNameWithTypeSuffixNotExists() {
    when(_helixResourceManager.hasTable("myTable_OFFLINE")).thenReturn(false);

    _coordinator.resolveTableName("myTable_OFFLINE", null);
  }

  /**
   * Verifies that concurrent retries with the same requestId result in exactly one execution.
   * The atomic reservation in ZK ensures only the first caller proceeds; all others get the
   * idempotent result.
   */
  @Test
  public void testConcurrentRetriesOnlyOneExecutes()
      throws Exception {
    // Setup: table exists
    when(_helixResourceManager.hasOfflineTable("testTable")).thenReturn(true);
    when(_helixResourceManager.hasRealtimeTable("testTable")).thenReturn(false);

    // Track how many times the executor is actually called
    AtomicInteger executorCallCount = new AtomicInteger(0);

    // The existing manifest that the idempotent path returns
    InsertStatementManifest existingManifest = new InsertStatementManifest(
        "stmt-winner", "req-concurrent", "hash-1", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED, System.currentTimeMillis(),
        System.currentTimeMillis(), Collections.emptyList(), null, null);

    // Simulate atomic reservation: first call wins (returns null), subsequent calls lose
    // (return the winning statementId)
    AtomicInteger reserveCallCount = new AtomicInteger(0);
    when(_statementStore.reserveRequestId(eq("testTable_OFFLINE"), eq("req-concurrent"), anyString()))
        .thenAnswer(inv -> {
          int callNum = reserveCallCount.incrementAndGet();
          if (callNum == 1) {
            return null;  // First caller wins
          }
          return "stmt-winner";  // All others see the existing reservation
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

    // Launch concurrent submissions
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
            .setRows(Collections.emptyList())
            .build();
        return _coordinator.submitInsert(request);
      }));
    }

    // Collect results
    int acceptedCount = 0;
    for (Future<InsertResult> future : futures) {
      InsertResult result = future.get();
      assertNotNull(result);
      // All results should be ACCEPTED (either the winner's result or the idempotent echo)
      assertEquals(result.getState(), InsertStatementState.ACCEPTED);
      acceptedCount++;
    }

    pool.shutdown();

    assertEquals(acceptedCount, numThreads);
    // Only one thread should have reached the executor
    assertEquals(executorCallCount.get(), 1, "Exactly one execution should occur for concurrent retries");
  }
}
