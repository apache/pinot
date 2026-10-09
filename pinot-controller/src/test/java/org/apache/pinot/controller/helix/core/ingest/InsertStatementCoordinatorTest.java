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

import java.util.List;
import java.util.Map;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Tests ROW validation, persistent result handling and create-only requestId behavior.
public class InsertStatementCoordinatorTest {
  private PinotHelixResourceManager _manager;
  private InsertStatementStore _store;
  private InsertExecutor _executor;
  private InsertStatementCoordinator _coordinator;

  @BeforeMethod
  public void setUp() {
    _manager = mock(PinotHelixResourceManager.class);
    _store = mock(InsertStatementStore.class);
    _executor = mock(InsertExecutor.class);
    _coordinator = new InsertStatementCoordinator(_manager, _store, mock(ControllerMetrics.class));
    _coordinator.registerExecutor("ROW", _executor);
    _coordinator.start();
    when(_manager.hasOfflineTable("t")).thenReturn(true);
    when(_manager.hasTable("t_OFFLINE")).thenReturn(true);
    when(_store.createStatement(any())).thenReturn(true);
    when(_store.updateStatement(any())).thenReturn(true);
    when(_executor.execute(any())).thenReturn(result(InsertStatementState.VISIBLE, null));
  }

  private static InsertRequest request(String statementId, String requestId) {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);
    return new InsertRequest.Builder().setStatementId(statementId).setRequestId(requestId)
        .setTableName("t").setInsertType(InsertType.ROW).setRows(List.of(row)).build();
  }

  private static InsertResult result(InsertStatementState state, String errorCode) {
    return new InsertResult.Builder().setStatementId("s1").setState(state).setErrorCode(errorCode)
        .setSegmentNames(List.of("segment1")).build();
  }

  private static InsertStatementManifest manifest(InsertRequest request, InsertStatementState state) {
    return new InsertStatementManifest("s1", "r1", request.computePayloadHash(), "t_OFFLINE", InsertType.ROW,
        state, 1, 1, List.of("segment1"), "failed", InsertErrorCode.SEGMENT_UPLOAD_FAILED_PARTIAL);
  }

  @Test
  public void testSuccessPersistsResult() {
    assertEquals(_coordinator.submitInsert(request("s1", "r1")).getState(), InsertStatementState.VISIBLE);
    verify(_store).reserveRequestId("t_OFFLINE", "r1", "s1");
    verify(_store).updateStatement(any());
    verify(_executor).execute(any());
  }

  @Test
  public void testRetryReturnsOriginalFailedResultWithoutExecuting() {
    InsertRequest request = request("s2", "r1");
    when(_store.reserveRequestId("t_OFFLINE", "r1", "s2")).thenReturn("s1");
    when(_store.getStatement("t_OFFLINE", "s1"))
        .thenReturn(manifest(request, InsertStatementState.ABORTED));
    InsertResult result = _coordinator.submitInsert(request);
    assertEquals(result.getStatementId(), "s1");
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), InsertErrorCode.SEGMENT_UPLOAD_FAILED_PARTIAL);
    assertEquals(result.getSegmentNames(), List.of("segment1"));
    verify(_executor, never()).execute(any());
    verify(_store, never()).createStatement(any());
  }

  @Test
  public void testMissingReservedManifestFailsClosed() {
    when(_store.reserveRequestId("t_OFFLINE", "r1", "s2")).thenReturn("s1");
    assertEquals(_coordinator.submitInsert(request("s2", "r1")).getErrorCode(), InsertErrorCode.IDEMPOTENCY_ERROR);
    verify(_executor, never()).execute(any());
    verify(_store, never()).createStatement(any());
  }

  @Test
  public void testPayloadMismatchRejectsRetry() {
    InsertRequest request = request("s2", "r1");
    when(_store.reserveRequestId("t_OFFLINE", "r1", "s2")).thenReturn("s1");
    InsertStatementManifest prior = new InsertStatementManifest("s1", "r1", "different", "t_OFFLINE",
        InsertType.ROW, InsertStatementState.VISIBLE, 1, 1, List.of("segment1"), null, null);
    when(_store.getStatement("t_OFFLINE", "s1")).thenReturn(prior);
    assertEquals(_coordinator.submitInsert(request).getErrorCode(), InsertErrorCode.IDEMPOTENCY_CONFLICT);
    verify(_executor, never()).execute(any());
  }

  @Test
  public void testReservationFailureDoesNotExecute() {
    when(_store.reserveRequestId(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("ZK down"));
    assertEquals(_coordinator.submitInsert(request("s1", "r1")).getErrorCode(), InsertErrorCode.IDEMPOTENCY_ERROR);
    verify(_executor, never()).execute(any());
  }

  @Test
  public void testManifestFailureDoesNotExecute() {
    when(_store.createStatement(any())).thenReturn(false);
    assertEquals(_coordinator.submitInsert(request("s1", "r1")).getErrorCode(), InsertErrorCode.STORE_ERROR);
    verify(_executor, never()).execute(any());
  }

  @Test
  public void testResultPersistFailureDoesNotRepeatExecution() {
    when(_store.updateStatement(any())).thenReturn(false);
    InsertResult result = _coordinator.submitInsert(request("s1", "r1"));
    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getErrorCode(), InsertErrorCode.STATE_PERSIST_ERROR);
    verify(_executor).execute(any());
  }

  @Test
  public void testExecutorFailurePersistsFailureManifest() {
    when(_executor.execute(any())).thenThrow(new RuntimeException("upload failed"));
    InsertResult result = _coordinator.submitInsert(request("s1", "r1"));
    assertEquals(result.getState(), InsertStatementState.ABORTED);
    assertEquals(result.getErrorCode(), InsertErrorCode.EXECUTOR_ERROR);
    verify(_store).updateStatement(any());
  }

  @Test
  public void testHybridTableRequiresExplicitType() {
    when(_manager.hasRealtimeTable("t")).thenReturn(true);
    assertEquals(_coordinator.submitInsert(request("s1", "r1")).getErrorCode(),
        InsertErrorCode.TABLE_RESOLUTION_ERROR);
    assertEquals(_coordinator.resolveTableName("t", TableType.REALTIME), "t_REALTIME");
    verify(_store, never()).reserveRequestId(anyString(), anyString(), anyString());
  }

  @Test
  public void testRowLimitAndDisabledCoordinatorRejectBeforeReservation() {
    _coordinator = new InsertStatementCoordinator(_manager, _store, mock(ControllerMetrics.class), 0, 100);
    _coordinator.registerExecutor("ROW", _executor);
    _coordinator.start();
    assertEquals(_coordinator.submitInsert(request("s1", "r1")).getErrorCode(), InsertErrorCode.ROW_LIMIT_EXCEEDED);
    _coordinator.stop();
    assertEquals(_coordinator.submitInsert(request("s1", "r1")).getErrorCode(),
        InsertErrorCode.COORDINATOR_NOT_READY);
    verify(_store, never()).reserveRequestId(anyString(), anyString(), anyString());
  }

  @Test
  public void testStatusAndListReturnPersistedError() {
    InsertStatementManifest prior = manifest(request("s1", "r1"), InsertStatementState.ABORTED);
    when(_store.getStatement("t_OFFLINE", "s1")).thenReturn(prior);
    when(_store.listStatements("t_OFFLINE")).thenReturn(List.of(prior));
    assertNotNull(_coordinator.getStatus("s1", "t_OFFLINE"));
    assertEquals(_coordinator.listStatements("t_OFFLINE").get(0).getErrorCode(),
        InsertErrorCode.SEGMENT_UPLOAD_FAILED_PARTIAL);
    assertEquals(_coordinator.getStatus("missing", "t_OFFLINE").getErrorCode(), InsertErrorCode.NOT_FOUND);
  }
  @Test
  public void testLargeMultiValueAndNestedPayloadsRejectBeforeReservation() {
    _coordinator = new InsertStatementCoordinator(_manager, _store, mock(ControllerMetrics.class), 10, 100);
    _coordinator.registerExecutor("ROW", _executor);
    _coordinator.start();
    for (Object value : List.of(List.of("a".repeat(100)), Map.of("nested", List.of("b".repeat(100))),
        new String[]{"c".repeat(100)})) {
      GenericRow row = new GenericRow();
      row.putValue("value", value);
      InsertRequest request = new InsertRequest.Builder().setStatementId("s1").setRequestId("r1")
          .setTableName("t").setInsertType(InsertType.ROW).setRows(List.of(row)).build();
      assertEquals(_coordinator.submitInsert(request).getErrorCode(), InsertErrorCode.PAYLOAD_TOO_LARGE);
    }
    verify(_store, never()).reserveRequestId(anyString(), anyString(), anyString());
    verify(_executor, never()).execute(any());
  }
}
