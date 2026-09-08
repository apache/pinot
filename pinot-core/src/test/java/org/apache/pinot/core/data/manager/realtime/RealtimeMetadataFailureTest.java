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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.LogicalTableExecutionInfo;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.executor.SingleTableExecutionInfo;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TableSegmentsContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the table failure boundary after metadata removal throws, including concurrent consumer admission.
public class RealtimeMetadataFailureTest {
  private static final String TABLE_NAME = "testTable_REALTIME";
  private static final LLCSegmentName FAILED_SEGMENT = new LLCSegmentName("testTable", 0, 0, 0);
  private static final LLCSegmentName NEXT_SEGMENT = new LLCSegmentName("testTable", 0, 1, 0);

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @Test
  public void testWaitingConsumerRechecksMetadataAfterAcquisition()
      throws Exception {
    TestTableDataManager table = spy(new TestTableDataManager());
    ConsumerCoordinator coordinator = new ConsumerCoordinator(false, table);
    assertTrue(coordinator.getSemaphore().tryAcquire());
    CountDownLatch checkedBeforeAcquire = new CountDownLatch(1);
    CountDownLatch continueAcquire = new CountDownLatch(1);
    AtomicBoolean firstCheck = new AtomicBoolean(true);
    doAnswer(invocation -> {
      invocation.callRealMethod();
      if (firstCheck.getAndSet(false)) {
        checkedBeforeAcquire.countDown();
        assertTrue(continueAcquire.await(5, TimeUnit.SECONDS));
      }
      return null;
    }).when(table).checkMetadataHealthy();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> successor = executor.submit(() -> {
        coordinator.acquire(NEXT_SEGMENT);
        return null;
      });
      assertTrue(checkedBeforeAcquire.await(5, TimeUnit.SECONDS));
      table.onSegmentMetadataRemovalFailure(FAILED_SEGMENT.getSegmentName());
      coordinator.release();
      continueAcquire.countDown();

      ExecutionException exception =
          expectThrows(ExecutionException.class, () -> successor.get(5, TimeUnit.SECONDS));
      assertTrue(exception.getCause() instanceof IllegalStateException);
      assertTrue(exception.getCause().getMessage().contains(FAILED_SEGMENT.getSegmentName()));
      assertEquals(coordinator.getSemaphore().availablePermits(), 1,
          "Rejected acquisition must return its permit");
    } finally {
      continueAcquire.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFailedMetadataRejectsNewConsumersBeforeLoadingSegments()
      throws Exception {
    TestTableDataManager table = new TestTableDataManager();
    table.onSegmentMetadataRemovalFailure(FAILED_SEGMENT.getSegmentName());
    // There is no segment/ZK setup: the failure must be checked before loading or allocating the next segment.
    expectThrows(IllegalStateException.class, () -> table.addConsumingSegment(NEXT_SEGMENT.getSegmentName()));
    ConsumerCoordinator coordinator = new ConsumerCoordinator(true, table);
    expectThrows(IllegalStateException.class, () -> coordinator.acquire(NEXT_SEGMENT));
    coordinator.register(FAILED_SEGMENT);
    expectThrows(IllegalStateException.class, () -> coordinator.acquire(NEXT_SEGMENT));
    assertEquals(coordinator.getSemaphore().availablePermits(), 1);

    TestTableDataManager healthyTable = new TestTableDataManager();
    ConsumerCoordinator healthyCoordinator = new ConsumerCoordinator(false, healthyTable);
    healthyCoordinator.acquire(NEXT_SEGMENT);
    healthyCoordinator.release();
  }

  @DataProvider
  public Object[][] queryModes() {
    return new Object[][]{
        {UpsertConfig.Mode.NONE, UpsertConfig.ConsistencyMode.NONE},
        {UpsertConfig.Mode.FULL, UpsertConfig.ConsistencyMode.NONE},
        {UpsertConfig.Mode.FULL, UpsertConfig.ConsistencyMode.SYNC},
        {UpsertConfig.Mode.FULL, UpsertConfig.ConsistencyMode.SNAPSHOT},
        {UpsertConfig.Mode.PARTIAL, UpsertConfig.ConsistencyMode.NONE},
        {UpsertConfig.Mode.PARTIAL, UpsertConfig.ConsistencyMode.SYNC},
        {UpsertConfig.Mode.PARTIAL, UpsertConfig.ConsistencyMode.SNAPSHOT}
    };
  }

  @Test(dataProvider = "queryModes")
  public void testFailedMetadataRejectsQueryInsteadOfReturningPartialView(UpsertConfig.Mode mode,
      UpsertConfig.ConsistencyMode consistencyMode)
      throws Exception {
    TestTableDataManager table = new TestTableDataManager();
    table.setUpsertMode(mode, consistencyMode);
    SegmentDataManager segment = mock(SegmentDataManager.class);
    table.addSegment(NEXT_SEGMENT.getSegmentName(), segment);
    table.onSegmentMetadataRemovalFailure(FAILED_SEGMENT.getSegmentName());
    InstanceDataManager instance = mock(InstanceDataManager.class);
    when(instance.getTableDataManager(TABLE_NAME)).thenReturn(table);

    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> SingleTableExecutionInfo.create(instance, TABLE_NAME, List.of(NEXT_SEGMENT.getSegmentName()), null,
            mock(QueryContext.class)));
    assertTrue(exception.getMessage().contains(FAILED_SEGMENT.getSegmentName()));
    verify(segment, never()).increaseReferenceCount();
    List<String> missingSegments = new ArrayList<>();
    expectThrows(IllegalStateException.class,
        () -> table.acquireSegments(List.of(NEXT_SEGMENT.getSegmentName()), missingSegments));
    assertTrue(missingSegments.isEmpty(), "Metadata failure must not be reported as a missing segment");
  }

  @Test
  public void testFailedLogicalTableQueryReleasesEarlierTables() {
    TestTableDataManager healthyTable = new TestTableDataManager();
    SegmentDataManager segment = mock(SegmentDataManager.class);
    when(segment.increaseReferenceCount()).thenReturn(true);
    healthyTable.addSegment("healthySegment", segment);
    TestTableDataManager failedTable = new TestTableDataManager();
    failedTable.onSegmentMetadataRemovalFailure(FAILED_SEGMENT.getSegmentName());
    InstanceDataManager instance = mock(InstanceDataManager.class);
    when(instance.getTableDataManager("healthyTable_REALTIME")).thenReturn(healthyTable);
    when(instance.getTableDataManager(TABLE_NAME)).thenReturn(failedTable);
    ServerQueryRequest request = mock(ServerQueryRequest.class);
    when(request.getTableSegmentsContexts()).thenReturn(List.of(
        new TableSegmentsContext("healthyTable_REALTIME", List.of("healthySegment"), null),
        new TableSegmentsContext(TABLE_NAME, List.of(NEXT_SEGMENT.getSegmentName()), null)));

    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> LogicalTableExecutionInfo.create(instance, request, mock(QueryContext.class)));
    assertTrue(exception.getMessage().contains(FAILED_SEGMENT.getSegmentName()));
    verify(segment).increaseReferenceCount();
    verify(segment).decreaseReferenceCount();
  }

  @Test
  public void testFailedMetadataReturnsQueryErrorBlock()
      throws Exception {
    TestTableDataManager table = new TestTableDataManager();
    table.onSegmentMetadataRemovalFailure(FAILED_SEGMENT.getSegmentName());
    InstanceDataManager instance = mock(InstanceDataManager.class);
    when(instance.getTableDataManager(TABLE_NAME)).thenReturn(table);
    ServerQueryExecutorV1Impl executor = new ServerQueryExecutorV1Impl();
    executor.init(new PinotConfiguration(), instance, ServerMetrics.get());
    ServerQueryRequest request = mock(ServerQueryRequest.class);
    when(request.getTableNameWithType()).thenReturn(TABLE_NAME);
    when(request.getSegmentsToQuery()).thenReturn(List.of(NEXT_SEGMENT.getSegmentName()));
    when(request.getQueryContext()).thenReturn(
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM " + TABLE_NAME));
    when(request.getTimerContext()).thenReturn(new TimerContext(TABLE_NAME, ServerMetrics.get(),
        System.currentTimeMillis()));

    InstanceResponseBlock response = executor.execute(request, mock(ExecutorService.class), null);
    assertEquals(response.getExceptions().size(), 1);
    assertTrue(response.getExceptions().get(QueryErrorCode.QUERY_EXECUTION.getId())
        .contains(FAILED_SEGMENT.getSegmentName()));
    assertNull(response.getResultsBlock());
  }

  private static class TestTableDataManager extends RealtimeTableDataManager {
    TestTableDataManager() {
      super(null);
      _tableNameWithType = TABLE_NAME;
      _logger = LoggerFactory.getLogger(TestTableDataManager.class);
      _recentlyDeletedSegments = CacheBuilder.newBuilder().build();
    }

    @Override
    public StreamIngestionConfig getStreamIngestionConfig() {
      return null;
    }

    void setUpsertMode(UpsertConfig.Mode mode, UpsertConfig.ConsistencyMode consistencyMode) {
      if (mode != UpsertConfig.Mode.NONE) {
        _tableUpsertMetadataManager = mock(TableUpsertMetadataManager.class);
        UpsertContext context = mock(UpsertContext.class);
        when(context.getUpsertMode()).thenReturn(mode);
        when(context.getConsistencyMode()).thenReturn(consistencyMode);
        when(_tableUpsertMetadataManager.getContext()).thenReturn(context);
        when(_tableUpsertMetadataManager.getNewlyAddedSegments()).thenReturn(Set.of());
      }
    }

    void addSegment(String name, SegmentDataManager segment) {
      _segmentDataManagerMap.put(name, segment);
    }
  }
}
