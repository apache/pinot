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
package org.apache.pinot.query.service.dispatch;

import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.executor.OpChainCompletionListener;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.service.dispatch.streaming.StreamingQuerySession;
import org.apache.pinot.query.service.server.QueryServer;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class QueryDispatcherTest extends QueryTestSet {
  private static final AtomicLong REQUEST_ID_GEN = new AtomicLong();
  private static final int QUERY_SERVER_COUNT = 2;

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private final Map<Integer, QueryRunner> _queryRunnerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;
  private QueryDispatcher _queryDispatcher;

  @Test
  public void testMaterializedFailureCleansCompletedServers()
      throws Exception {
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    CountDownLatch cancelled = new CountDownLatch(_queryServerMap.size());
    for (QueryServer server : _queryServerMap.values()) {
      doAnswer(invocation -> {
        invocation.callRealMethod();
        cancelled.countDown();
        return null;
      }).when(server).cancel(argThat(request -> request.getRequestId() == requestId), any());
    }
    QueryDispatcher dispatcher = spy(new QueryDispatcher(mock(MailboxService.class), mock(FailureDetector.class),
        null, true, Duration.ofSeconds(1)));
    IllegalStateException failure = new IllegalStateException("Consumer submission failed");
    doAnswer(invocation -> {
      Set<QueryServerInstance> servers = invocation.getArgument(3);
      for (int port : _queryServerMap.keySet()) {
        servers.add(new QueryServerInstance("Server_localhost_" + port, "localhost", port, port));
      }
      // No open streams remain; cleanup must still reach the completed producers.
      throw failure;
    }).when(dispatcher)
        .submitWithStream(anyLong(), any(DispatchableSubPlan.class), anyLong(), anySet(), anyMap(), any());
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      assertSame(expectThrows(IllegalStateException.class, () -> dispatcher.submitAndReduce(context,
          _queryEnvironment.planQuery("SELECT * FROM a"), 10_000, Map.of("materializedExchange", "true"))), failure);
      assertTrue(cancelled.await(10, TimeUnit.SECONDS));
    } finally {
      dispatcher.shutdown();
    }
  }

  @Test
  public void testValidateMaterializedOutputs() {
    Worker.MaterializedPartitionHandle partition0 = materializedHandle(7L, 1, 2, 0);
    Worker.MaterializedPartitionHandle partition1 = materializedHandle(7L, 1, 2, 1);

    QueryDispatcher.validateMaterializedOutputs(7L, Set.of("1/2/0", "1/2/1"),
        List.of(partition1, partition0));

    assertThrows(IllegalStateException.class,
        () -> QueryDispatcher.validateMaterializedOutputs(7L, Set.of("1/2/0"), List.of(partition0, partition0)));
    assertThrows(IllegalStateException.class,
        () -> QueryDispatcher.validateMaterializedOutputs(7L, Set.of("1/2/0", "1/2/1"), List.of(partition0)));
    assertThrows(IllegalStateException.class,
        () -> QueryDispatcher.validateMaterializedOutputs(7L, Set.of("1/2/0"),
            List.of(partition0.toBuilder().setRequestId(8L).build())));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);
      when(queryRunner.processQuery(any(), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
      QueryServer queryServer = Mockito.spy(new QueryServer(availablePort, queryRunner));
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
      _queryRunnerMap.put(availablePort, queryRunner);
    }
    List<Integer> portList = new ArrayList<>(_queryServerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(1, portList.get(0), portList.get(1),
        QueryEnvironmentTestBase.TABLE_SCHEMAS, QueryEnvironmentTestBase.SERVER1_SEGMENTS,
        QueryEnvironmentTestBase.SERVER2_SEGMENTS, null);
    _queryDispatcher =
        new QueryDispatcher(Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, true,
            Duration.ofSeconds(1));
  }

  /// The proto segment list encoding ships disabled and is turned on by an operator through cluster config, which has
  /// to reach the broker without a restart, and off again the same way.
  @Test
  public void testProtoSegmentListFollowsClusterConfig() {
    String key = CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_PROTO_SEGMENT_LIST;
    QueryDispatcher dispatcher =
        new QueryDispatcher(Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, false,
            Duration.ofSeconds(1));
    try {
      assertFalse(dispatcher.isEnableProtoSegmentList(), "The encoding must ship disabled");

      dispatcher.onChange(Set.of(key), Map.of(key, "true"));
      assertTrue(dispatcher.isEnableProtoSegmentList(), "Cluster config must turn the encoding on");

      dispatcher.onChange(Set.of(key), Map.of(key, "false"));
      assertFalse(dispatcher.isEnableProtoSegmentList(), "Cluster config must turn the encoding off again");

      // A change that does not touch the key leaves it alone.
      dispatcher.onChange(Set.of(key), Map.of(key, "TRUE"));
      dispatcher.onChange(Set.of("some.other.key"), Map.of("some.other.key", "x"));
      assertTrue(dispatcher.isEnableProtoSegmentList());

      // Anything that is not a boolean reads as disabled, the safe direction.
      dispatcher.onChange(Set.of(key), Map.of(key, "SAFE"));
      assertFalse(dispatcher.isEnableProtoSegmentList());
    } finally {
      dispatcher.shutdown();
    }
  }

  /// Clearing the cluster-config key disables the encoding, whatever the static broker config said: the fallback is
  /// always the legacy encoding that every server understands.
  @Test
  public void testClearingClusterConfigDisablesTheEncoding() {
    String key = CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_PROTO_SEGMENT_LIST;
    QueryDispatcher dispatcher =
        new QueryDispatcher(Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, false,
            Duration.ofSeconds(1), 0, 0, false, false, CommonConstants.Broker.DEFAULT_STREAM_STATS_DRAIN_MS, true);
    try {
      assertTrue(dispatcher.isEnableProtoSegmentList(), "The static broker config seeds the value");

      dispatcher.onChange(Set.of(key), Map.of());
      assertFalse(dispatcher.isEnableProtoSegmentList(),
          "Clearing the key must fall back to the legacy encoding");
    } finally {
      dispatcher.shutdown();
    }
  }

  @AfterClass
  public void tearDown() {
    _queryDispatcher.shutdown();
    for (QueryServer worker : _queryServerMap.values()) {
      worker.shutdown();
    }
  }

  @Test
  public void testStagedDispatchOption() {
    assertFalse(QueryOptionsUtils.isStagedDispatch(Map.of()));
    assertFalse(QueryOptionsUtils.isStagedDispatch(Map.of("stagedDispatch", "false")));
    assertTrue(QueryOptionsUtils.isStagedDispatch(Map.of("stagedDispatch", "TrUe")));
  }

  @DataProvider
  public Object[][] stagedSubmissionFailures() {
    // Success, producer submission failure, consumer submission failure.
    return new Object[][]{{0}, {2}, {1}};
  }

  @Test(dataProvider = "stagedSubmissionFailures")
  public void testStagedDispatchOrdersGroupsAndCleansCompletedProducers(int failingStage)
      throws Exception {
    List<Integer> ports = new ArrayList<>(_queryServerMap.keySet());
    DispatchableSubPlan plan = stagedPlan(ports);
    QueryDispatcher dispatcher = spy(_queryDispatcher);
    List<Set<Integer>> submittedGroups = new ArrayList<>();
    List<Deadline> deadlines = new ArrayList<>();
    doAnswer(invocation -> {
      Set<DispatchablePlanFragment> stages = invocation.getArgument(1);
      Set<Integer> stageIds = new HashSet<>();
      for (DispatchablePlanFragment stage : stages) {
        stageIds.add(stage.getPlanFragment().getFragmentId());
      }
      StreamingQuerySession session = invocation.getArgument(5);
      if (stageIds.contains(1)) {
        session.awaitSuccessfulStages(Set.of(2), 0, TimeUnit.NANOSECONDS);
        session.awaitStreamsClosed(0, TimeUnit.NANOSECONDS);
      }
      submittedGroups.add(stageIds);
      deadlines.add(invocation.getArgument(2));
      return invocation.callRealMethod();
    }).when(dispatcher).submitWithStream(anyLong(), anySet(), any(), anySet(), anyMap(), any());

    // Use real server submission and stream observers; only opchain execution is simulated.
    for (QueryRunner queryRunner : _queryRunnerMap.values()) {
      AtomicReference<OpChainCompletionListener> listener = new AtomicReference<>();
      doAnswer(invocation -> {
        listener.set(invocation.getArgument(1));
        return null;
      }).when(queryRunner).registerOpChainCompletionListener(anyLong(), any());
      doAnswer(invocation -> {
        WorkerMetadata worker = invocation.getArgument(0);
        StagePlan stage = invocation.getArgument(1);
        int stageId = stage.getStageMetadata().getStageId();
        if (stageId == failingStage) {
          throw new IllegalStateException("submission failure for stage " + stageId);
        }
        long requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
        OpChainExecutionContext context = mock(OpChainExecutionContext.class);
        when(context.getMaterializedOutputHandles()).thenReturn(stageId == 2
            ? List.of(materializedHandle(requestId, 2, worker.getWorkerId(), 0))
            : List.of());
        MultiStageOperator root = mock(MultiStageOperator.class);
        when(root.getOperatorType()).thenReturn(MultiStageOperator.Type.MAILBOX_SEND);
        listener.get().onOpChainComplete(new OpChainId(requestId, worker.getWorkerId(), stageId),
            root, null, context, null);
        return CompletableFuture.completedFuture(null);
      }).when(queryRunner).processQuery(any(), any(), any());
    }
    for (QueryServer server : _queryServerMap.values()) {
      clearInvocations(server);
    }

    Map<String, String> options = Map.of("stagedDispatch", "true", "materializedExchange", "true");
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest();
        MockedStatic<QueryDispatcher> statics = mockStatic(QueryDispatcher.class, CALLS_REAL_METHODS)) {
      long requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
      RequestContext request = new DefaultRequestContext();
      request.setRequestId(requestId);
      DataSchema schema = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema();
      ResultTable table = new ResultTable(schema, List.of());
      statics.when(() -> QueryDispatcher.runReducer(any(), anyMap(), any())).thenAnswer(invocation -> {
        assertEquals(submittedGroups, List.of(Set.of(2), Set.of(1)));
        return new QueryDispatcher.QueryResult(table, MultiStageQueryStats.emptyStats(0), 0L);
      });

      if (failingStage == 0) {
        QueryDispatcher.QueryResult result = dispatcher.submitAndReduce(request, plan, 10_000L, options);
        assertNull(result.getProcessingException());
        assertSame(result.getResultTable(), table);
        assertEquals(result.getStageCoverage().get(2).getResponded(), 1);
        for (QueryServer server : _queryServerMap.values()) {
          verify(server, never()).cancel(any(), any());
        }
      } else {
        RuntimeException error = expectThrows(RuntimeException.class,
            () -> dispatcher.submitAndReduce(request, plan, 10_000L, options));
        assertTrue(error.getMessage().contains("submission failure for stage " + failingStage));
        statics.verify(() -> QueryDispatcher.runReducer(any(), anyMap(), any()), never());
        // Server 0 only ran the producer. On consumer failure its stream is already closed, so this must be unary.
        verify(_queryServerMap.get(ports.get(0)), timeout(5000))
            .cancel(argThat(cancel -> cancel.getRequestId() == requestId), any());
      }
      assertEquals(submittedGroups, failingStage == 2 ? List.of(Set.of(2)) : List.of(Set.of(2), Set.of(1)));
      if (deadlines.size() == 2) {
        assertSame(deadlines.get(0), deadlines.get(1), "Every group must use the same absolute deadline");
      }
    } finally {
      for (QueryRunner queryRunner : _queryRunnerMap.values()) {
        reset(queryRunner);
        when(queryRunner.processQuery(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
      }
    }
  }

  private static DispatchableSubPlan stagedPlan(List<Integer> ports) {
    DataSchema schema = new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
    Map<Integer, DispatchablePlanFragment> stages = new HashMap<>();
    for (int stageId = 0; stageId <= 2; stageId++) {
      PlanNode input = stageId == 2
          ? new ValueNode(stageId, schema, PlanNode.NodeHint.EMPTY, List.of(), List.of())
          : new MailboxReceiveNode(stageId, schema, stageId + 1, PinotRelExchangeType.STREAMING,
              RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), List.of(), false, false, null, stageId == 1);
      PlanNode root = stageId == 0
          ? input
          : new MailboxSendNode(stageId, schema, List.of(input), List.of(stageId - 1), PinotRelExchangeType.STREAMING,
              RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), false, List.of(), false, "absHashCode", stageId == 2);
      int port = ports.get(stageId == 2 ? 0 : 1);
      QueryServerInstance server = new QueryServerInstance("server_" + port, "localhost", port, port);
      stages.put(stageId, new DispatchablePlanFragment(new PlanFragment(stageId, root, List.of()),
          List.of(new WorkerMetadata(0, Map.of())), stageId == 0 ? Map.of() : Map.of(server, List.of(0)), Map.of()));
    }
    return new DispatchableSubPlan(PairList.of(0, "col"), stages, Set.of(), Map.of(), 0L);
  }

  private static Worker.MaterializedPartitionHandle materializedHandle(long requestId, int stageId, int workerId,
      int partitionId) {
    return Worker.MaterializedPartitionHandle.newBuilder()
        .setRequestId(requestId)
        .setProducerStageId(stageId)
        .setProducerWorkerId(workerId)
        .setLogicalPartitionId(partitionId)
        .build();
  }

  @Test(dataProvider = "testSql")
  public void testQueryDispatcherCanSendCorrectPayload(String sql)
      throws Exception {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 10_000L, new HashSet<>(),
          Map.of());
    }
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerThrows() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doThrow(new RuntimeException("foo")).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 10_000L, new HashSet<>(),
          Map.of());
      fail("Method call above should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryServerCallsOnError()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doAnswer(invocationOnMock -> {
      StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
      observer.onError(new RuntimeException("foo"));
      return Set.of();
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submitAndReduce(context, dispatchableSubPlan, 10_000L, Map.of());
      fail("Method call above should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.verify(queryServer, Mockito.times(1))
          .cancel(Mockito.argThat(a -> a.getRequestId() == requestId), Mockito.any());
    }
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryReducerReturnsError()
      throws Exception {
    String sql = "SELECT * FROM a";
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      // will throw b/c mailboxService is mocked
      QueryDispatcher.QueryResult queryResult =
          _queryDispatcher.submitAndReduce(context, dispatchableSubPlan, 10_000L, Map.of());
      if (queryResult.getProcessingException() == null) {
        fail("Method call above should have failed");
      }
    } catch (NullPointerException e) {
      // Expected
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.verify(queryServer, Mockito.times(1))
          .cancel(Mockito.argThat(a -> a.getRequestId() == requestId), Mockito.any());
    }
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerCallsOnError() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doAnswer(invocationOnMock -> {
      StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
      observer.onError(new RuntimeException("foo"));
      return null;
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 10_000L, new HashSet<>(),
          Map.of());
      fail("Method call above should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerTimesOut() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    CountDownLatch neverClosingLatch = new CountDownLatch(1);
    Mockito.doAnswer(invocationOnMock -> {
      neverClosingLatch.await();
      StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
      observer.onCompleted();
      return null;
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 200L, new HashSet<>(), Map.of());
      fail("Method call above should have failed");
    } catch (Exception e) {
      String message = e.getMessage();
      assertTrue(
          message.contains("Timed out waiting for response") || message.contains("Error dispatching query"));
    }
    neverClosingLatch.countDown();
    Mockito.reset(failingQueryServer);
  }

  @Test(expectedExceptions = TimeoutException.class)
  public void testQueryDispatcherThrowsWhenDeadlinePreExpiredAndAsyncResponseNotPolled()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 0L, new HashSet<>(), Map.of());
    }
  }

  @Test
  public void testStatsManagerNotCalledWhenSubmitFails()
      throws Exception {
    ServerRoutingStatsManager statsManager = Mockito.mock(ServerRoutingStatsManager.class);
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);

    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doThrow(new RuntimeException("partial dispatch failure"))
        .when(failingQueryServer).submit(Mockito.any(), Mockito.any());

    DispatchableSubPlan plan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submitAndReduce(context, plan, 10_000L, Map.of(), statsManager);
      fail("Should have thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Error dispatching query"));
    }

    Mockito.verifyNoInteractions(statsManager);
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testRealStatsManagerInflightReturnsToZero()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, 1.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, -1);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, 0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 0.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT, 3);

    PinotConfiguration brokerConfig = new PinotConfiguration();
    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(
        brokerConfig.subset(CommonConstants.Broker.METRICS_CONFIG_PREFIX));
    BrokerMetrics brokerMetrics = new BrokerMetrics(
        CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX,
        metricsRegistry,
        CommonConstants.Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS,
        List.of());
    brokerMetrics.initializeGlobalMeters();
    BrokerMetrics.register(brokerMetrics);

    ServerRoutingStatsManager statsManager = new ServerRoutingStatsManager(
        new PinotConfiguration(properties), brokerMetrics);
    statsManager.init();

    String sql = "SELECT * FROM a";
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan plan = _queryEnvironment.planQuery(sql);

    Set<String> expectedInstanceIds = new HashSet<>();
    for (DispatchablePlanFragment fragment : plan.getQueryStagesWithoutRoot()) {
      for (QueryServerInstance server : fragment.getServerInstanceToWorkerIdMap().keySet()) {
        expectedInstanceIds.add(server.getInstanceId());
      }
    }
    assertFalse(expectedInstanceIds.isEmpty());

    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submitAndReduce(context, plan, 10_000L, Map.of(), statsManager);
    } catch (NullPointerException e) {
      // expected: reduce phase fails with mocked MailboxService
    }

    // Wait for the async executor to process all stats tasks (1 submission + 1 arrival per server).
    int expectedTasks = expectedInstanceIds.size() * 2;
    TestUtils.waitForCondition(
        aVoid -> statsManager.getCompletedTaskCount() >= expectedTasks,
        10L, 5000,
        "Timed out waiting for stats manager to process all tasks");

    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      for (String instanceId : expectedInstanceIds) {
        Integer numInFlight = statsManager.fetchNumInFlightRequestsForServer(instanceId);
        assertNotNull(numInFlight, "Expected stats entry for " + instanceId);
        assertEquals(numInFlight.intValue(), 0,
            "Expected 0 in-flight requests for " + instanceId + " after submitAndReduce returns");
      }
    }

    statsManager.shutDown();
  }
}
