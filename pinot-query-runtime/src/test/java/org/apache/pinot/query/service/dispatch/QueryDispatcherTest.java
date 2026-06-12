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

import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.QueryRunner;
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
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;


public class QueryDispatcherTest extends QueryTestSet {
  private static final AtomicLong REQUEST_ID_GEN = new AtomicLong();
  private static final int QUERY_SERVER_COUNT = 2;

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;
  private QueryDispatcher _queryDispatcher;

  @BeforeClass
  public void setUp()
      throws Exception {
    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);
      QueryServer queryServer = Mockito.spy(new QueryServer(availablePort, queryRunner));
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
    }
    List<Integer> portList = new ArrayList<>(_queryServerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(1, portList.get(0), portList.get(1),
        QueryEnvironmentTestBase.TABLE_SCHEMAS, QueryEnvironmentTestBase.SERVER1_SEGMENTS,
        QueryEnvironmentTestBase.SERVER2_SEGMENTS, null);
    _queryDispatcher = Mockito.spy(
        new QueryDispatcher(Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, true,
            Duration.ofSeconds(1))
    );
  }

  @AfterMethod
  public void resetMocks() {
    Mockito.reset(_queryDispatcher);
    for (QueryServer worker : _queryServerMap.values()) {
      Mockito.reset(worker);
    }
  }

  @AfterClass
  public void tearDown() {
    _queryDispatcher.shutdown();
    for (QueryServer worker : _queryServerMap.values()) {
      worker.shutdown();
    }
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
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
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
    long requestId;
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
      RequestContext context = new DefaultRequestContext();
      context.setRequestId(requestId);
      BrokerResponseNativeV2 response = _queryDispatcher.submitAndReduceForTest(dispatchableSubPlan, Map.of());
      Assertions.assertThat(response.getExceptions())
          .describedAs("Expected exceptions from the response")
          .isNotEmpty();
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    Mockito.verify(_queryDispatcher, Mockito.times(1))
        .cancel(requestId);
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryReducerReturnsError()
      throws Exception {
    String sql = "SELECT * FROM a";
    long requestId;
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
      RequestContext context = new DefaultRequestContext();
      context.setRequestId(requestId);

      QueryServer aServer = _queryServerMap.values().stream()
          .findAny()
          .orElseThrow();
      Mockito.doThrow(RuntimeException.class).when(aServer).submit(Mockito.any(), Mockito.any());

      // will throw b/c mailboxService is mocked
      try {
        QueryDispatcher.DispatcherStreamingBrokerResponse queryResult =
            _queryDispatcher.submit(context, dispatchableSubPlan, 10_000L, Map.of());

        StreamingBrokerResponse.Metainfo metainfo = queryResult.consumeData(
            data -> {
              // no-op
            });
        if (metainfo.getExceptions().isEmpty()) {
          Assert.fail("Method call above should have failed");
        }
      } catch (RuntimeException e) {
        // expected
      }
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    Mockito.verify(_queryDispatcher, Mockito.times(1))
        .cancel(requestId);
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
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
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
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      String message = e.getMessage();
      Assert.assertTrue(
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

  /**
   * Task 4(a): Consume (or attempt) then close — servers must NOT receive a cancel RPC.
   *
   * <p>{@code consume()} always calls {@code cleanupQueryServerTracking} in its {@code finally} block
   * regardless of whether {@code consumeData} throws or returns normally.  A subsequent
   * {@link QueryDispatcher.DispatcherStreamingBrokerResponse#close()} therefore calls
   * {@code cancel(requestId)}, which finds no tracking entry and returns false without sending any
   * cancel RPC to servers.
   *
   * <p>This test dispatches normally (no server error mock) so that the public
   * {@link QueryDispatcher#submit(RequestContext, DispatchableSubPlan, long, Map)} returns
   * successfully.  The {@code consumeData} call will throw a {@link RuntimeException} (mocked
   * MailboxService provides no real mailbox), but that is still caught by {@code consume()}'s
   * RuntimeException handler, so the {@code finally} still fires and cleans up the tracking entry.
   */
  @Test
  public void testCloseAfterConsumeAttemptDoesNotCancelServers()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    long requestId;
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
      RequestContext context = new DefaultRequestContext();
      context.setRequestId(requestId);

      QueryDispatcher.DispatcherStreamingBrokerResponse response =
          _queryDispatcher.submit(context, dispatchableSubPlan, 10_000L, Map.of());

      // consumeData may throw RuntimeException (mocked MailboxService → no real mailbox).
      // Either way, consume()'s finally will call cleanupQueryServerTracking, removing the entry.
      try {
        response.consumeData(data -> { });
      } catch (RuntimeException ignored) {
        // RuntimeException from the mocked mailbox is expected; consume() has already cleaned up.
      }

      // close() calls cancel(requestId) which finds no tracking entry → no-ops.
      response.close();
    }

    // Give any async cancel a moment to arrive (it must NOT arrive)
    Thread.sleep(50);

    // The tracking entry was removed by consume's finally; close() must NOT forward a cancel RPC.
    for (QueryServer server : _queryServerMap.values()) {
      Mockito.verify(server, Mockito.never()).cancel(any(), any());
    }
  }

  /**
   * Task 4(b): Close without consuming.
   *
   * <p>When a caller obtains a {@link QueryDispatcher.DispatcherStreamingBrokerResponse} and then
   * calls {@link QueryDispatcher.DispatcherStreamingBrokerResponse#close()} without ever calling
   * {@code consumeData}, the tracking entry is still present in {@code _serversByQuery} and the
   * servers must receive exactly one cancel RPC.
   */
  @Test
  public void testCloseWithoutConsumeCancelsServers()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    long requestId;
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
      RequestContext context = new DefaultRequestContext();
      context.setRequestId(requestId);

      QueryDispatcher.DispatcherStreamingBrokerResponse response =
          _queryDispatcher.submit(context, dispatchableSubPlan, 10_000L, Map.of());

      // Close without consuming — the _serversByQuery entry is still present
      response.close();
    }

    // Give the async cancel gRPC call time to reach the servers
    Thread.sleep(50);

    // cancel(requestId) must have been invoked exactly once (by close())
    Mockito.verify(_queryDispatcher, Mockito.times(1)).cancel(requestId);

    // At least one server must have received the cancel RPC
    long cancelledServers = _queryServerMap.values().stream()
        .filter(server -> {
          try {
            Mockito.verify(server, Mockito.atLeastOnce()).cancel(any(), any());
            return true;
          } catch (AssertionError e) {
            return false;
          }
        })
        .count();
    Assert.assertTrue(cancelledServers > 0, "At least one server must have received a cancel RPC");
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
      Assert.fail("Should have thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
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
        Collections.emptyList());
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
    Assert.assertFalse(expectedInstanceIds.isEmpty());

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
        Assert.assertNotNull(numInFlight, "Expected stats entry for " + instanceId);
        Assert.assertEquals(numInFlight.intValue(), 0,
            "Expected 0 in-flight requests for " + instanceId + " after submitAndReduce returns");
      }
    }

    statsManager.shutDown();
  }
}
