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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.service.server.QueryServer;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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
    _queryDispatcher =
        new QueryDispatcher(Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, true,
            Duration.ofSeconds(1));
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
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submitAndReduce(context, dispatchableSubPlan, 10_000L, Map.of());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
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
        Assert.fail("Method call above should have failed");
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

  /**
   * When no indirect timing is extracted (e.g. reduce phase fails before stats propagate), the
   * finally block must still decrement in-flight for all submitted servers. With no known timings
   * and no cancel attempted, all servers fall into Tier 4 (no timing data, server responsive)
   * and get -1 (decrement only, no EMA update) rather than the contaminated wall-clock.
   */
  @Test
  public void testStatsManagerRecordsSubmissionAndArrivalForDispatchedServers()
      throws Exception {
    // Clock ticks by tickMs on each call: submitTimeMs = 1000, then elapsedMs = 1100 - 1000 = 100.
    final long tickMs = 100L;
    AtomicLong fakeClockMs = new AtomicLong(1_000L);
    QueryDispatcher dispatcher = newDispatcher(() -> fakeClockMs.getAndAdd(tickMs));

    ServerRoutingStatsManager statsManager = Mockito.mock(ServerRoutingStatsManager.class);
    String sql = "SELECT * FROM a";
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan plan = _queryEnvironment.planQuery(sql);

    Set<String> expectedInstanceIds = getExpectedInstanceIds(plan);
    try {
      try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
        dispatcher.submitAndReduce(context, plan, 10_000L, Map.of(), statsManager);
      } catch (NullPointerException e) {
        // expected: reduce phase fails with mocked MailboxService
      }
      for (String instanceId : expectedInstanceIds) {
        Mockito.verify(statsManager).recordStatsForQuerySubmission(requestId, instanceId);
        // No indirect timing was extracted (mocked MailboxService -> no real stats), so
        // knownTimings is empty -> all servers get -1 (Tier 4: no timing data).
        Mockito.verify(statsManager).recordStatsUponResponseArrival(
            Mockito.eq(requestId), Mockito.eq(instanceId), Mockito.eq(-1L));
      }
    } finally {
      dispatcher.shutdown();
    }
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
    Set<String> expectedInstanceIds = getExpectedInstanceIds(plan);

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

  @Test
  public void testNoStatsRecordedWhenAdaptiveRoutingDisabled()
      throws Exception {
    // When statsManager is null (adaptive routing disabled), submitAndReduce must not
    // dereference it — the null guards in submitAndReduce must prevent any NPE from the
    // stats path. Any exception thrown here should come from the mocked MailboxService
    // (reduce phase), not from a null statsManager dereference.
    String sql = "SELECT * FROM a";
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan plan = _queryEnvironment.planQuery(sql);

    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      _queryDispatcher.submitAndReduce(context, plan, 10_000L, Map.of(), null);
    } catch (NullPointerException e) {
      // Acceptable: reduce phase NPEs because MailboxService is mocked.
      // Verify the NPE did NOT originate from the stats-manager null-guard path.
      for (StackTraceElement frame : e.getStackTrace()) {
        Assert.assertFalse(
            frame.getMethodName().contains("recordStats") || frame.getMethodName().contains("applyUpstreamTimings"),
            "NPE must not originate from stats recording path, but got: " + frame);
      }
    }
  }

  @Test
  public void testStatsBalancedWhenDispatchThrowsUnknownException() throws Exception {
    // Stub submit() to populate serversOut like serializePlanFragments(), then throw a RuntimeException. This exercises
    // the unknown-exception path where tryRecover() plain-cancels and re-throws without calling cancelWithStats.
    QueryDispatcher spyDispatcher = Mockito.spy(_queryDispatcher);
    Mockito.doAnswer(invocation -> {
      DispatchableSubPlan submittedPlan = invocation.getArgument(1);
      Set<QueryServerInstance> serversOut = invocation.getArgument(3);
      Consumer<QueryServerInstance> hook = invocation.getArgument(5);
      for (DispatchablePlanFragment fragment : submittedPlan.getQueryStagesWithoutRoot()) {
        for (QueryServerInstance server : fragment.getServerInstanceToWorkerIdMap().keySet()) {
          serversOut.add(server);
          if (hook != null) {
            hook.accept(server);
          }
        }
      }
      throw new RuntimeException("simulated failure");
    })
        .when(spyDispatcher)
        .submit(Mockito.anyLong(), Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any(),
            Mockito.any());

    ServerRoutingStatsManager statsManager = Mockito.mock(ServerRoutingStatsManager.class);
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
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

    RuntimeException thrown = Assert.expectThrows(RuntimeException.class, () -> {
      try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
        spyDispatcher.submitAndReduce(context, plan, 10_000L, Map.of(), statsManager);
      }
    });
    Assert.assertEquals(thrown.getMessage(), "simulated failure");

    // Servers are incremented before submit() runs. Plain cancel() does not return stats, so cancelOutcome = NONE
    // and every server is balanced via Tier 4 with latency = -1L.
    for (String instanceId : expectedInstanceIds) {
      Mockito.verify(statsManager).recordStatsForQuerySubmission(requestId, instanceId);
      Mockito.verify(statsManager).recordStatsUponResponseArrival(requestId, instanceId, -1L);
    }
  }

  /**
   * When {@code submit()} throws {@link TimeoutException} (one server never ACKs the dispatch),
   * {@code submitAndReduce()} must catch it via {@code tryRecover()} and return a failed
   * {@code QueryResult} — not propagate the exception.
   *
   * <p>With the fix, {@code recordStatsForQuerySubmission} is called before {@code submit()}, so
   * {@code incrementedServers} is populated even on timeout. The server that did not respond to
   * cancel is recorded as degraded with a positive {@code elapsedMs} via Tier 3.
   */
  @Test
  public void testSubmitAndReduceReturnsResultWhenSubmitTimesOut()
      throws Exception {
    // All servers hang on submit so processResults() times out after the short deadline.
    CountDownLatch neverClosingLatch = new CountDownLatch(1);
    List<QueryServer> allServers = new ArrayList<>(_queryServerMap.values());
    for (QueryServer server : allServers) {
      Mockito.doAnswer(invocationOnMock -> {
        neverClosingLatch.await();
        StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
        observer.onCompleted();
        return null;
      }).when(server).submit(Mockito.any(), Mockito.any());
    }

    ServerRoutingStatsManager statsManager = Mockito.mock(ServerRoutingStatsManager.class);
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

    try {
      try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
        // submit() times out because all servers never ACK -> tryRecover() handles TimeoutException.
        // Depending on whether cancelWithStats succeeds, this either returns a QueryResult with a
        // processing exception or throws a RuntimeException wrapping the cancel failure.
        QueryDispatcher.QueryResult result =
            _queryDispatcher.submitAndReduce(context, plan, 200L, Map.of(), statsManager);
        Assert.assertNotNull(result.getProcessingException(),
            "Expected a processing exception in the result when submit times out");
      }
    } catch (RuntimeException e) {
      // Cancel phase may also throw if the hanging servers don't respond to the cancel RPC.
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"),
          "Expected dispatch error from cancel phase, got: " + e.getMessage());
    } finally {
      neverClosingLatch.countDown();
      for (QueryServer server : allServers) {
        Mockito.reset(server);
      }
    }
  }

    // Servers are incremented before submit() runs, so recordStatsForQuerySubmission is called for all.
    for (String instanceId : expectedInstanceIds) {
      Mockito.verify(statsManager).recordStatsForQuerySubmission(requestId, instanceId);
    }
    // The hanging server does not respond to cancel → Tier 3 → elapsedMs > 0.
    Mockito.verify(statsManager, Mockito.atLeastOnce())
        .recordStatsUponResponseArrival(Mockito.eq(requestId), Mockito.any(), Mockito.longThat(l -> l > 0));
  }

  /**
   * Simulates the SIGSTOP scenario: one server hangs on submit AND does not respond to cancel
   * (TCP connection alive, server frozen). With the fix, {@code incrementedServers} is populated
   * before {@code submit()}, so {@code recordPerServerLatencies} marks the unresponsive server
   * degraded via Tier 3 with {@code latency = elapsedMs > 0} — the EMA is updated correctly.
   *
   * <p>A fake clock pins {@code elapsedMs} to 1000 ms regardless of wall-clock time.
   * A short cancel timeout (100 ms) keeps the test fast.
   */
  @Test
  public void testDispatchTimeoutRecordsElapsedLatency()
      throws Exception {
    AtomicLong fakeClockMs = new AtomicLong(1_000L);
    // Each getAsLong() call advances the fake clock by 1000 ms:
    //   call 1 (submitTimeMs) → 1000, call 2 (finally elapsedMs) → 2000 − 1000 = 1000.
    QueryDispatcher dispatcher = new QueryDispatcher(
        Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, true,
        Duration.ofMillis(100),          // short cancel timeout so the test completes quickly
        () -> fakeClockMs.getAndAdd(1_000L));

    QueryServer hangingServer = _queryServerMap.values().iterator().next();
    CountDownLatch neverClosingLatch = new CountDownLatch(1);
    // Hang both submit and cancel to simulate a fully frozen server (SIGSTOP).
    Mockito.doAnswer(inv -> {
      neverClosingLatch.await();
      return null;
    }).when(hangingServer).submit(Mockito.any(), Mockito.any());
    Mockito.doAnswer(inv -> {
      neverClosingLatch.await();
      return null;
    }).when(hangingServer).cancel(Mockito.any(), Mockito.any());

    ServerRoutingStatsManager statsManager = Mockito.mock(ServerRoutingStatsManager.class);
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

    try {
      QueryDispatcher.QueryResult result;
      try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
        result = dispatcher.submitAndReduce(context, plan, 200L, Map.of(), statsManager);
      }
      Assert.assertNotNull(result.getProcessingException(),
          "Expected a processing exception when submit times out");

      // All servers must have had their in-flight counter incremented before dispatch.
      for (String instanceId : expectedInstanceIds) {
        Mockito.verify(statsManager).recordStatsForQuerySubmission(requestId, instanceId);
      }
      // The hanging server did not respond to cancel → Tier 3 → elapsedMs = 1000 > 0.
      // (Other servers that responded to cancel fall into Tier 4 → −1L.)
      Mockito.verify(statsManager, Mockito.atLeastOnce())
          .recordStatsUponResponseArrival(Mockito.eq(requestId), Mockito.any(), Mockito.eq(1_000L));
    } finally {
      neverClosingLatch.countDown();
      Mockito.reset(hangingServer);
      dispatcher.shutdown();
    }
  }

  /**
   * Reproduces the bug where runReducer returns a QueryResult with processingException (e.g. EXECUTION_TIMEOUT
   * from a mailbox receive timeout), and the old code called cancel() instead of cancelWithStats(). This left
   * cancelOutcome == NONE, causing all servers to fall into Tier 4 (latency=-1) — a no-op for the EMA.
   *
   * After the fix (cancelWithStats on the processingException path), servers that don't respond to the cancel
   * get Tier 3 (latency=elapsedMs), which is > 0 and actually updates the EMA.
   */
  @Test
  public void testProcessingExceptionPathRecordsElapsedLatencyNotMinusOne()
      throws Exception {
    // Clock ticks by tickMs on each call: submitTimeMs = 1000, then elapsedMs = 1100 - 1000 = 100.
    final long tickMs = 100L;
    AtomicLong fakeClockMs = new AtomicLong(1_000L);
    QueryDispatcher dispatcher = newDispatcher(() -> fakeClockMs.getAndAdd(tickMs));

    ServerRoutingStatsManager statsManager = Mockito.mock(ServerRoutingStatsManager.class);
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

    // Build a QueryResult with a processingException — simulates runReducer returning an
    // EXECUTION_TIMEOUT error block (the real production path for MSE timeouts).
    QueryProcessingException processingException =
        new QueryProcessingException(QueryErrorCode.EXECUTION_TIMEOUT, "simulated timeout");
    MultiStageQueryStats emptyStats = MultiStageQueryStats.emptyStats(0);
    QueryDispatcher.QueryResult errorResult =
        new QueryDispatcher.QueryResult(processingException, emptyStats, 0L);

    // Make servers hang on cancel — simulating a slow/overloaded server that caused the query timeout
    // and also doesn't respond to the cancel RPC. This is what triggers Tier 3.
    CountDownLatch cancelLatch = new CountDownLatch(1);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.doAnswer(invocationOnMock -> {
        cancelLatch.await();
        StreamObserver<Worker.CancelResponse> observer = invocationOnMock.getArgument(1);
        observer.onCompleted();
        return null;
      }).when(queryServer).cancel(Mockito.any(), Mockito.any());
    }

    try (MockedStatic<QueryDispatcher> mockedStatic =
        Mockito.mockStatic(QueryDispatcher.class, Mockito.CALLS_REAL_METHODS)) {
      mockedStatic.when(() -> QueryDispatcher.runReducer(Mockito.any(), Mockito.any(), Mockito.any()))
          .thenReturn(errorResult);

      QueryDispatcher.QueryResult result;
      try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
        result = dispatcher.submitAndReduce(context, plan, 10_000L, Map.of(), statsManager);
      }
      Assert.assertNotNull(result.getProcessingException());

      for (String instanceId : expectedInstanceIds) {
        Mockito.verify(statsManager).recordStatsForQuerySubmission(requestId, instanceId);
        Mockito.verify(statsManager).recordStatsUponResponseArrival(
            Mockito.eq(requestId), Mockito.eq(instanceId), Mockito.longThat(latency -> latency > 0));
      }
    } finally {
      cancelLatch.countDown();
      for (QueryServer queryServer : _queryServerMap.values()) {
        Mockito.reset(queryServer);
      }
      dispatcher.shutdown();
    }
  }

  /** Creates a local {@link QueryDispatcher} wired to the shared query servers with an injected clock. */
  private QueryDispatcher newDispatcher(LongSupplier clock) {
    return new QueryDispatcher(Mockito.mock(MailboxService.class), Mockito.mock(FailureDetector.class), null, true,
        Duration.ofSeconds(1), clock);
  }

  private Set<String> getExpectedInstanceIds(DispatchableSubPlan plan) {
    Set<String> expectedInstanceIds = new HashSet<>();
    for (DispatchablePlanFragment fragment : plan.getQueryStagesWithoutRoot()) {
      for (QueryServerInstance server : fragment.getServerInstanceToWorkerIdMap().keySet()) {
        expectedInstanceIds.add(server.getInstanceId());
      }
    }
    Assert.assertFalse(expectedInstanceIds.isEmpty());
    return expectedInstanceIds;
  }
}
