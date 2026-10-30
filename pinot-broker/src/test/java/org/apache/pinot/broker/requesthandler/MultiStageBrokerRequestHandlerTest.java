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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.failuredetector.FailureDetectorFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.physical.PinotDispatchPlanner;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerOperator;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.QueryFingerprint;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.util.TestUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class MultiStageBrokerRequestHandlerTest extends QueryEnvironmentTestBase {

  @Test
  public void testOnQueryCompletionHookReceivesBrokerResponseForMse()
      throws Exception {
    // Verify that the overridable onQueryCompletion(RequestContext, BrokerResponse) hook is invoked
    // for the multi-stage engine path and receives the BrokerResponse that handleRequest() produced.
    // Mirrors the SSE-side test in BaseSingleStageBrokerRequestHandlerTest.
    AtomicReference<BrokerResponse> capturedResponse = new AtomicReference<>();

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    MultiStageBrokerRequestHandler handler =
        new MultiStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
            mock(RoutingManager.class), new AllowAllAccessControlFactory(), queryQuotaManager,
            mock(TableCache.class), mock(MultiStageQueryThrottler.class), mock(FailureDetector.class),
            ThreadAccountantUtils.getNoOpAccountant(), null, mock(WorkerManager.class), mock(WorkerManager.class)) {
          @Override
          public void start() {
            // Skip dispatcher.start() and Calcite warmupCompile — neither is needed for this hook test.
          }

          @Override
          public void shutDown() {
            // Match start() — no dispatcher was started, so there is nothing to shut down. Mirrors
            // the SSE-side BaseSingleStageBrokerRequestHandlerTest pattern.
          }

          @Override
          protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
              JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
              @Nullable HttpHeaders httpHeaders, AccessControl accessControl) {
            // Bypass MSE planning/dispatch — all we need is a non-null response flowing back through
            // BaseBrokerRequestHandler.handleRequest, which is what fires the onQueryCompletion hook.
            return new BrokerResponseNativeV2();
          }

          @Override
          protected void onQueryCompletion(RequestContext requestContext, BrokerResponse brokerResponse) {
            capturedResponse.set(brokerResponse);
          }
        };

    try {
      handler.handleRequest("SELECT 1");
    } catch (Exception ignored) {
      // routing/auth may fail — we only care that the hook was called with a non-null response
    }
    Assert.assertNotNull(capturedResponse.get(),
        "onQueryCompletion hook must be called with the BrokerResponse from handleRequest for MSE");
  }

  /// The broker calls [MultiStageBrokerRequestHandler#prepareStageForDispatch] for each stage before it sends the stage
  /// to its workers, both to run the query and to explain it on the servers.
  @Test
  public void testPrepareStageForDispatchHook()
      throws Exception {
    int port1 = NetUtils.findOpenPort();
    int port2 = NetUtils.findOpenPort();
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2);
    TABLE_SCHEMAS.forEach((tableName, schema) -> factory.registerTable(schema, tableName));
    SERVER1_SEGMENTS.forEach(
        (tableName, segments) -> segments.forEach(segment -> factory.registerSegment(port1, tableName, segment)));
    SERVER2_SEGMENTS.forEach(
        (tableName, segments) -> segments.forEach(segment -> factory.registerSegment(port2, tableName, segment)));
    RoutingManager routingManager = factory.buildRoutingManager(null);
    WorkerManager workerManager =
        new WorkerManager("Broker_localhost", "localhost", NetUtils.findOpenPort(), routingManager);
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    BrokerQueryEventListenerFactory.init(config);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    AtomicReference<DispatchableSubPlan> throttledPlan = new AtomicReference<>();
    try (MockedConstruction<QueryDispatcher> dispatchers = mockConstruction(QueryDispatcher.class)) {
      MultiStageBrokerRequestHandler handler = new MultiStageBrokerRequestHandler(config, "testBrokerId",
          new BrokerRequestIdGenerator(), routingManager, new AllowAllAccessControlFactory(), queryQuotaManager,
          factory.buildTableCache(), mock(MultiStageQueryThrottler.class), mock(FailureDetector.class),
          ThreadAccountantUtils.getNoOpAccountant(), null, workerManager, workerManager) {
        @Override
        protected void prepareStageForDispatch(DispatchablePlanFragment stage, Map<String, String> queryOptions) {
          stage.getCustomProperties().put("testKey", "stage" + stage.getPlanFragment().getFragmentId());
        }

        @Override
        protected int getEstimatedNumQueryThreads(DispatchableSubPlan dispatchableSubPlan) {
          throttledPlan.set(dispatchableSubPlan);
          return super.getEstimatedNumQueryThreads(dispatchableSubPlan);
        }
      };
      String query = "SELECT col1, COUNT(*) FROM a GROUP BY col1";

      // The mocked throttler stops the query before it runs
      handler.handleRequest(query);
      Map<Integer, DispatchablePlanFragment> stages = throttledPlan.get().getQueryStageMap();
      assertEquals(stages.size(), 3);
      stages.forEach((stageId, stage) -> assertEquals(stage.getCustomProperties().get("testKey"), "stage" + stageId));

      handler.handleRequest("SET " + QueryOptionKey.EXPLAIN_ASKING_SERVERS + " = true; EXPLAIN PLAN FOR " + query);
      ArgumentCaptor<DispatchablePlanFragment> explainedStages =
          ArgumentCaptor.forClass(DispatchablePlanFragment.class);
      verify(dispatchers.constructed().get(0), atLeastOnce())
          .explain(any(), explainedStages.capture(), anyLong(), any());
      for (DispatchablePlanFragment stage : explainedStages.getAllValues()) {
        assertEquals(stage.getCustomProperties().get("testKey"), "stage" + stage.getPlanFragment().getFragmentId());
      }
    }
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsInjectsStreamingGroupByFlushThreshold()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingGroupByFlushThreshold("5000");

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD), "5000",
        "Broker default should be injected when query option is absent");
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsPerQueryOverrideWins()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingGroupByFlushThreshold("5000");

    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD, "0");
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD), "0",
        "Per-query SET = 0 must override the broker default");

    queryOptions.clear();
    queryOptions.put(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD, "100");
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD), "100",
        "Per-query SET must take precedence over the broker default");
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsNoInjectionWhenConfigUnset()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingGroupByFlushThreshold(null);

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD),
        "No option should be injected when the broker default is unset");
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsInjectsStreamingDistinctFlushThreshold()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingDistinctFlushThreshold("5000");

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD), "5000",
        "Broker default should be injected when query option is absent");
    Assert.assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD),
        "The distinct default must not also enable streaming group-by");
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsStreamingDistinctPerQueryOverrideWins()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingDistinctFlushThreshold("5000");

    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD, "0");
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD), "0",
        "Per-query SET = 0 must override the broker default");

    queryOptions.clear();
    queryOptions.put(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD, "100");
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD), "100",
        "Per-query SET must take precedence over the broker default");
  }

  @Test
  public void testMergeStageStatsTotalMemAllocatedBytes() {
    StatMap<BaseMailboxReceiveOperator.StatKey> rootReceive = new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
        .merge(BaseMailboxReceiveOperator.StatKey.ALLOCATED_MEMORY_BYTES, 100_000L);
    StatMap<BaseMailboxReceiveOperator.StatKey> breakerReceive =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
            .merge(BaseMailboxReceiveOperator.StatKey.ALLOCATED_MEMORY_BYTES, 1L);
    StatMap<PipelineBreakerOperator.StatKey> breaker = new StatMap<>(PipelineBreakerOperator.StatKey.class)
        .merge(PipelineBreakerOperator.StatKey.ALLOCATED_MEMORY_BYTES, 10L);
    StatMap<LeafOperator.StatKey> leaf = new StatMap<>(LeafOperator.StatKey.class)
        .merge(LeafOperator.StatKey.ALLOCATED_MEMORY_BYTES, 100L)
        .merge(LeafOperator.StatKey.THREAD_MEM_ALLOCATED_BYTES, 1_000L);
    StatMap<MailboxSendOperator.StatKey> send = new StatMap<>(MailboxSendOperator.StatKey.class)
        .merge(MailboxSendOperator.StatKey.ALLOCATED_MEMORY_BYTES, 10_000L);

    // Stage 1 is a leaf stage with a pipeline breaker, flattened the same way in legacy and stream stats modes
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    MultiStageBrokerRequestHandler.mergeStageStats(brokerResponse, List.of(
        new MultiStageQueryStats.StageStats.Closed(List.of(MultiStageOperator.Type.MAILBOX_RECEIVE),
            List.of(rootReceive)),
        new MultiStageQueryStats.StageStats.Closed(
            List.of(MultiStageOperator.Type.MAILBOX_RECEIVE, MultiStageOperator.Type.PIPELINE_BREAKER,
                MultiStageOperator.Type.LEAF, MultiStageOperator.Type.MAILBOX_SEND),
            List.of(breakerReceive, breaker, leaf, send))));

    // Op chain roots (stage roots and the breaker) plus the leaf threads, but not the breaker's receive or the leaf's
    // own op chain allocation, which their roots already include
    assertEquals(brokerResponse.getTotalMemAllocatedBytes(), 100_000L + 10_000L + 1_000L + 10L);
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsNoStreamingDistinctInjectionWhenConfigUnset()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingDistinctFlushThreshold(null);

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD),
        "No option should be injected when the broker default is unset");
  }

  @Test
  public void testDispatchMaxInboundMessageSizeFromConfig()
      throws Exception {
    assertEquals(newHandler(new PinotConfiguration()).getQueryDispatcher().getDispatchMaxInboundMessageSizeBytes(),
        MultiStageQueryRunner.DEFAULT_OF_DISPATCH_CHANNEL_MAX_INBOUND_MESSAGE_SIZE_BYTES);
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_DISPATCH_CHANNEL_MAX_INBOUND_MESSAGE_SIZE_BYTES, "12345678");
    assertEquals(newHandler(config).getQueryDispatcher().getDispatchMaxInboundMessageSizeBytes(), 12_345_678);
  }

  @Test
  public void testDefaultDisabledPlannerRulesFromClusterConfig() {
    // ServiceStartableUtils.applyClusterConfig applies cluster configs with setProperty, which doesn't split lists
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_MSE_PLANNER_DISABLED_RULES,
        "SortJoinCopy, AggregateUnionAggregate,");
    assertEquals(MultiStageBrokerRequestHandler.getDefaultDisabledPlannerRules(config),
        Set.of(PlannerRuleNames.SORT_JOIN_COPY, PlannerRuleNames.AGGREGATE_UNION_AGGREGATE));
  }

  @Test
  public void testDefaultDisabledPlannerRulesUnsetOrEmpty() {
    assertEquals(MultiStageBrokerRequestHandler.getDefaultDisabledPlannerRules(new PinotConfiguration()),
        CommonConstants.Broker.DEFAULT_DISABLED_RULES);

    // An empty value disables no rules instead of falling back to the defaults
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_MSE_PLANNER_DISABLED_RULES, "");
    assertEquals(MultiStageBrokerRequestHandler.getDefaultDisabledPlannerRules(config), Set.of());
  }

  @Test
  public void testQueryFingerprintingReloadsWithoutRecreatingHandler()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new PinotConfiguration());
    handler.getQueryDispatcher().start();
    String key = CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_QUERY_FINGERPRINTING;
    try {
      assertQueryFingerprint(handler, true);
      handler.onChange(Set.of(key), Map.of(key, "false"));
      assertQueryFingerprint(handler, false);
      handler.onChange(Set.of(key), Map.of(key, "true"));
      assertQueryFingerprint(handler, true);
      handler.onChange(Set.of(key), Map.of(key, "false"));
      assertQueryFingerprint(handler, false);
      handler.onChange(Set.of(key), Map.of());
      assertQueryFingerprint(handler, true);
    } finally {
      handler.shutDown();
    }
  }

  @Test
  public void testQueryHashStillPropagatesWhenFingerprintingDisabledDuringRequest()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new PinotConfiguration());
    handler.getQueryDispatcher().start();
    String key = CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_QUERY_FINGERPRINTING;
    try (RequestScope requestContext = spy(Tracing.getTracer().createRequestScope())) {
      // Turn fingerprinting off immediately after this query generates its fingerprint, before compilation.
      doAnswer(invocation -> {
        invocation.callRealMethod();
        handler.onChange(Set.of(key), Map.of(key, "false"));
        return null;
      }).when(requestContext).setQueryFingerprint(any(QueryFingerprint.class));
      assertQueryFingerprint(handler, requestContext, true);
      assertQueryFingerprint(handler, false);
    } finally {
      handler.shutDown();
    }
  }

  private static void assertQueryFingerprint(MultiStageBrokerRequestHandler handler, boolean expected)
      throws Exception {
    try (RequestScope requestContext = Tracing.getTracer().createRequestScope()) {
      assertQueryFingerprint(handler, requestContext, expected);
    }
  }

  private static void assertQueryFingerprint(MultiStageBrokerRequestHandler handler, RequestScope requestContext,
      boolean expected)
      throws Exception {
    String sql = "SELECT col FROM missingTable WHERE col = 100";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());
    // The missing table stops planning before dispatch; fingerprint generation and propagation still run.
    handler.handleRequest(1L, sql, sqlNodeAndOptions, JsonUtils.newObjectNode(), null, requestContext, null,
        new AllowAllAccessControlFactory().create());
    QueryFingerprint fingerprint = requestContext.getQueryFingerprint();
    String workerQueryHash = sqlNodeAndOptions.getOptions().get(QueryOptionKey.QUERY_HASH);
    if (expected) {
      assertNotNull(fingerprint);
      assertEquals(workerQueryHash, fingerprint.getQueryHash());
    } else {
      assertNull(fingerprint);
      assertNull(workerQueryHash);
    }
  }

  private static MultiStageBrokerRequestHandler newHandlerWithStreamingGroupByFlushThreshold(
      @Nullable String streamingGroupByFlushThreshold)
      throws Exception {
    return newHandlerWithFlushThresholds(streamingGroupByFlushThreshold, null);
  }

  private static MultiStageBrokerRequestHandler newHandlerWithStreamingDistinctFlushThreshold(
      @Nullable String streamingDistinctFlushThreshold)
      throws Exception {
    return newHandlerWithFlushThresholds(null, streamingDistinctFlushThreshold);
  }

  private static MultiStageBrokerRequestHandler newHandlerWithFlushThresholds(
      @Nullable String streamingGroupByFlushThreshold, @Nullable String streamingDistinctFlushThreshold)
      throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    if (streamingGroupByFlushThreshold != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_GROUP_BY_FLUSH_THRESHOLD,
          streamingGroupByFlushThreshold);
    }
    if (streamingDistinctFlushThreshold != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_DISTINCT_FLUSH_THRESHOLD,
          streamingDistinctFlushThreshold);
    }
    return newHandler(config);
  }

  private static MultiStageBrokerRequestHandler newHandler(PinotConfiguration config)
      throws Exception {
    return newHandler(config, mock(RoutingManager.class), mock(FailureDetector.class), null);
  }

  private static MultiStageBrokerRequestHandler newHandler(PinotConfiguration config, RoutingManager routingManager,
      FailureDetector failureDetector, @Nullable MultiClusterRoutingContext multiClusterRoutingContext)
      throws Exception {
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    return new MultiStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
        routingManager, new AllowAllAccessControlFactory(), queryQuotaManager,
        mock(TableCache.class), mock(MultiStageQueryThrottler.class), failureDetector,
        ThreadAccountantUtils.getNoOpAccountant(), multiClusterRoutingContext, mock(WorkerManager.class),
        mock(WorkerManager.class)) {
      @Override
      public void start() {
      }

      @Override
      public void shutDown() {
        super.shutDown();
      }
    };
  }

  /// The failure detector keeps an instance unhealthy while any retrier returns UNHEALTHY for it. A multi-stage worker
  /// that is not an enabled server of the routing manager, for example a server of a remote cluster, must recover
  /// once the dispatch channel to it is READY, with the retriers of all request handlers registered.
  @Test
  public void testFailureDetectorRecoversWorkerOutsideRoutingManager()
      throws Exception {
    String workerId = "Server_remote_8098";
    PinotConfiguration failureDetectorConfig = new PinotConfiguration();
    failureDetectorConfig.setProperty(CommonConstants.Broker.FailureDetector.CONFIG_OF_TYPE,
        CommonConstants.Broker.FailureDetector.Type.CONNECTION.name());
    failureDetectorConfig.setProperty(CommonConstants.Broker.FailureDetector.CONFIG_OF_RETRY_INITIAL_DELAY_MS, 100);
    failureDetectorConfig.setProperty(CommonConstants.Broker.FailureDetector.CONFIG_OF_RETRY_DELAY_FACTOR, 1);
    // Recover only through the retriers, not because the retries ran out
    failureDetectorConfig.setProperty(CommonConstants.Broker.FailureDetector.CONFIG_OF_MAX_RETRIES, 1_000);
    FailureDetector failureDetector =
        FailureDetectorFactory.getFailureDetector(failureDetectorConfig, BrokerMetrics.noop());
    failureDetector.registerHealthyServerNotifier(instanceId -> {
    });
    failureDetector.registerUnhealthyServerNotifier(instanceId -> {
    });
    failureDetector.start();
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(Map.of());
    BrokerQueryEventListenerFactory.init(new PinotConfiguration());
    // Like the broker starter, register the retriers of the single-stage handlers first
    SingleConnectionBrokerRequestHandler singleConnectionHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), routingManager, new AllowAllAccessControlFactory(), null, null, null, null,
            mock(ServerRoutingStatsManager.class), failureDetector, ThreadAccountantUtils.getNoOpAccountant(), null,
            null);
    GrpcBrokerRequestHandler grpcHandler =
        new GrpcBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", new BrokerRequestIdGenerator(),
            routingManager, new AllowAllAccessControlFactory(), null, null, failureDetector,
            ThreadAccountantUtils.getNoOpAccountant(), null);
    AtomicReference<FailureDetector.ServerState> workerState =
        new AtomicReference<>(FailureDetector.ServerState.UNHEALTHY);
    try (MockedConstruction<QueryDispatcher> dispatchers = mockConstruction(QueryDispatcher.class,
        (dispatcher, context) -> when(dispatcher.checkConnectivityToInstance(workerId)).thenAnswer(
            invocation -> workerState.get()))) {
      newHandler(new PinotConfiguration(), routingManager, failureDetector, null);
      QueryDispatcher dispatcher = dispatchers.constructed().get(0);
      failureDetector.markServerUnhealthy(workerId, "remote");

      // The dispatch channel to the worker is not READY
      verify(dispatcher, timeout(5_000L).atLeast(3)).checkConnectivityToInstance(workerId);
      assertEquals(failureDetector.getUnhealthyServers(), Set.of(workerId));

      workerState.set(FailureDetector.ServerState.HEALTHY);
      TestUtils.waitForCondition(aVoid -> failureDetector.getUnhealthyServers().isEmpty(), 5_000L,
          "The worker did not recover");
    } finally {
      failureDetector.stop();
      singleConnectionHandler.shutDown();
      grpcHandler.shutDown();
    }
  }

  /// The retrier checks a server at its current address, in this cluster or with multi-cluster routing in a remote
  /// cluster. It checks other instances at the address that a query was last dispatched to.
  @Test
  public void testRetryUnhealthyServerChecksCurrentAddress()
      throws Exception {
    ServerInstance localServer = mock(ServerInstance.class);
    ServerInstance remoteServer = mock(ServerInstance.class);
    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(Map.of("Server_local_8098", localServer));
    RoutingManager multiClusterRoutingManager = mock(RoutingManager.class);
    when(multiClusterRoutingManager.getEnabledServerInstanceMap()).thenReturn(
        Map.of("Server_local_8098", localServer, "Server_remote_8098", remoteServer));
    MultiClusterRoutingContext multiClusterRoutingContext =
        new MultiClusterRoutingContext(Map.of(), routingManager, multiClusterRoutingManager, Set.of());
    try (MockedConstruction<QueryDispatcher> dispatchers = mockConstruction(QueryDispatcher.class)) {
      MultiStageBrokerRequestHandler handler = newHandler(new PinotConfiguration(), routingManager,
          mock(FailureDetector.class), multiClusterRoutingContext);
      QueryDispatcher dispatcher = dispatchers.constructed().get(0);
      when(dispatcher.checkConnectivityToInstance(localServer)).thenReturn(FailureDetector.ServerState.HEALTHY);
      when(dispatcher.checkConnectivityToInstance(remoteServer)).thenReturn(FailureDetector.ServerState.UNHEALTHY);
      when(dispatcher.checkConnectivityToInstance("Server_worker_8098")).thenReturn(
          FailureDetector.ServerState.HEALTHY);

      assertEquals(handler.retryUnhealthyServer("Server_local_8098"), FailureDetector.ServerState.HEALTHY);
      assertEquals(handler.retryUnhealthyServer("Server_remote_8098"), FailureDetector.ServerState.UNHEALTHY);
      assertEquals(handler.retryUnhealthyServer("Server_worker_8098"), FailureDetector.ServerState.HEALTHY);
      verify(dispatcher, never()).checkConnectivityToInstance("Server_local_8098");
      verify(dispatcher, never()).checkConnectivityToInstance("Server_remote_8098");
    }
  }

  // Timeout guards against deadlock: if rewriteReduceStageForEmptyLeaves fails to inline
  // all MailboxReceiveNodes, the reducer will block forever polling a mailbox with no sender.
  @Test(timeOut = 10_000)
  public void testAllLeafStagesEmptyReducerDoesNotWaitForChildStageMailbox() {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery("SELECT COUNT(*) FROM a WHERE ts < 0 LIMIT 1");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);

    QueryDispatcher.QueryResult queryResult = runReducer(fragmentMap, subPlan);
    assertNull(queryResult.getProcessingException());
    ResultTable resultTable = queryResult.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 0L);
  }

  @Test(timeOut = 10_000)
  public void testAllLeafStagesEmptyReducerRunsPushedDownAggregates() {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(
        "SELECT COUNT(*), DISTINCTCOUNT(col1), DISTINCTCOUNT(col2), SUM(col3) FROM a WHERE ts < 0 LIMIT 1");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);

    QueryDispatcher.QueryResult queryResult = runReducer(fragmentMap, subPlan);
    assertNull(queryResult.getProcessingException());
    ResultTable resultTable = queryResult.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    assertEquals(((Number) rows.get(0)[0]).longValue(), 0L);
    assertEquals(((Number) rows.get(0)[1]).longValue(), 0L);
    assertEquals(((Number) rows.get(0)[2]).longValue(), 0L);
    assertNull(rows.get(0)[3]);
  }

  @Test(timeOut = 10_000)
  public void testAllLeafStagesEmptyReducerWithJoin() {
    DispatchableSubPlan subPlan =
        _queryEnvironment.planQuery("SELECT COUNT(*) FROM a JOIN b ON a.col1 = b.col1 WHERE a.ts < 0");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);

    QueryDispatcher.QueryResult queryResult = runReducer(fragmentMap, subPlan);
    assertNull(queryResult.getProcessingException());
    ResultTable resultTable = queryResult.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 0L);
  }

  private static QueryDispatcher.QueryResult runReducer(Map<Integer, DispatchablePlanFragment> fragmentMap,
      DispatchableSubPlan originalSubPlan) {
    // Match production: drop orphan stages after rewrite inlines them into stage 0.
    fragmentMap.keySet().retainAll(Set.of(0));
    DispatchableSubPlan rewrittenPlan = new DispatchableSubPlan(
        originalSubPlan.getQueryResultFields(), fragmentMap, originalSubPlan.getTableNames(),
        originalSubPlan.getTableToUnavailableSegmentsMap(), originalSubPlan.getNumSegmentsPrunedByBroker(), true);
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      return QueryDispatcher.runReducer(rewrittenPlan, Map.of(), Mockito.mock(MailboxService.class));
    }
  }
}
