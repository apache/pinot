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
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.physical.PinotDispatchPlanner;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


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
  public void testApplyBrokerDefaultQueryOptionsNoStreamingDistinctInjectionWhenConfigUnset()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingDistinctFlushThreshold(null);

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_DISTINCT_FLUSH_THRESHOLD),
        "No option should be injected when the broker default is unset");
  }

  /// The estimated early exit has no soundness guarantee, so it must stay off unless a cluster explicitly opts in,
  /// and a per-query SET must still win.
  @Test
  public void testApplyBrokerDefaultQueryOptionsStreamingDistinctEstimatedExitStdDev()
      throws Exception {
    MultiStageBrokerRequestHandler off = newHandlerWithEstimatedExitStdDev(null);
    Map<String, String> queryOptions = new HashMap<>();
    off.applyBrokerDefaultQueryOptions(queryOptions);
    assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_DISTINCT_ESTIMATED_EXIT_STD_DEV),
        "The unsound estimated exit must not be injected unless the cluster opts in");

    MultiStageBrokerRequestHandler on = newHandlerWithEstimatedExitStdDev("3");
    queryOptions.clear();
    on.applyBrokerDefaultQueryOptions(queryOptions);
    assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_ESTIMATED_EXIT_STD_DEV), "3");

    queryOptions.clear();
    queryOptions.put(QueryOptionKey.STREAMING_DISTINCT_ESTIMATED_EXIT_STD_DEV, "0");
    on.applyBrokerDefaultQueryOptions(queryOptions);
    assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_ESTIMATED_EXIT_STD_DEV), "0",
        "Per-query SET = 0 must be able to opt back out of a cluster that enabled it");
  }

  /// A cluster configured out of range must fail at startup, where the message names the config. Without this it
  /// starts cleanly and then fails every MSE DISTINCT query at plan time, blaming the query.
  @Test
  public void testOutOfRangeEstimatedExitStdDevConfigIsRejectedAtStartup() {
    for (String value : new String[]{"4", "99"}) {
      try {
        newHandlerWithEstimatedExitStdDev(value);
        Assert.fail("Expected the broker to reject std dev " + value);
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains(QueryOptionKey.STREAMING_DISTINCT_ESTIMATED_EXIT_STD_DEV),
            e.getMessage());
      } catch (Exception e) {
        Assert.fail("Expected IllegalArgumentException but got: " + e);
      }
    }
  }

  /// The exact regime is on by default, so the cluster needs a way to turn it off without touching every client.
  /// `0` is a real value here, not an unset marker, so it has to be injectable.
  @Test
  public void testApplyBrokerDefaultQueryOptionsStreamingDistinctMaxTrackedCardinality()
      throws Exception {
    MultiStageBrokerRequestHandler unset = newHandlerWithMaxTrackedCardinality(null);
    Map<String, String> queryOptions = new HashMap<>();
    unset.applyBrokerDefaultQueryOptions(queryOptions);
    assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_DISTINCT_MAX_TRACKED_CARDINALITY),
        "Nothing should be injected when the cluster has not set it");

    MultiStageBrokerRequestHandler killed = newHandlerWithMaxTrackedCardinality("0");
    queryOptions.clear();
    killed.applyBrokerDefaultQueryOptions(queryOptions);
    assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_MAX_TRACKED_CARDINALITY), "0",
        "0 is the kill switch and must reach the servers");

    queryOptions.clear();
    queryOptions.put(QueryOptionKey.STREAMING_DISTINCT_MAX_TRACKED_CARDINALITY, "4096");
    killed.applyBrokerDefaultQueryOptions(queryOptions);
    assertEquals(queryOptions.get(QueryOptionKey.STREAMING_DISTINCT_MAX_TRACKED_CARDINALITY), "4096",
        "Per-query SET must take precedence over the cluster default");
  }

  private static MultiStageBrokerRequestHandler newHandlerWithMaxTrackedCardinality(
      @Nullable String maxTrackedCardinality)
      throws Exception {
    return newHandlerWithFlushThresholds(null, null, null, maxTrackedCardinality);
  }

  private static MultiStageBrokerRequestHandler newHandlerWithEstimatedExitStdDev(
      @Nullable String estimatedExitStdDev)
      throws Exception {
    return newHandlerWithFlushThresholds(null, null, estimatedExitStdDev, null);
  }

  private static MultiStageBrokerRequestHandler newHandlerWithStreamingGroupByFlushThreshold(
      @Nullable String streamingGroupByFlushThreshold)
      throws Exception {
    return newHandlerWithFlushThresholds(streamingGroupByFlushThreshold, null, null, null);
  }

  private static MultiStageBrokerRequestHandler newHandlerWithStreamingDistinctFlushThreshold(
      @Nullable String streamingDistinctFlushThreshold)
      throws Exception {
    return newHandlerWithFlushThresholds(null, streamingDistinctFlushThreshold, null, null);
  }

  private static MultiStageBrokerRequestHandler newHandlerWithFlushThresholds(
      @Nullable String streamingGroupByFlushThreshold, @Nullable String streamingDistinctFlushThreshold,
      @Nullable String estimatedExitStdDev, @Nullable String maxTrackedCardinality)
      throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    if (streamingGroupByFlushThreshold != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_GROUP_BY_FLUSH_THRESHOLD,
          streamingGroupByFlushThreshold);
    }
    if (streamingDistinctFlushThreshold != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_DISTINCT_FLUSH_THRESHOLD,
          streamingDistinctFlushThreshold);
    }
    if (estimatedExitStdDev != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_DISTINCT_ESTIMATED_EXIT_STD_DEV,
          estimatedExitStdDev);
    }
    if (maxTrackedCardinality != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_DISTINCT_MAX_TRACKED_CARDINALITY,
          maxTrackedCardinality);
    }
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    return new MultiStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
        mock(RoutingManager.class), new AllowAllAccessControlFactory(), queryQuotaManager,
        mock(TableCache.class), mock(MultiStageQueryThrottler.class), mock(FailureDetector.class),
        ThreadAccountantUtils.getNoOpAccountant(), null, mock(WorkerManager.class), mock(WorkerManager.class)) {
      @Override
      public void start() {
      }

      @Override
      public void shutDown() {
      }
    };
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
