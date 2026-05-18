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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class MultiStageBrokerRequestHandlerTest {

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
  public void testBuildEmptyBrokerResponseWhenAllSegmentsPruned()
      throws Exception {
    // Verify that when all segments are pruned (no real servers to dispatch to),
    // buildEmptyBrokerResponse returns a valid response with the correct schema,
    // zero rows, and zero servers queried/responded.

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    MultiStageQueryThrottler throttler = mock(MultiStageQueryThrottler.class);

    MultiStageBrokerRequestHandler handler =
        new MultiStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
            mock(RoutingManager.class), new AllowAllAccessControlFactory(), queryQuotaManager,
            mock(TableCache.class), throttler, mock(FailureDetector.class),
            ThreadAccountantUtils.getNoOpAccountant(), null, mock(WorkerManager.class), mock(WorkerManager.class)) {
          @Override
          public void start() {
            // Skip dispatcher.start() and Calcite warmupCompile
          }

          @Override
          public void shutDown() {
          }
        };

    // Construct a DispatchableSubPlan that simulates all segments being pruned:
    // - Only stage 0 (broker reduce) exists with one column in the result schema
    // - No real servers are assigned (empty serverInstanceToWorkerIdMap)
    DataSchema rootSchema = new DataSchema(
        new String[]{"col1", "col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING}
    );

    PlanNode mockRootPlanNode = mock(PlanNode.class);
    when(mockRootPlanNode.getDataSchema()).thenReturn(rootSchema);
    when(mockRootPlanNode.getStageId()).thenReturn(0);

    PlanFragment rootFragment = new PlanFragment(0, mockRootPlanNode, List.of());
    DispatchablePlanFragment rootStage = new DispatchablePlanFragment(rootFragment);

    // Result fields: map column index -> column name
    PairList<Integer, String> resultFields = PairList.copyOf(0, "col1", 1, "col2");

    Map<Integer, DispatchablePlanFragment> queryStageMap = new HashMap<>();
    queryStageMap.put(0, rootStage);

    long numPrunedSegments = 42;
    DispatchableSubPlan subPlan = new DispatchableSubPlan(
        resultFields, queryStageMap, Set.of("testTable_OFFLINE"),
        Collections.emptyMap(), numPrunedSegments);

    // Mock the compiled query
    QueryEnvironment.CompiledQuery compiledQuery = mock(QueryEnvironment.CompiledQuery.class);
    SqlNodeAndOptions sqlNodeAndOptions = mock(SqlNodeAndOptions.class);
    when(sqlNodeAndOptions.getOptions()).thenReturn(new HashMap<>());
    when(compiledQuery.getSqlNodeAndOptions()).thenReturn(sqlNodeAndOptions);
    when(compiledQuery.getOptions()).thenReturn(new HashMap<>());

    // Mock request context
    RequestContext requestContext = mock(RequestContext.class);
    when(requestContext.getRequestArrivalTimeMillis()).thenReturn(System.currentTimeMillis());

    // Call the fast-path method
    BrokerResponse response = handler.buildEmptyBrokerResponse(
        subPlan, Set.of("testTable_OFFLINE"), requestContext, compiledQuery, false, null, false);

    // Verify the response is a BrokerResponseNativeV2 with correct properties
    Assert.assertTrue(response instanceof BrokerResponseNativeV2);
    BrokerResponseNativeV2 v2Response = (BrokerResponseNativeV2) response;

    // Verify schema is correct
    ResultTable resultTable = v2Response.getResultTable();
    Assert.assertNotNull(resultTable, "Result table should not be null");
    DataSchema schema = resultTable.getDataSchema();
    Assert.assertEquals(schema.getColumnNames(), new String[]{"col1", "col2"});
    Assert.assertEquals(schema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

    // Verify zero rows
    Assert.assertEquals(resultTable.getRows().size(), 0, "Should have zero result rows");

    // Verify server counts
    Assert.assertEquals(v2Response.getNumServersQueried(), 0, "Should have zero servers queried");
    Assert.assertEquals(v2Response.getNumServersResponded(), 0, "Should have zero servers responded");

    // Verify no exceptions
    Assert.assertTrue(v2Response.getExceptions().isEmpty(), "Should have no exceptions");

    // Verify that the query throttler was never called (the fast-path skips it)
    verify(throttler, never()).tryAcquire(org.mockito.ArgumentMatchers.anyInt(),
        org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.any());
    verify(throttler, never()).release(org.mockito.ArgumentMatchers.anyInt());
  }
}
