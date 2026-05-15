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
import java.util.Map;
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
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
  public void testApplyBrokerDefaultQueryOptionsInjectsStreamingGroupByFlushThreshold()
      throws Exception {
    // When the broker config is set, the option is injected for queries that don't already specify it.
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingGroupByFlushThreshold("5000");

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertEquals(queryOptions.get(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD), "5000",
        "Broker default should be injected when query option is absent");
  }

  @Test
  public void testApplyBrokerDefaultQueryOptionsPerQueryOverrideWins()
      throws Exception {
    // A per-query SET — including SET = 0 to disable — must take precedence over the broker default.
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
    // With the broker config unset (default 0), no option is injected.
    MultiStageBrokerRequestHandler handler = newHandlerWithStreamingGroupByFlushThreshold(null);

    Map<String, String> queryOptions = new HashMap<>();
    handler.applyBrokerDefaultQueryOptions(queryOptions);
    Assert.assertFalse(queryOptions.containsKey(QueryOptionKey.STREAMING_GROUP_BY_FLUSH_THRESHOLD),
        "No option should be injected when the broker default is unset");
  }

  private static MultiStageBrokerRequestHandler newHandlerWithStreamingGroupByFlushThreshold(
      @Nullable String streamingGroupByFlushThreshold)
      throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    if (streamingGroupByFlushThreshold != null) {
      config.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_STREAMING_GROUP_BY_FLUSH_THRESHOLD,
          streamingGroupByFlushThreshold);
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
        // Skip dispatcher.start() and Calcite warmupCompile — neither is needed for this test.
      }

      @Override
      public void shutDown() {
        // Match start() — no dispatcher was started, so there is nothing to shut down.
      }
    };
  }
}
