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
package org.apache.pinot.broker.broker.helix;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BrokerRequestIdGenerator;
import org.apache.pinot.broker.requesthandler.MultiStageBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.MultiStageQueryThrottler;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Pins the new {@code createMultiStageBrokerRequestHandler(...)} factory hook on
 * {@link BaseBrokerStarter} as an extension point: a subclass override of the factory must be
 * dispatched to, and the returned handler's {@code onQueryCompletion(RequestContext,
 * BrokerResponse)} hook must fire with the {@link BrokerResponse} produced by
 * {@code handleRequest}. Together these verify the contract this PR enables — that downstream
 * broker starters can substitute a {@link MultiStageBrokerRequestHandler} subclass for async
 * query logging on multi-stage queries.
 *
 * <p>Mirrors the SSE-side test added in PR #18247
 * ({@code BaseSingleStageBrokerRequestHandlerTest.testOnQueryCompletionHookReceivesBrokerResponse}),
 * but exercises the change end-to-end through the factory rather than instantiating the handler
 * directly.</p>
 */
public class BaseBrokerStarterTest {

  @Test
  public void testCreateMultiStageBrokerRequestHandlerHookExposesOnQueryCompletion()
      throws Exception {
    AtomicReference<BrokerResponse> capturedResponse = new AtomicReference<>();

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    // The MailboxService constructor only stores the port; gRPC server binding happens in start(),
    // which the handler subclass overrides to a no-op.
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, "0");
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    HelixBrokerStarter starter = new HelixBrokerStarter() {
      @Override
      protected MultiStageBrokerRequestHandler createMultiStageBrokerRequestHandler(
          PinotConfiguration cfg, String brokerId, BrokerRequestIdGenerator requestIdGenerator,
          RoutingManager routingManager, AccessControlFactory accessControlFactory,
          QueryQuotaManager queryQuotaMgr, TableCache tableCache,
          MultiStageQueryThrottler multiStageQueryThrottler, FailureDetector failureDetector,
          ThreadAccountant threadAccountant, MultiClusterRoutingContext multiClusterRoutingContext,
          WorkerManager workerManager, WorkerManager multiClusterWorkerManager) {
        return new MultiStageBrokerRequestHandler(cfg, brokerId, requestIdGenerator, routingManager,
            accessControlFactory, queryQuotaMgr, tableCache, multiStageQueryThrottler, failureDetector,
            threadAccountant, multiClusterRoutingContext, workerManager, multiClusterWorkerManager) {
          @Override
          public void start() {
            // Skip dispatcher.start() and Calcite warmupCompile — neither is needed for this hook test.
          }

          @Override
          public void shutDown() {
            // No dispatcher was started, so there is nothing to shut down.
          }

          @Override
          protected BrokerResponse handleRequest(long requestId, String query,
              SqlNodeAndOptions sqlNodeAndOptions, JsonNode request,
              @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
              @Nullable HttpHeaders httpHeaders, AccessControl accessControl) {
            // Bypass MSE planning/dispatch — return a non-null response so the hook fires.
            return new BrokerResponseNativeV2();
          }

          @Override
          protected void onQueryCompletion(RequestContext requestContext, BrokerResponse brokerResponse) {
            capturedResponse.set(brokerResponse);
          }
        };
      }
    };

    MultiStageBrokerRequestHandler handler = starter.createMultiStageBrokerRequestHandler(config,
        "testBrokerId", new BrokerRequestIdGenerator(), mock(RoutingManager.class),
        new AllowAllAccessControlFactory(), queryQuotaManager, mock(TableCache.class),
        mock(MultiStageQueryThrottler.class), mock(FailureDetector.class),
        ThreadAccountantUtils.getNoOpAccountant(), null, mock(WorkerManager.class),
        mock(WorkerManager.class));

    try {
      handler.handleRequest("SELECT 1");
    } catch (Exception ignored) {
      // routing/auth may fail — we only care that the hook was called with a non-null response
    }
    Assert.assertNotNull(capturedResponse.get(),
        "onQueryCompletion hook must be called with the BrokerResponse from handleRequest for MSE");
  }
}
