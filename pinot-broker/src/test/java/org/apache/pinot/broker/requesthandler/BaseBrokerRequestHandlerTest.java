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

import java.util.Map;
import java.util.Set;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler.ServerStats;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.RequestContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests [BaseBrokerRequestHandler#getServerInstanceForRetry], covering local and federated
/// (multi-cluster) server lookup used during failure-detector retries.
public class BaseBrokerRequestHandlerTest {
  private static final String LOCAL_INSTANCE_ID = "Server_local_1";
  private static final String REMOTE_INSTANCE_ID = "Server_remote_1";
  private static final String UNKNOWN_INSTANCE_ID = "Server_unknown_1";

  @BeforeClass
  public void setUp() {
    BrokerMetrics.register(mock(BrokerMetrics.class));
    BrokerQueryEventListenerFactory.init(new PinotConfiguration());
  }

  private static BaseBrokerRequestHandler createHandler(RoutingManager routingManager,
      MultiClusterRoutingContext multiClusterRoutingContext) {
    PinotConfiguration config = new PinotConfiguration();
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    TableCache tableCache = mock(TableCache.class);
    return new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
        routingManager, new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
        ThreadAccountantUtils.getNoOpAccountant(), multiClusterRoutingContext) {
      @Override
      public void start() {
      }

      @Override
      public void shutDown() {
      }

      @Override
      protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
          BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
          RequestContext requestContext) {
        return new BrokerResponseNative();
      }
    };
  }

  @Test
  public void testGetServerInstanceForRetryReturnsLocalMatch() {
    ServerInstance localInstance = mock(ServerInstance.class);
    RoutingManager localRoutingManager = mock(RoutingManager.class);
    when(localRoutingManager.getEnabledServerInstanceMap()).thenReturn(
        Map.of(LOCAL_INSTANCE_ID, localInstance));

    RoutingManager multiClusterRoutingManager = mock(RoutingManager.class);
    when(multiClusterRoutingManager.getEnabledServerInstanceMap()).thenReturn(Map.of());
    MultiClusterRoutingContext multiClusterRoutingContext =
        new MultiClusterRoutingContext(Map.of(), localRoutingManager, multiClusterRoutingManager, Set.of());

    BaseBrokerRequestHandler handler = createHandler(localRoutingManager, multiClusterRoutingContext);
    Assert.assertSame(handler.getServerInstanceForRetry(LOCAL_INSTANCE_ID), localInstance);
  }

  @Test
  public void testGetServerInstanceForRetryFallsBackToRemoteRoutingManager() {
    RoutingManager localRoutingManager = mock(RoutingManager.class);
    when(localRoutingManager.getEnabledServerInstanceMap()).thenReturn(Map.of());

    ServerInstance remoteInstance = mock(ServerInstance.class);
    RoutingManager multiClusterRoutingManager = mock(RoutingManager.class);
    when(multiClusterRoutingManager.getEnabledServerInstanceMap()).thenReturn(
        Map.of(REMOTE_INSTANCE_ID, remoteInstance));
    MultiClusterRoutingContext multiClusterRoutingContext =
        new MultiClusterRoutingContext(Map.of(), localRoutingManager, multiClusterRoutingManager, Set.of());

    BaseBrokerRequestHandler handler = createHandler(localRoutingManager, multiClusterRoutingContext);
    Assert.assertSame(handler.getServerInstanceForRetry(REMOTE_INSTANCE_ID), remoteInstance);
  }

  @Test
  public void testGetServerInstanceForRetryReturnsNullWhenAbsentFromBothManagers() {
    RoutingManager localRoutingManager = mock(RoutingManager.class);
    when(localRoutingManager.getEnabledServerInstanceMap()).thenReturn(
        Map.of(LOCAL_INSTANCE_ID, mock(ServerInstance.class)));

    RoutingManager multiClusterRoutingManager = mock(RoutingManager.class);
    when(multiClusterRoutingManager.getEnabledServerInstanceMap()).thenReturn(
        Map.of(REMOTE_INSTANCE_ID, mock(ServerInstance.class)));
    MultiClusterRoutingContext multiClusterRoutingContext =
        new MultiClusterRoutingContext(Map.of(), localRoutingManager, multiClusterRoutingManager, Set.of());

    BaseBrokerRequestHandler handler = createHandler(localRoutingManager, multiClusterRoutingContext);
    Assert.assertNull(handler.getServerInstanceForRetry(UNKNOWN_INSTANCE_ID));
  }

  @Test
  public void testGetServerInstanceForRetryReturnsNullWhenNoMultiClusterRoutingContext() {
    RoutingManager localRoutingManager = mock(RoutingManager.class);
    when(localRoutingManager.getEnabledServerInstanceMap()).thenReturn(Map.of());

    BaseBrokerRequestHandler handler = createHandler(localRoutingManager, null);
    Assert.assertNull(handler.getServerInstanceForRetry(UNKNOWN_INSTANCE_ID));
  }

  @Test
  public void testGetServerInstanceForRetryReturnsNullWhenMultiClusterRoutingManagerNotConfigured() {
    RoutingManager localRoutingManager = mock(RoutingManager.class);
    when(localRoutingManager.getEnabledServerInstanceMap()).thenReturn(Map.of());

    MultiClusterRoutingContext multiClusterRoutingContext =
        new MultiClusterRoutingContext(Map.of(), localRoutingManager, null, Set.of());

    BaseBrokerRequestHandler handler = createHandler(localRoutingManager, multiClusterRoutingContext);
    Assert.assertNull(handler.getServerInstanceForRetry(UNKNOWN_INSTANCE_ID));
  }
}
