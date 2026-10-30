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

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteProvider;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Warmup counts tables only after data arrives and never passes timed-out server responses to the reducer.
public class BrokerWarmupResponseTest {
  @DataProvider(name = "responses")
  public Object[][] responses() {
    return new Object[][]{{"empty"}, {"timed-out"}, {"mixed"}};
  }

  @Test(dataProvider = "responses")
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testProbeOnlyWarmsAndReducesResponsesContainingData(String shape)
      throws Exception {
    RoutingManager routing = mock(RoutingManager.class);
    TableCache cache = mock(TableCache.class);
    ImplicitHybridTableRouteProvider provider = mock(ImplicitHybridTableRouteProvider.class);
    ImplicitHybridTableRouteInfo route = mock(ImplicitHybridTableRouteInfo.class);
    when(provider.getTableRouteInfo("events", cache, routing)).thenReturn(route);
    when(route.isExists()).thenReturn(true);
    when(route.hasOffline()).thenReturn(true);
    when(route.getOfflineTableName()).thenReturn("events_OFFLINE");
    when(route.isRouteExists()).thenReturn(true);
    ServerRoutingInstance timedOut = new ServerRoutingInstance("timeout", 8000, TableType.OFFLINE);
    ServerRoutingInstance successful = new ServerRoutingInstance("success", 8001, TableType.OFFLINE);
    DataTable data = mock(DataTable.class);
    Map<ServerRoutingInstance, ServerResponse> responses = new LinkedHashMap<>();
    if (!shape.equals("empty")) {
      responses.put(timedOut, mock(ServerResponse.class));
    }
    if (shape.equals("mixed")) {
      ServerResponse success = mock(ServerResponse.class);
      when(success.getDataTable()).thenReturn(data);
      responses.put(successful, success);
    }
    AsyncQueryResponse response = mock(AsyncQueryResponse.class);
    when(response.getFinalResponses()).thenReturn(responses);
    try (MockedStatic<BrokerQueryEventListenerFactory> listeners = mockStatic(BrokerQueryEventListenerFactory.class);
        MockedConstruction<QueryRouter> routers = mockConstruction(QueryRouter.class,
            (router, context) -> when(router.submitQuery(anyLong(), eq("events"), same(route), eq(100L)))
                .thenReturn(response));
        MockedConstruction<BrokerReduceService> reducers = mockConstruction(BrokerReduceService.class)) {
      listeners.when(BrokerQueryEventListenerFactory::getBrokerQueryEventListener)
          .thenReturn(mock(BrokerQueryEventListener.class));
      listeners.when(BrokerQueryEventListenerFactory::getTrackedHeaders).thenReturn(Set.of());
      SingleConnectionBrokerRequestHandler handler = new SingleConnectionBrokerRequestHandler(
          new PinotConfiguration(), "testBroker", new BrokerRequestIdGenerator(), routing,
          new AllowAllAccessControlFactory(), mock(QueryQuotaManager.class), cache, null, null,
          mock(ServerRoutingStatsManager.class), mock(FailureDetector.class), ThreadAccountantUtils.getNoOpAccountant(),
          mock(MultiClusterRoutingContext.class));
      handler._implicitHybridTableRouteProvider = provider;
      BrokerRequest request = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM events_OFFLINE LIMIT 1");
      Set<String> warmed = new HashSet<>();
      Method probe = SingleConnectionBrokerRequestHandler.class.getDeclaredMethod("probe", BrokerRequest.class,
          long.class, Set.class);
      probe.setAccessible(true);
      try {
        assertTrue((long) probe.invoke(handler, request, 100L, warmed) >= 0);
        verify(routers.constructed().get(0)).submitQuery(anyLong(), eq("events"), same(route), eq(100L));
        BrokerReduceService reducer = reducers.constructed().get(0);
        if (shape.equals("mixed")) {
          assertEquals(warmed, Set.of("events_OFFLINE"));
          ArgumentCaptor<Map<ServerRoutingInstance, DataTable>> reduced = ArgumentCaptor.forClass(Map.class);
          verify(reducer).reduceOnDataTable(same(request), same(request), reduced.capture(), eq(100L),
              any(BrokerMetrics.class));
          assertEquals(reduced.getValue(), Map.of(successful, data));
        } else {
          assertTrue(warmed.isEmpty());
          verify(reducer, never()).reduceOnDataTable(any(), any(), any(), anyLong(), any());
        }
      } finally {
        handler.shutDown();
      }
    }
  }
}
