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
package org.apache.pinot.core.routing;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LogicalTableRouteProviderCalculateRouteTest extends BaseTableRouteTest {
  private QueryThreadContext.CloseableContext _closeableContext;

  @BeforeMethod
  public void setupQueryThreadContext() {
    _closeableContext = QueryThreadContext.open();
  }

  @AfterMethod
  void closeQueryThreadContext() {
    if (_closeableContext != null) {
      _closeableContext.close();
      _closeableContext = null;
    }
  }

  private void assertTableRoute(String tableName, String logicalTableName,
      Map<String, Set<String>> expectedOfflineRoutingTable, Map<String, Set<String>> expectedRealtimeRoutingTable,
      boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, logicalTableName);
    BrokerRequestPair brokerRequestPair =
        getBrokerRequestPair(logicalTableName, routeInfo.hasOffline(), routeInfo.hasRealtime(),
            routeInfo.getOfflineTableName(), routeInfo.getRealtimeTableName());

    _logicalTableRouteProvider.calculateRoutes(routeInfo, _routingManager, brokerRequestPair._offlineBrokerRequest,
        brokerRequestPair._realtimeBrokerRequest, 0);
    LogicalTableRouteInfo logicalTableRouteInfo = (LogicalTableRouteInfo) routeInfo;

    if (isOfflineExpected) {
      assertNotNull(logicalTableRouteInfo.getOfflineTables());
      assertEquals(logicalTableRouteInfo.getOfflineTables().size(), 1);
      Map<ServerInstance, SegmentsToQuery> offlineRoutingTable =
          logicalTableRouteInfo.getOfflineTables().get(0).getOfflineRoutingTable();
      assertNotNull(offlineRoutingTable);
      assertRoutingTableEqual(offlineRoutingTable, expectedOfflineRoutingTable);
    } else {
      assertTrue(logicalTableRouteInfo.getOfflineExecutionServers().isEmpty());
    }

    if (isRealtimeExpected) {
      assertNotNull(logicalTableRouteInfo.getRealtimeTables());
      assertEquals(logicalTableRouteInfo.getRealtimeTables().size(), 1);
      Map<ServerInstance, SegmentsToQuery> realtimeRoutingTable =
          logicalTableRouteInfo.getRealtimeTables().get(0).getRealtimeRoutingTable();
      assertNotNull(realtimeRoutingTable);
      assertRoutingTableEqual(realtimeRoutingTable, expectedRealtimeRoutingTable);
    } else {
      assertTrue(logicalTableRouteInfo.getRealtimeExecutionServers().isEmpty());
    }

    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
    } else {
      assertFalse(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
      // Check requestMap
      Map<ServerRoutingInstance, InstanceRequest> requestMap = routeInfo.getRequestMap(0, "broker", false);
      assertNotNull(requestMap);
      assertFalse(requestMap.isEmpty());
    }

    if (routeInfo.isHybrid()) {
      assertNotNull(routeInfo.getTimeBoundaryInfo(), "Time boundary info should not be null for hybrid table");
    } else {
      assertNull(routeInfo.getTimeBoundaryInfo(), "Time boundary info should be null for non-hybrid table");
    }
  }

  @Test(dataProvider = "offlineTableAndRouteProvider")
  void testOfflineTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, "offlineTableAndRouteProvider", expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableAndRouteProvider")
  void testRealtimeTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, "realtimeTableAndRouteProvider", null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableAndRouteProvider")
  void testHybridTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, "hybridTableAndRouteProvider", expectedOfflineRoutingTable,
        expectedRealtimeRoutingTable, expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testRouteNotExists(String tableName) {
    assertTableRoute(tableName, "routeNotExistsProvider", null, null, false, false);
  }

  @Test(dataProvider = "partiallyDisabledTableAndRouteProvider")
  void testPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, "partiallyDisabledTableAndRouteProvider", expectedOfflineRoutingTable,
        expectedRealtimeRoutingTable, expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "disabledTableProvider")
  void testDisabledTable(String tableName) {
    assertTableRoute(tableName, "disabledTableProvider", null, null, false, false);
  }
}
