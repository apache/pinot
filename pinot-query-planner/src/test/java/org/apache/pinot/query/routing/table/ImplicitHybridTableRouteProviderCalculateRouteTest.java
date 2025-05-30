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
package org.apache.pinot.query.routing.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Test class for {@link ImplicitHybridTableRouteProvider} to test the routing table calculation (calculateRoutes)
 */
public class ImplicitHybridTableRouteProviderCalculateRouteTest extends BaseTableRouteTest {
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

  private TableRouteInfo getImplicitHybridTableRouteInfo(String tableName) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(tableName, _tableCache, _routingManager);
    BrokerRequestPair brokerRequestPair =
        getBrokerRequestPair(tableName, routeInfo.hasOffline(), routeInfo.hasRealtime(),
            routeInfo.getOfflineTableName(), routeInfo.getRealtimeTableName());

    _hybridTableRouteProvider.calculateRoutes(routeInfo, _routingManager, brokerRequestPair._offlineBrokerRequest,
        brokerRequestPair._realtimeBrokerRequest, 0);
    return routeInfo;
  }

  private void assertTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable, boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteInfo routeInfo = getImplicitHybridTableRouteInfo(tableName);

    // If a routing table for offline table is expected, then compare it with the expected routing table.
    if (isOfflineExpected) {
      assertNotNull(routeInfo.getOfflineRoutingTable());
      assertFalse(routeInfo.getOfflineRoutingTable().isEmpty());
      assertEquals(routeInfo.getOfflineRoutingTable().size(), expectedOfflineRoutingTable.size());
      assertRoutingTableEqual(routeInfo.getOfflineRoutingTable(), expectedOfflineRoutingTable);
    } else {
      assertNull(routeInfo.getOfflineRoutingTable());
    }

    // If a routing table for realtime table is expected, then compare it with the expected routing table.
    if (isRealtimeExpected) {
      assertNotNull(routeInfo.getRealtimeRoutingTable());
      assertFalse(routeInfo.getRealtimeRoutingTable().isEmpty());
      assertEquals(routeInfo.getRealtimeRoutingTable().size(), expectedRealtimeRoutingTable.size());
      assertRoutingTableEqual(routeInfo.getRealtimeRoutingTable(), expectedRealtimeRoutingTable);
    } else {
      assertNull(routeInfo.getRealtimeRoutingTable());
    }

    //TODO: There are no meaningful tests to check unavailable segments and pruned segments.
    assertTrue(routeInfo.getUnavailableSegments().isEmpty());
    assertEquals(routeInfo.getNumPrunedSegmentsTotal(), 0);

    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
    } else {
      assertFalse(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
      // Check requestMap
      Map<ServerRoutingInstance, InstanceRequest> requestMap = routeInfo.getRequestMap(0, "broker", false);
      assertNotNull(requestMap);
      assertFalse(requestMap.isEmpty());
    }
  }

  @Test(dataProvider = "offlineTableAndRouteProvider")
  void testOfflineTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableAndRouteProvider")
  void testRealtimeTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableAndRouteProvider")
  void testHybridTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, expectedOfflineRoutingTable, expectedRealtimeRoutingTable,
        expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testRouteNotExists(String tableName) {
    assertTableRoute(tableName, null, null, false, false);
  }

  @Test(dataProvider = "partiallyDisabledTableAndRouteProvider")
  void testPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, expectedOfflineRoutingTable, expectedRealtimeRoutingTable,
        expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "disabledTableProvider")
  void testDisabledTable(String tableName) {
    assertTableRoute(tableName, null, null, false, false);
  }

  private static class GetTableRouteResult {
    public final BrokerRequest _offlineBrokerRequest;
    public final BrokerRequest _realtimeBrokerRequest;
    public final Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
    public final Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;
    public final List<String> _unavailableSegments;
    public final int _numPrunedSegmentsTotal;
    public final boolean _offlineTableDisabled;
    public final boolean _realtimeTableDisabled;

    public GetTableRouteResult(BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest,
        Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
        Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, List<String> unavailableSegments,
        int numPrunedSegmentsTotal, boolean offlineTableDisabled, boolean realtimeTableDisabled) {
      _offlineBrokerRequest = offlineBrokerRequest;
      _realtimeBrokerRequest = realtimeBrokerRequest;
      _offlineRoutingTable = offlineRoutingTable;
      _realtimeRoutingTable = realtimeRoutingTable;
      _unavailableSegments = unavailableSegments;
      _numPrunedSegmentsTotal = numPrunedSegmentsTotal;
      _offlineTableDisabled = offlineTableDisabled;
      _realtimeTableDisabled = realtimeTableDisabled;
    }
  }

  /**
   * This is a copy of the section in BaseSingleStageBrokerRequestHandlerTest.doHandleRequest which gets the
   * routing tables for offline and realtime tables. It also get the list of unavailable segments and the number of
   * pruned segments.
   * Note that an important side effect of this method is that it sets the offlineBrokerRequest and
   * realtimeBrokerRequest to null if the routing table is empty. This is used in subsequent code to determine if
   * either of the physical tables is available
   * @param tableName table name
   * @param routingManager Routing manager
   * @return GetTableRouteResult containing the routing tables, unavailable segments and number of pruned segments
   */
  private static GetTableRouteResult getTableRouting(String tableName, RoutingManager routingManager) {
// Get the tables hit by the request
    String offlineTableName = null;
    String realtimeTableName = null;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == TableType.OFFLINE) {
      // Offline table
      if (routingManager.routingExists(tableName)) {
        offlineTableName = tableName;
      }
    } else if (tableType == TableType.REALTIME) {
      // Realtime table
      if (routingManager.routingExists(tableName)) {
        realtimeTableName = tableName;
      }
    } else {
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (routingManager.routingExists(offlineTableNameToCheck)) {
        offlineTableName = offlineTableNameToCheck;
      }
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (routingManager.routingExists(realtimeTableNameToCheck)) {
        realtimeTableName = realtimeTableNameToCheck;
      }
    }

    Map<ServerInstance, ServerRouteInfo> offlineRoutingTable = null;
    Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable = null;
    BrokerRequestPair brokerRequestPair =
        getBrokerRequestPair(tableName, offlineTableName != null, realtimeTableName != null, offlineTableName,
            realtimeTableName);
    BrokerRequest offlineBrokerRequest = brokerRequestPair._offlineBrokerRequest;
    BrokerRequest realtimeBrokerRequest = brokerRequestPair._realtimeBrokerRequest;

    List<String> unavailableSegments = new ArrayList<>();
    int numPrunedSegmentsTotal = 0;
    boolean offlineTableDisabled = false;
    boolean realtimeTableDisabled = false;

    if (offlineBrokerRequest != null) {
      offlineTableDisabled = routingManager.isTableDisabled(offlineTableName);
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!offlineTableDisabled) {
        routingTable = routingManager.getRoutingTable(offlineBrokerRequest, 0);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      realtimeTableDisabled = routingManager.isTableDisabled(realtimeTableName);
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!realtimeTableDisabled) {
        routingTable = routingManager.getRoutingTable(realtimeBrokerRequest, 0);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        realtimeBrokerRequest = null;
      }
    }

    return new GetTableRouteResult(offlineBrokerRequest, realtimeBrokerRequest, offlineRoutingTable,
        realtimeRoutingTable, unavailableSegments, numPrunedSegmentsTotal, offlineTableDisabled,
        realtimeTableDisabled);
  }

  /**
   * Checks if two table routes are the same. A expected routingTable is a Map<String, Set<String>> where the key is the
   * server name and the value is a set of segments. This is compared to the routing table
   * Map<ServerInstance, ServerRouteInfo>
   * @param tableName
   * @param expectedOfflineRoutingTable
   * @param expectedRealtimeRoutingTable
   * @param isOfflineExpected
   * @param isRealtimeExpected
   */
  private void assertEqualsTableRouteInfoGetTableRouteResult(String tableName,
      Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable, boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteInfo routeInfo = getImplicitHybridTableRouteInfo(tableName);
    GetTableRouteResult expectedTableRoute = getTableRouting(tableName, _routingManager);

    if (isOfflineExpected) {
      assertNotNull(routeInfo.getOfflineRoutingTable());
      assertFalse(routeInfo.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(), routeInfo.getOfflineRoutingTable().entrySet());
      assertRoutingTableEqual(routeInfo.getOfflineRoutingTable(), expectedOfflineRoutingTable);
    } else {
      assertNull(routeInfo.getOfflineRoutingTable());
    }

    if (isRealtimeExpected) {
      assertNotNull(routeInfo.getRealtimeRoutingTable());
      assertFalse(routeInfo.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(), routeInfo.getRealtimeRoutingTable().entrySet());
      assertRoutingTableEqual(routeInfo.getRealtimeRoutingTable(), expectedRealtimeRoutingTable);
    } else {
      assertNull(routeInfo.getRealtimeRoutingTable());
    }

    assertEquals(routeInfo.getUnavailableSegments(), expectedTableRoute._unavailableSegments);
    assertEquals(routeInfo.getNumPrunedSegmentsTotal(), expectedTableRoute._numPrunedSegmentsTotal);
    if (!isOfflineExpected && !isRealtimeExpected) {
      assertFalse(routeInfo.isRouteExists());
    } else {
      assertTrue(routeInfo.isRouteExists());
    }
  }

  @Test(dataProvider = "offlineTableAndRouteProvider")
  void testTableRoutingForOffline(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertEqualsTableRouteInfoGetTableRouteResult(tableName, expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableAndRouteProvider")
  void testTableRoutingForRealtime(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertEqualsTableRouteInfoGetTableRouteResult(tableName, null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableAndRouteProvider")
  void testTableRoutingForHybrid(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertEqualsTableRouteInfoGetTableRouteResult(tableName, expectedOfflineRoutingTable, expectedRealtimeRoutingTable,
        expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testTableRoutingForRouteNotExists(String tableName) {
    TableRouteInfo routeInfo = getImplicitHybridTableRouteInfo(tableName);
    GetTableRouteResult expectedTableRoute = getTableRouting(tableName, _routingManager);

    assertNull(expectedTableRoute._offlineRoutingTable);
    assertNull(expectedTableRoute._realtimeRoutingTable);

    assertNull(routeInfo.getOfflineRoutingTable());
    assertNull(routeInfo.getRealtimeRoutingTable());
    assertTrue(routeInfo.getUnavailableSegments().isEmpty());
    assertEquals(routeInfo.getNumPrunedSegmentsTotal(), 0);
    assertFalse(routeInfo.isRouteExists());
  }

  @Test(dataProvider = "partiallyDisabledTableAndRouteProvider")
  void testTableRoutingForPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    TableRouteInfo routeInfo = getImplicitHybridTableRouteInfo(tableName);
    GetTableRouteResult expectedTableRoute = getTableRouting(tableName, _routingManager);

    if (expectedOfflineRoutingTable == null) {
      assertNull(routeInfo.getOfflineRoutingTable());
      assertNull(expectedTableRoute._offlineRoutingTable);
    } else {
      assertFalse(routeInfo.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertFalse(expectedTableRoute._offlineTableDisabled);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(), routeInfo.getOfflineRoutingTable().entrySet());
      assertRoutingTableEqual(routeInfo.getOfflineRoutingTable(), expectedOfflineRoutingTable);
    }

    if (expectedRealtimeRoutingTable == null) {
      assertNull(routeInfo.getRealtimeRoutingTable());
      assertNull(expectedTableRoute._realtimeRoutingTable);
    } else {
      assertFalse(routeInfo.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertFalse(expectedTableRoute._realtimeTableDisabled);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(), routeInfo.getRealtimeRoutingTable().entrySet());
      assertRoutingTableEqual(routeInfo.getRealtimeRoutingTable(), expectedRealtimeRoutingTable);
    }
  }

  @Test(dataProvider = "disabledTableProvider")
  void testTableRoutingForDisabledTable(String tableName) {
    TableRouteInfo routeInfo = getImplicitHybridTableRouteInfo(tableName);
    GetTableRouteResult expectedTableRoute = getTableRouting(tableName, _routingManager);

    if (expectedTableRoute._offlineTableDisabled) {
      assertNull(routeInfo.getOfflineRoutingTable());
    } else if (expectedTableRoute._offlineRoutingTable != null) {
      assertNotNull(routeInfo.getOfflineRoutingTable());
    }

    if (expectedTableRoute._realtimeTableDisabled) {
      assertNull(routeInfo.getRealtimeRoutingTable());
    } else if (expectedTableRoute._realtimeRoutingTable != null) {
      assertNotNull(routeInfo.getRealtimeRoutingTable());
    }
  }
}
