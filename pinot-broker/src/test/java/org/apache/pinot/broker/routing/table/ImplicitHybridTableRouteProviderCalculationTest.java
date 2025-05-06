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
package org.apache.pinot.broker.routing.table;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.query.routing.table.ImplicitHybridTableRouteProvider;
import org.apache.pinot.query.routing.table.TableRouteProvider;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ImplicitHybridTableRouteProviderCalculationTest extends BaseTableRouteTest {
  private static final String QUERY_FORMAT = "SELECT col1, col2 FROM %s LIMIT 10";
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

  @DataProvider(name = "offlineTableAndRouteProvider")
  public static Object[][] offlineTableAndRouteProvider() {
    //@formatter:off
    return new Object[][] {
        {"b_OFFLINE", Map.of("Server_localhost_2", ImmutableSet.of("b2"))},
        {"c_OFFLINE", Map.of("Server_localhost_1", ImmutableSet.of("c1"),
            "Server_localhost_2", ImmutableSet.of("c2", "c3"))},
        {"d_OFFLINE", Map.of("Server_localhost_1", ImmutableSet.of("d1"),
            "Server_localhost_2", ImmutableSet.of("d3"))},
        {"e_OFFLINE", Map.of("Server_localhost_1", ImmutableSet.of("e1"),
            "Server_localhost_2", ImmutableSet.of("e3"))},
    };
    //@formatter:on
  }

  @DataProvider(name = "realtimeTableAndRouteProvider")
  public static Object[][] realtimeTableAndRouteProvider() {
    //@formatter:off
    return new Object[][] {
        {"a_REALTIME", Map.of("Server_localhost_1", ImmutableSet.of("a1", "a2"),
            "Server_localhost_2", ImmutableSet.of("a3"))},
        {"b_REALTIME", Map.of("Server_localhost_1", ImmutableSet.of("b1"))},
        {"e_REALTIME", Map.of("Server_localhost_2", ImmutableSet.of("e2"))},
    };
    //@formatter:on
  }

  @DataProvider(name = "hybridTableAndRouteProvider")
  public static Object[][] hybridTableAndRouteProvider() {
    //@formatter:off
    return new Object[][] {
        {"d", Map.of("Server_localhost_1", ImmutableSet.of("d1"),
            "Server_localhost_2", ImmutableSet.of("d3")), null},
        {"e", Map.of("Server_localhost_1", ImmutableSet.of("e1"),
            "Server_localhost_2", ImmutableSet.of("e3")),
            Map.of("Server_localhost_2", ImmutableSet.of("e2"))},
    };
    //@formatter:on
  }

  @DataProvider(name = "partiallyDisabledTableAndRouteProvider")
  public static Object[][] partiallyDisabledTableAndRouteProvider() {
    //@formatter:off
    return new Object[][] {
        {"hybrid_o_disabled", null, Map.of("Server_localhost_1", ImmutableSet.of("hor1"),
            "Server_localhost_2", ImmutableSet.of("hor2"))},
        {"hybrid_r_disabled", Map.of("Server_localhost_1", ImmutableSet.of("hro1"),
            "Server_localhost_2", ImmutableSet.of("hro2")), null},
    };
    //@formatter:on
  }

  private void assertTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable, boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteProvider routeComputer = ImplicitHybridTableRouteProvider.create(tableName, _tableCache, _routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;

    if (routeComputer.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(routeComputer.getOfflineTableName());
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (routeComputer.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(routeComputer.getRealtimeTableName());
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    TableRouteInfo
        tableRouteInfo = routeComputer.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest,
        0);

    // If a routing table for offline table is expected, then compare it with the expected routing table.
    if (isOfflineExpected) {
      assertNotNull(tableRouteInfo.getOfflineRoutingTable());
      assertFalse(tableRouteInfo.getOfflineRoutingTable().isEmpty());
      assertEquals(tableRouteInfo.getOfflineRoutingTable().size(), expectedOfflineRoutingTable.size());
      assertRoutingTableEqual(tableRouteInfo.getOfflineRoutingTable(), expectedOfflineRoutingTable);
    } else {
      assertNull(tableRouteInfo.getOfflineRoutingTable());
    }

    // If a routing table for realtime table is expected, then compare it with the expected routing table.
    if (isRealtimeExpected) {
      assertNotNull(tableRouteInfo.getRealtimeRoutingTable());
      assertFalse(tableRouteInfo.getRealtimeRoutingTable().isEmpty());
      assertEquals(tableRouteInfo.getRealtimeRoutingTable().size(), expectedRealtimeRoutingTable.size());
      assertRoutingTableEqual(tableRouteInfo.getRealtimeRoutingTable(), expectedRealtimeRoutingTable);
    } else {
      assertNull(tableRouteInfo.getRealtimeRoutingTable());
    }

    //TODO: There are no meaningful tests to check unavailable segments and pruned segments.
    assertTrue(routeComputer.getUnavailableSegments().isEmpty());
    assertEquals(routeComputer.getNumPrunedSegmentsTotal(), 0);

    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(tableRouteInfo.getOfflineBrokerRequest() == null && tableRouteInfo.getRealtimeBrokerRequest() == null);
    } else {
      assertFalse(tableRouteInfo.getOfflineBrokerRequest() == null
          && tableRouteInfo.getRealtimeBrokerRequest() == null);
      // Check requestMap
      Map<ServerRoutingInstance, InstanceRequest> requestMap = tableRouteInfo.getRequestMap(0, "broker", false);
      assertNotNull(requestMap);
      assertFalse(requestMap.isEmpty());
    }
  }

  private static void assertRoutingTableEqual(Map<ServerInstance, ServerRouteInfo> routeComputer,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routeComputer.entrySet()) {
      ServerInstance serverInstance = entry.getKey();
      ServerRouteInfo serverRouteInfo = entry.getValue();
      Set<String> segments = ImmutableSet.copyOf(serverRouteInfo.getSegments());
      assertTrue(expectedRealtimeRoutingTable.containsKey(serverInstance.toString()));
      assertEquals(expectedRealtimeRoutingTable.get(serverInstance.toString()), segments);
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
   * @param offlineBrokerRequest Offline broker request
   * @param realtimeBrokerRequest Realtime broker request
   * @param offlineTableName Offline table name
   * @param realtimeTableName Realtime table name
   * @param routingManager Routing manager
   * @return GetTableRouteResult containing the routing tables, unavailable segments and number of pruned segments
   */
  private static GetTableRouteResult getTableRouting(BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, String offlineTableName, String realtimeTableName,
      RoutingManager routingManager) {
    Map<ServerInstance, ServerRouteInfo> offlineRoutingTable = null;
    Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable = null;
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

  private void assertTableRouting(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable, boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteProvider routeComputer = ImplicitHybridTableRouteProvider.create(tableName, _tableCache, _routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (routeComputer.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (routeComputer.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);
    TableRouteInfo tableRouteInfo =
        routeComputer.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (isOfflineExpected) {
      assertNotNull(tableRouteInfo.getOfflineRoutingTable());
      assertFalse(tableRouteInfo.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(),
          tableRouteInfo.getOfflineRoutingTable().entrySet());
      assertRoutingTableEqual(tableRouteInfo.getOfflineRoutingTable(), expectedOfflineRoutingTable);
    } else {
      assertNull(tableRouteInfo.getOfflineRoutingTable());
    }

    if (isRealtimeExpected) {
      assertNotNull(tableRouteInfo.getRealtimeRoutingTable());
      assertFalse(tableRouteInfo.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(),
          tableRouteInfo.getRealtimeRoutingTable().entrySet());
      assertRoutingTableEqual(tableRouteInfo.getRealtimeRoutingTable(), expectedRealtimeRoutingTable);
    } else {
      assertNull(tableRouteInfo.getRealtimeRoutingTable());
    }

    assertEquals(routeComputer.getUnavailableSegments(), expectedTableRoute._unavailableSegments);
    assertEquals(routeComputer.getNumPrunedSegmentsTotal(), expectedTableRoute._numPrunedSegmentsTotal);
    if (!isOfflineExpected && !isRealtimeExpected) {
      assertFalse(routeComputer.isRouteExists());
    } else {
      assertTrue(routeComputer.isRouteExists());
    }
  }

  @Test(dataProvider = "offlineTableAndRouteProvider")
  void testTableRoutingForOffline(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRouting(tableName, expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableAndRouteProvider")
  void testTableRoutingForRealtime(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRouting(tableName, null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableAndRouteProvider")
  void testTableRoutingForHybrid(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRouting(tableName, expectedOfflineRoutingTable, expectedRealtimeRoutingTable,
        expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testTableRoutingForRouteNotExists(String tableName) {
    TableRouteProvider routeComputer = ImplicitHybridTableRouteProvider.create(tableName, _tableCache, _routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (routeComputer.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (routeComputer.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    assertNull(expectedTableRoute._offlineRoutingTable);
    assertNull(expectedTableRoute._realtimeRoutingTable);

    TableRouteInfo tableRouteInfo =
        routeComputer.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    assertNull(tableRouteInfo.getOfflineRoutingTable());
    assertNull(tableRouteInfo.getRealtimeRoutingTable());
    assertTrue(routeComputer.getUnavailableSegments().isEmpty());
    assertEquals(routeComputer.getNumPrunedSegmentsTotal(), 0);
    assertFalse(routeComputer.isRouteExists());
  }

  @Test(dataProvider = "partiallyDisabledTableAndRouteProvider")
  void testTableRoutingForPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    TableRouteProvider tableRouteProvider =
        ImplicitHybridTableRouteProvider.create(tableName, _tableCache, _routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest;
    BrokerRequest realtimeBrokerRequest;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
    offlinePinotQuery.getDataSource().setTableName(offlineTableName);
    offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);

    PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
    realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
    realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    TableRouteInfo tableRouteInfo =
        tableRouteProvider.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (expectedOfflineRoutingTable == null) {
      assertNull(tableRouteInfo.getOfflineRoutingTable());
      assertNull(expectedTableRoute._offlineRoutingTable);
    } else {
      assertFalse(tableRouteInfo.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertFalse(expectedTableRoute._offlineTableDisabled);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(),
          tableRouteInfo.getOfflineRoutingTable().entrySet());
      assertRoutingTableEqual(tableRouteInfo.getOfflineRoutingTable(), expectedOfflineRoutingTable);
    }

    if (expectedRealtimeRoutingTable == null) {
      assertNull(tableRouteInfo.getRealtimeRoutingTable());
      assertNull(expectedTableRoute._realtimeRoutingTable);
    } else {
      assertFalse(tableRouteInfo.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertFalse(expectedTableRoute._realtimeTableDisabled);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(),
          tableRouteInfo.getRealtimeRoutingTable().entrySet());
      assertRoutingTableEqual(tableRouteInfo.getRealtimeRoutingTable(), expectedRealtimeRoutingTable);
    }
  }

  @Test(dataProvider = "disabledTableProvider")
  void testTableRoutingForDisabledTable(String tableName) {
    TableRouteProvider tableRouteProvider =
        ImplicitHybridTableRouteProvider.create(tableName, _tableCache, _routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (tableRouteProvider.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (tableRouteProvider.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    TableRouteInfo tableRouteInfo =
        tableRouteProvider.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (expectedTableRoute._offlineTableDisabled) {
      assertNull(tableRouteInfo.getOfflineRoutingTable());
    } else if (expectedTableRoute._offlineRoutingTable != null) {
      assertNotNull(tableRouteInfo.getOfflineRoutingTable());
    }

    if (expectedTableRoute._realtimeTableDisabled) {
      assertNull(tableRouteInfo.getRealtimeRoutingTable());
    } else if (expectedTableRoute._realtimeRoutingTable != null) {
      assertNotNull(tableRouteInfo.getRealtimeRoutingTable());
    }
  }
}
