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
import org.apache.pinot.core.transport.TableRoute;
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


public class ImplicitTableRouteComputerCalculationTest extends BaseTableRouteTest {
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

  @DataProvider(name = "offlineTableProvider")
  public static Object[][] offlineTableProvider() {
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

  @DataProvider(name = "realtimeTableProvider")
  public static Object[][] realtimeTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"a_REALTIME", Map.of("Server_localhost_1", ImmutableSet.of("a1", "a2"),
            "Server_localhost_2", ImmutableSet.of("a3"))},
        {"b_REALTIME", Map.of("Server_localhost_1", ImmutableSet.of("b1"))},
        {"e_REALTIME", Map.of("Server_localhost_2", ImmutableSet.of("e2"))},
    };
    //@formatter:on
  }

  @DataProvider(name = "hybridTableProvider")
  public static Object[][] hybridTableProvider() {
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

  @DataProvider(name = "partiallyDisabledTableProvider")
  public static Object[][] partiallyDisabledTableProvider() {
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
    TableRouteComputer routeComputer = new ImplicitTableRouteComputer(tableName);
    routeComputer.getTableConfig(_tableCache);
    routeComputer.checkRoutes(_routingManager);

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

    TableRoute tableRoute = routeComputer.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (isOfflineExpected) {
      assertNotNull(routeComputer.getOfflineRoutingTable());
      assertFalse(routeComputer.getOfflineRoutingTable().isEmpty());
      assertEquals(routeComputer.getOfflineRoutingTable().size(), expectedOfflineRoutingTable.size());
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routeComputer.getOfflineRoutingTable().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        ServerRouteInfo serverRouteInfo = entry.getValue();
        Set<String> segments = ImmutableSet.copyOf(serverRouteInfo.getSegments());
        assertTrue(expectedOfflineRoutingTable.containsKey(serverInstance.toString()));
        assertEquals(expectedOfflineRoutingTable.get(serverInstance.toString()), segments);
      }
    } else {
      assertNull(routeComputer.getOfflineRoutingTable());
    }

    if (isRealtimeExpected) {
      assertNotNull(routeComputer.getRealtimeRoutingTable());
      assertFalse(routeComputer.getRealtimeRoutingTable().isEmpty());
      assertEquals(routeComputer.getRealtimeRoutingTable().size(), expectedRealtimeRoutingTable.size());
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routeComputer.getRealtimeRoutingTable().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        ServerRouteInfo serverRouteInfo = entry.getValue();
        Set<String> segments = ImmutableSet.copyOf(serverRouteInfo.getSegments());
        assertTrue(expectedRealtimeRoutingTable.containsKey(serverInstance.toString()));
        assertEquals(expectedRealtimeRoutingTable.get(serverInstance.toString()), segments);
      }
    } else {
      assertNull(routeComputer.getRealtimeRoutingTable());
    }

    assertTrue(routeComputer.getUnavailableSegments().isEmpty());
    assertEquals(routeComputer.getNumPrunedSegmentsTotal(), 0);
    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(tableRoute.getOfflineBrokerRequest() == null && tableRoute.getRealtimeBrokerRequest() == null);
    } else {
      assertFalse(tableRoute.getOfflineBrokerRequest() == null && tableRoute.getRealtimeBrokerRequest() == null);
      Map<ServerRoutingInstance, InstanceRequest> requestMap = tableRoute.getRequestMap(0, "broker", false);
      assertNotNull(requestMap);
      assertFalse(requestMap.isEmpty());
    }
  }

  @Test(dataProvider = "offlineTableProvider")
  void testOfflineTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableProvider")
  void testRealtimeTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableProvider")
  void testHybridTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, expectedOfflineRoutingTable, expectedRealtimeRoutingTable,
        expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testRouteNotExists(String tableName) {
    assertTableRoute(tableName, null, null, false, false);
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
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
    TableRouteComputer tableRoute = new ImplicitTableRouteComputer(tableName);
    tableRoute.getTableConfig(_tableCache);
    tableRoute.checkRoutes(_routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (tableRoute.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (tableRoute.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);
    tableRoute.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (isOfflineExpected) {
      assertNotNull(tableRoute.getOfflineRoutingTable());
      assertFalse(tableRoute.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(), tableRoute.getOfflineRoutingTable().entrySet());
    } else {
      assertNull(tableRoute.getOfflineRoutingTable());
    }

    if (isRealtimeExpected) {
      assertNotNull(tableRoute.getRealtimeRoutingTable());
      assertFalse(tableRoute.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(),
          tableRoute.getRealtimeRoutingTable().entrySet());
    } else {
      assertNull(tableRoute.getRealtimeRoutingTable());
    }

    assertEquals(tableRoute.getUnavailableSegments(), expectedTableRoute._unavailableSegments);
    assertEquals(tableRoute.getNumPrunedSegmentsTotal(), expectedTableRoute._numPrunedSegmentsTotal);
    if (!isOfflineExpected && !isRealtimeExpected) {
      assertFalse(tableRoute.isRouteExists());
    } else {
      assertTrue(tableRoute.isRouteExists());
    }
  }

  @Test(dataProvider = "offlineTableProvider")
  void testTableRoutingForOffline(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRouting(tableName, expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableProvider")
  void testTableRoutingForRealtime(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRouting(tableName, null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableProvider")
  void testTableRoutingForHybrid(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRouting(tableName, expectedOfflineRoutingTable, expectedRealtimeRoutingTable,
        expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testTableRoutingForRouteNotExists(String tableName) {
    TableRouteComputer tableRoute = new ImplicitTableRouteComputer(tableName);
    tableRoute.getTableConfig(_tableCache);
    tableRoute.checkRoutes(_routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (tableRoute.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (tableRoute.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    assertNull(expectedTableRoute._offlineRoutingTable);
    assertNull(expectedTableRoute._realtimeRoutingTable);

    tableRoute.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    assertNull(tableRoute.getOfflineRoutingTable());
    assertNull(tableRoute.getRealtimeRoutingTable());
    assertTrue(tableRoute.getUnavailableSegments().isEmpty());
    assertEquals(tableRoute.getNumPrunedSegmentsTotal(), 0);
    assertFalse(tableRoute.isRouteExists());
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  void testTableRoutingForPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    TableRouteComputer tableRouteComputer = new ImplicitTableRouteComputer(tableName);
    tableRouteComputer.getTableConfig(_tableCache);
    tableRouteComputer.checkRoutes(_routingManager);

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

    tableRouteComputer.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (expectedOfflineRoutingTable == null) {
      assertNull(tableRouteComputer.getOfflineRoutingTable());
      assertNull(expectedTableRoute._offlineRoutingTable);
    }

    if (expectedRealtimeRoutingTable == null) {
      assertNull(tableRouteComputer.getRealtimeRoutingTable());
      assertNull(expectedTableRoute._realtimeRoutingTable);
    }

    if (tableRouteComputer.getOfflineRoutingTable() != null) {
      assertFalse(tableRouteComputer.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertFalse(expectedTableRoute._offlineTableDisabled);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(),
          tableRouteComputer.getOfflineRoutingTable().entrySet());
    }

    if (tableRouteComputer.getRealtimeRoutingTable() != null) {
      assertFalse(tableRouteComputer.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertFalse(expectedTableRoute._realtimeTableDisabled);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(),
          tableRouteComputer.getRealtimeRoutingTable().entrySet());
    }
  }

  @Test(dataProvider = "disabledTableProvider")
  void testTableRoutingForDisabledTable(String tableName) {
    TableRouteComputer tableRouteComputer = new ImplicitTableRouteComputer(tableName);
    tableRouteComputer.getTableConfig(_tableCache);
    tableRouteComputer.checkRoutes(_routingManager);

    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (tableRouteComputer.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (tableRouteComputer.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    GetTableRouteResult expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    tableRouteComputer.calculateRoutes(_routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (expectedTableRoute._offlineTableDisabled) {
      assertNull(tableRouteComputer.getOfflineRoutingTable());
    } else if (expectedTableRoute._offlineRoutingTable != null) {
      assertNotNull(tableRouteComputer.getOfflineRoutingTable());
    }

    if (expectedTableRoute._realtimeTableDisabled) {
      assertNull(tableRouteComputer.getRealtimeRoutingTable());
    } else if (expectedTableRoute._realtimeRoutingTable != null) {
      assertNotNull(tableRouteComputer.getRealtimeRoutingTable());
    }
  }
}
