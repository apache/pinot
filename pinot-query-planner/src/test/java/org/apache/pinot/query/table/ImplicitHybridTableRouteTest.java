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
package org.apache.pinot.query.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.query.testutils.MockRoutingManagerFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ImplicitHybridTableRouteTest {
  //@formatter:off
  public static final Map<String, List<String>> SERVER1_SEGMENTS =
      ImmutableMap.of(
          "a_REALTIME", ImmutableList.of("a1", "a2"),
          "b_REALTIME", ImmutableList.of("b1"),
          "c_OFFLINE", ImmutableList.of("c1"),
          "d_OFFLINE", ImmutableList.of("d1"),
          "e_OFFLINE", ImmutableList.of("e1"),
          "hybrid_o_disabled_OFFLINE", ImmutableList.of("hod1"),
          "hybrid_r_disabled_REALTIME", ImmutableList.of("hrd1"),
          "hybrid_o_disabled_REALTIME", ImmutableList.of("hor1"),
          "hybrid_r_disabled_OFFLINE", ImmutableList.of("hro1"));

  public static final Map<String, List<String>> SERVER2_SEGMENTS =
      ImmutableMap.of(
          "a_REALTIME", ImmutableList.of("a3"),
          "b_OFFLINE", ImmutableList.of("b2"),
          "c_OFFLINE", ImmutableList.of("c2", "c3"),
          "d_OFFLINE", ImmutableList.of("d3"),
          "e_REALTIME", ImmutableList.of("e2"),
          "e_OFFLINE", ImmutableList.of("e3"),
          "hybrid_o_disabled_OFFLINE", ImmutableList.of("hod2"),
          "hybrid_r_disabled_REALTIME", ImmutableList.of("hrd2"),
          "hybrid_o_disabled_REALTIME", ImmutableList.of("hor2"),
          "hybrid_r_disabled_OFFLINE", ImmutableList.of("hro2"));
  //@formatter:on

  public static final Map<String, Schema> TABLE_SCHEMAS = new HashMap<>();
  private static final Set<String> DISABLED_TABLES = new HashSet<>();

  static {
    TABLE_SCHEMAS.put("a_REALTIME", getSchemaBuilder("a").build());
    TABLE_SCHEMAS.put("b_OFFLINE", getSchemaBuilder("b").build());
    TABLE_SCHEMAS.put("b_REALTIME", getSchemaBuilder("b").build());
    TABLE_SCHEMAS.put("c_OFFLINE", getSchemaBuilder("c").build());
    TABLE_SCHEMAS.put("d", getSchemaBuilder("d").build());
    TABLE_SCHEMAS.put("e", getSchemaBuilder("e").build());
    // The following tables are disabled.
    TABLE_SCHEMAS.put("hybrid_disabled", getSchemaBuilder("hybrid_disabled").build());
    DISABLED_TABLES.add("hybrid_disabled_OFFLINE");
    DISABLED_TABLES.add("hybrid_disabled_REALTIME");
    TABLE_SCHEMAS.put("hybrid_o_disabled", getSchemaBuilder("hybrid_o_disabled").build());
    DISABLED_TABLES.add("hybrid_o_disabled_OFFLINE");
    TABLE_SCHEMAS.put("hybrid_r_disabled", getSchemaBuilder("hybrid_r_disabled").build());
    DISABLED_TABLES.add("hybrid_r_disabled_REALTIME");
    TABLE_SCHEMAS.put("o_disabled_OFFLINE", getSchemaBuilder("o_disabled").build());
    DISABLED_TABLES.add("o_disabled_OFFLINE");
    TABLE_SCHEMAS.put("r_disabled_REALTIME", getSchemaBuilder("r_disabled").build());
    DISABLED_TABLES.add("r_disabled_REALTIME");
    // The following three tables are registered but there are no routes for these tables.
    TABLE_SCHEMAS.put("no_route_table", getSchemaBuilder("no_route_table").build());
    TABLE_SCHEMAS.put("no_route_table_O_OFFLINE", getSchemaBuilder("no_route_table").build());
    TABLE_SCHEMAS.put("no_route_table_R_REALTIME", getSchemaBuilder("no_route_table").build());
  }

  //@formatter:off
  static Schema.SchemaBuilder getSchemaBuilder(String schemaName) {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col5", FieldSpec.DataType.BOOLEAN, false)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addDateTime("ts_timestamp", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .addMetric("col4", FieldSpec.DataType.BIG_DECIMAL, 0)
        .addMetric("col6", FieldSpec.DataType.INT, 0)
        .setSchemaName(schemaName);
  }
  //@formatter:on

  private static final String QUERY_FORMAT = "SELECT col1, col2 FROM %s LIMIT 10";

  RoutingManager _routingManager;
  TableCache _tableCache;

  @BeforeClass
  public void setUp() {
    int port1 = 1;
    int port2 = 2;
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2);
    for (Map.Entry<String, Schema> entry : TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(port1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(port2, entry.getKey(), segment);
      }
    }

    for (String disabledTable : DISABLED_TABLES) {
      factory.disableTable(disabledTable);
    }

    _routingManager = factory.buildRoutingManager(null);
    _tableCache = factory.buildTableCache();
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

  @DataProvider(name = "routeNotExistsProvider")
  public static Object[][] routeNotExistsProvider() {
    //@formatter:off
    return new Object[][] {
        {"d_REALTIME"},
        {"no_route_table"},
        {"no_route_table_O"},
        {"no_route_table_R"},
        {"no_route_table_O_OFFLINE"},
        {"no_route_table_R_REALTIME"}
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

  @DataProvider(name = "disabledTableProvider")
  public static Object[][] disabledTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"hybrid_disabled"},
        {"hybrid_o_disabled_OFFLINE"},
        {"hybrid_r_disabled_REALTIME"},
        {"o_disabled"},
        {"o_disabled_OFFLINE"},
        {"r_disabled_REALTIME"},
        {"r_disabled"}
    };
    //@formatter:on
  }

  private void assertTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable, boolean isOfflineExpected, boolean isRealtimeExpected) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;

    if (hybridTable.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(hybridTable.getOfflineTable().getTableNameWithType());
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (hybridTable.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(hybridTable.getRealtimeTable().getTableNameWithType());
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    ImplicitHybridTableRoute route =
        ImplicitHybridTableRoute.from(hybridTable, _routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (isOfflineExpected) {
      assertNotNull(route.getOfflineRoutingTable());
      assertFalse(route.getOfflineRoutingTable().isEmpty());
      assertEquals(route.getOfflineRoutingTable().size(), expectedOfflineRoutingTable.size());
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : route.getOfflineRoutingTable().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        ServerRouteInfo serverRouteInfo = entry.getValue();
        Set<String> segments = ImmutableSet.copyOf(serverRouteInfo.getSegments());
        assertTrue(expectedOfflineRoutingTable.containsKey(serverInstance.toString()));
        assertEquals(expectedOfflineRoutingTable.get(serverInstance.toString()), segments);
      }

      Map<ServerRoutingInstance, InstanceRequest> offlineRequestMap = route.getOfflineRequestMap(0, "broker", false);
      assertNotNull(offlineRequestMap);
      assertFalse(offlineRequestMap.isEmpty());
    } else {
      assertNull(route.getOfflineRoutingTable());
    }

    if (isRealtimeExpected) {
      assertNotNull(route.getRealtimeRoutingTable());
      assertFalse(route.getRealtimeRoutingTable().isEmpty());
      assertEquals(route.getRealtimeRoutingTable().size(), expectedRealtimeRoutingTable.size());
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : route.getRealtimeRoutingTable().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        ServerRouteInfo serverRouteInfo = entry.getValue();
        Set<String> segments = ImmutableSet.copyOf(serverRouteInfo.getSegments());
        assertTrue(expectedRealtimeRoutingTable.containsKey(serverInstance.toString()));
        assertEquals(expectedRealtimeRoutingTable.get(serverInstance.toString()), segments);
      }

      Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap = route.getRealtimeRequestMap(0, "broker", false);
      assertNotNull(realtimeRequestMap);
      assertFalse(realtimeRequestMap.isEmpty());
    } else {
      assertNull(route.getRealtimeRoutingTable());
    }

    assertTrue(route.getUnavailableSegments().isEmpty());
    assertEquals(route.getNumPrunedSegmentsTotal(), 0);
    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(route.isEmpty());
    } else {
      assertFalse(route.isEmpty());
      Map<ServerRoutingInstance, InstanceRequest> requestMap = route.getRequestMap(0, "broker", false);
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

  private static class TableRoute {
    public final BrokerRequest _offlineBrokerRequest;
    public final BrokerRequest _realtimeBrokerRequest;
    public final Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
    public final Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;
    public final List<String> _unavailableSegments;
    public final int _numPrunedSegmentsTotal;
    public final boolean _offlineTableDisabled;
    public final boolean _realtimeTableDisabled;

    public TableRoute(BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest,
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

  private static TableRoute getTableRouting(BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest,
      String offlineTableName, String realtimeTableName, RoutingManager routingManager) {
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

    return new TableRoute(offlineBrokerRequest, realtimeBrokerRequest, offlineRoutingTable, realtimeRoutingTable,
        unavailableSegments, numPrunedSegmentsTotal, offlineTableDisabled, realtimeTableDisabled);
  }

  private void assertTableRouting(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable, boolean isOfflineExpected, boolean isRealtimeExpected) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (hybridTable.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (hybridTable.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    TableRoute expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);
    ImplicitHybridTableRoute route =
        ImplicitHybridTableRoute.from(hybridTable, _routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (isOfflineExpected) {
      assertNotNull(route.getOfflineRoutingTable());
      assertFalse(route.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(), route.getOfflineRoutingTable().entrySet());
    } else {
      assertNull(route.getOfflineRoutingTable());
    }

    if (isRealtimeExpected) {
      assertNotNull(route.getRealtimeRoutingTable());
      assertFalse(route.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(), route.getRealtimeRoutingTable().entrySet());
    } else {
      assertNull(route.getRealtimeRoutingTable());
    }

    assertEquals(route.getUnavailableSegments(), expectedTableRoute._unavailableSegments);
    assertEquals(route.getNumPrunedSegmentsTotal(), expectedTableRoute._numPrunedSegmentsTotal);
    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(route.isEmpty());
    } else {
      assertFalse(route.isEmpty());
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
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (hybridTable.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (hybridTable.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    TableRoute expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    assertNull(expectedTableRoute._offlineRoutingTable);
    assertNull(expectedTableRoute._realtimeRoutingTable);

    ImplicitHybridTableRoute route =
        ImplicitHybridTableRoute.from(hybridTable, _routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    assertNull(route.getOfflineRoutingTable());
    assertNull(route.getRealtimeRoutingTable());
    assertTrue(route.getUnavailableSegments().isEmpty());
    assertEquals(route.getNumPrunedSegmentsTotal(), 0);
    assertTrue(route.isEmpty());
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  void testTableRoutingForPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
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

    TableRoute expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    ImplicitHybridTableRoute route =
        ImplicitHybridTableRoute.from(hybridTable, _routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (expectedOfflineRoutingTable == null) {
      assertNull(route.getOfflineRoutingTable());
      assertNull(expectedTableRoute._offlineRoutingTable);
    }

    if (expectedRealtimeRoutingTable == null) {
      assertNull(route.getRealtimeRoutingTable());
      assertNull(expectedTableRoute._realtimeRoutingTable);
    }

    if (route.getOfflineRoutingTable() != null) {
      assertFalse(route.getOfflineRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._offlineRoutingTable);
      assertFalse(expectedTableRoute._offlineTableDisabled);
      assertEquals(expectedTableRoute._offlineRoutingTable.entrySet(), route.getOfflineRoutingTable().entrySet());
    }

    if (route.getRealtimeRoutingTable() != null) {
      assertFalse(route.getRealtimeRoutingTable().isEmpty());
      assertNotNull(expectedTableRoute._realtimeRoutingTable);
      assertFalse(expectedTableRoute._realtimeTableDisabled);
      assertEquals(expectedTableRoute._realtimeRoutingTable.entrySet(), route.getRealtimeRoutingTable().entrySet());
    }
  }

  @Test(dataProvider = "disabledTableProvider")
  void testTableRoutingForDisabledTable(String tableName) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    if (hybridTable.hasOffline()) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (hybridTable.hasRealtime()) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    TableRoute expectedTableRoute =
        getTableRouting(offlineBrokerRequest, realtimeBrokerRequest, offlineTableName, realtimeTableName,
            _routingManager);

    ImplicitHybridTableRoute route =
        ImplicitHybridTableRoute.from(hybridTable, _routingManager, offlineBrokerRequest, realtimeBrokerRequest, 0);

    if (expectedTableRoute._offlineTableDisabled) {
      assertNull(route.getOfflineRoutingTable());
    } else if (expectedTableRoute._offlineRoutingTable != null) {
      assertNotNull(route.getOfflineRoutingTable());
    }

    if (expectedTableRoute._realtimeTableDisabled) {
      assertNull(route.getRealtimeRoutingTable());
    } else if (expectedTableRoute._realtimeRoutingTable != null) {
      assertNotNull(route.getRealtimeRoutingTable());
    }
  }
}
