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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.query.testutils.MockRoutingManagerFactory;
import org.apache.pinot.query.timeboundary.TimeBoundaryStrategy;
import org.apache.pinot.query.timeboundary.TimeBoundaryStrategyService;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.apache.pinot.spi.auth.request.PinotQuery;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BaseTableRouteTest {
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

  RoutingManager _routingManager;
  TableCache _tableCache;
  ImplicitHybridTableRouteProvider _hybridTableRouteProvider;
  LogicalTableRouteProvider _logicalTableRouteProvider;
  TimeBoundaryStrategy _timeBoundaryStrategy;
  MockedStatic<TimeBoundaryStrategyService> _timeBoundaryStrategyFactoryMockedStatic;

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
    _hybridTableRouteProvider = new ImplicitHybridTableRouteProvider();
    _logicalTableRouteProvider = new LogicalTableRouteProvider();
    _timeBoundaryStrategyFactoryMockedStatic = mockStatic(TimeBoundaryStrategyService.class);
    _timeBoundaryStrategy = mock(TimeBoundaryStrategy.class);
    TimeBoundaryStrategyService mockService = mock(TimeBoundaryStrategyService.class);
    when(TimeBoundaryStrategyService.getInstance()).thenReturn(mockService);
    when(mockService.getTimeBoundaryStrategy(any())).thenReturn(_timeBoundaryStrategy);
    when(_timeBoundaryStrategy.computeTimeBoundary(any())).thenReturn(mock(TimeBoundaryInfo.class));
  }

  @AfterClass
  public void tearDown() {
    _timeBoundaryStrategyFactoryMockedStatic.close();
  }

  @DataProvider(name = "offlineTableProvider")
  public static Object[][] offlineTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"b_OFFLINE"},
        {"c"},
        {"c_OFFLINE"},
        {"d_OFFLINE"},
        {"e_OFFLINE"},
        {"no_route_table_O"},
        {"no_route_table_O_OFFLINE"},
        {"o_disabled_OFFLINE"}
    };
    //@formatter:on
  }

  @DataProvider(name = "realtimeTableProvider")
  public static Object[][] realtimeTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"a"},
        {"a_REALTIME"},
        {"b_REALTIME"},
        {"e_REALTIME"},
        {"no_route_table_R"},
        {"no_route_table_R_REALTIME"},
        {"r_disabled_REALTIME"}
    };
    //@formatter:on
  }

  @DataProvider(name = "hybridTableProvider")
  public static Object[][] hybridTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"d"},
        {"e"},
        {"no_route_table"},
        {"hybrid_disabled"},
        {"hybrid_o_disabled"},
        {"hybrid_r_disabled"}
    };
    //@formatter:on
  }

  @DataProvider(name = "nonExistentTableProvider")
  public static Object[][] nonExistentTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"non_existent_table"},
        {"non_existent_table_O"},
        {"non_existent_table_R"},
        {"a_OFFLINE"},
        {"c_REALTIME"},
        {"no_route_table_O_REALTIME"},
        {"no_route_table_R_OFFLINE"},
        {"o_disabled_REALTIME"},
        {"r_disabled_OFFLINE"}
    };
    //@formatter:on
  }

  @DataProvider(name = "routeExistsProvider")
  public static Object[][] routeExistsProvider() {
    //@formatter:off
    return new Object[][] {
        {"a"},
        {"a_REALTIME"},
        {"b"},
        {"b_OFFLINE"},
        {"b_REALTIME"},
        {"c"},
        {"c_OFFLINE"},
        {"d"},
        {"d_OFFLINE"},
        {"e"},
        {"e_OFFLINE"},
        {"e_REALTIME"}
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

  @DataProvider(name = "notDisabledTableProvider")
  public static Object[][] notDisabledTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"a"},
        {"a_REALTIME"},
        {"b"},
        {"b_OFFLINE"},
        {"b_REALTIME"},
        {"c"},
        {"c_OFFLINE"},
        {"d"},
        {"d_OFFLINE"},
        {"e"},
        {"e_OFFLINE"},
        {"e_REALTIME"},
        {"no_route_table"},
        {"no_route_table_O"},
        {"no_route_table_R"},
        {"no_route_table_O_OFFLINE"},
        {"no_route_table_R_REALTIME"},
        {"hybrid_o_disabled_REALTIME"},
        {"hybrid_r_disabled_OFFLINE"},
    };
    //@formatter:on
  }

  @DataProvider(name = "partiallyDisabledTableProvider")
  public static Object[][] partiallyDisabledTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"hybrid_o_disabled"},
        {"hybrid_r_disabled"}
    };
    //@formatter:on
  }

  @DataProvider(name = "disabledTableProvider")
  public static Object[][] disabledTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"hybrid_disabled"},
        {"hybrid_disabled_OFFLINE"},
        {"hybrid_disabled_REALTIME"},
        {"hybrid_o_disabled_OFFLINE"},
        {"hybrid_r_disabled_REALTIME"},
        {"o_disabled_OFFLINE"},
        {"r_disabled_REALTIME"}
    };
    //@formatter:on
  }

  private static final String QUERY_FORMAT = "SELECT col1, col2 FROM %s LIMIT 10";

  static class BrokerRequestPair {
    public final BrokerRequest _offlineBrokerRequest;
    public final BrokerRequest _realtimeBrokerRequest;

    public BrokerRequestPair(BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest) {
      _offlineBrokerRequest = offlineBrokerRequest;
      _realtimeBrokerRequest = realtimeBrokerRequest;
    }
  }

  static BrokerRequestPair getBrokerRequestPair(String tableName, boolean hasOffline, boolean hasRealtime,
      String offlineTableName, String realtimeTableName) {
    String query = String.format(QUERY_FORMAT, tableName);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.convertToBrokerRequest(CalciteSqlParser.compileToPinotQuery(query));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;

    if (hasOffline) {
      PinotQuery offlinePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);
    }

    if (hasRealtime) {
      PinotQuery realtimePinotQuery = brokerRequest.getPinotQuery().deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    }

    return new BrokerRequestPair(offlineBrokerRequest, realtimeBrokerRequest);
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

  protected TableRouteInfo getLogicalTableRouteInfo(String tableName, String logicalTableName) {
    LogicalTableConfigBuilder builder = new LogicalTableConfigBuilder();
    builder.setTableName(logicalTableName);
    Map<String, PhysicalTableConfig> tableConfigMap = new HashMap<>();
    if (TableNameBuilder.getTableTypeFromTableName(tableName) == null) {
      // Generate offline and realtime table names
      tableConfigMap.put(TableNameBuilder.OFFLINE.tableNameWithType(tableName), new PhysicalTableConfig());
      tableConfigMap.put(TableNameBuilder.REALTIME.tableNameWithType(tableName), new PhysicalTableConfig());
      builder.setRefOfflineTableName(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      builder.setRefRealtimeTableName(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    } else if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.OFFLINE) {
      tableConfigMap.put(tableName, new PhysicalTableConfig());
      builder.setRefOfflineTableName(tableName);
    } else if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.REALTIME) {
      tableConfigMap.put(tableName, new PhysicalTableConfig());
      builder.setRefRealtimeTableName(tableName);
    } else {
      throw new IllegalArgumentException("Invalid table type");
    }

    builder.setPhysicalTableConfigMap(tableConfigMap);
    builder.setBrokerTenant("brokerTenant");
    builder.setTimeBoundaryConfig(
        new TimeBoundaryConfig("min", Map.of("includedTables", List.of("randomTable_OFFLINE"))));
    LogicalTableConfig logicalTable = builder.build();
    when(_tableCache.getLogicalTableConfig(eq(logicalTableName))).thenReturn(logicalTable);

    return _logicalTableRouteProvider.getTableRouteInfo(logicalTableName, _tableCache, _routingManager);
  }

  static void assertRoutingTableEqual(Map<ServerInstance, ServerRouteInfo> routeComputer,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routeComputer.entrySet()) {
      ServerInstance serverInstance = entry.getKey();
      ServerRouteInfo serverRouteInfo = entry.getValue();
      Set<String> segments = ImmutableSet.copyOf(serverRouteInfo.getSegments());
      assertTrue(expectedRealtimeRoutingTable.containsKey(serverInstance.toString()));
      assertEquals(expectedRealtimeRoutingTable.get(serverInstance.toString()), segments);
    }
  }
}
