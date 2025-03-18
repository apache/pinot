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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.testutils.MockRoutingManagerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ImplicitHybridTableTest {
  //@formatter:off
  public static final Map<String, List<String>> SERVER1_SEGMENTS =
      ImmutableMap.of(
          "a_REALTIME", ImmutableList.of("a1", "a2"),
          "b_REALTIME", ImmutableList.of("b1"),
          "c_OFFLINE", ImmutableList.of("c1"),
          "d_OFFLINE", ImmutableList.of("d1"),
          "e_OFFLINE", ImmutableList.of("e1"));
  public static final Map<String, List<String>> SERVER2_SEGMENTS =
      ImmutableMap.of(
          "a_REALTIME", ImmutableList.of("a3"),
          "b_OFFLINE", ImmutableList.of("b2"),
          "c_OFFLINE", ImmutableList.of("c2", "c3"),
          "d_OFFLINE", ImmutableList.of("d3"),
          "e_REALTIME", ImmutableList.of("e2"),
          "e_OFFLINE", ImmutableList.of("e3"));
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

  @Test(dataProvider = "offlineTableProvider")
  public void testOfflineTable(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isOffline(), "The table should be offline");
    assertNotNull(table.getOfflineTables());
    assertEquals(table.getOfflineTables().size(), 1);
    assertNotNull(table.getOfflineTable());
    assertEquals(table.getOfflineTable().getTableNameWithType(), TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(parameter));
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

  @Test(dataProvider = "realtimeTableProvider")
  public void testRealtimeTable(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isRealtime(), "The table should be realtime");
    assertNotNull(table.getRealtimeTables());
    assertEquals(table.getRealtimeTables().size(), 1);
    assertNotNull(table.getRealtimeTable());
    assertEquals(table.getRealtimeTable().getTableNameWithType(), TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(parameter));
  }

  @DataProvider(name = "hybridTableProvider")
  public static Object[][] hybridTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"b"},
        {"d"},
        {"e"},
        {"no_route_table"},
        {"hybrid_disabled"},
        {"hybrid_o_disabled"},
        {"hybrid_r_disabled"}
    };
    //@formatter:on
  }

  @Test(dataProvider = "hybridTableProvider")
  public void testHybridTable(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isHybrid(), "The table should be hybrid");

    assertNotNull(table.getOfflineTables());
    assertEquals(table.getOfflineTables().size(), 1);
    assertNotNull(table.getOfflineTable());
    assertEquals(table.getOfflineTable().getTableNameWithType(), TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(parameter));

    assertNotNull(table.getRealtimeTables());
    assertEquals(table.getRealtimeTables().size(), 1);
    assertNotNull(table.getRealtimeTable());
    assertEquals(table.getRealtimeTable().getTableNameWithType(), TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(parameter));

    assertNotNull(table.getAllPhysicalTables());
    assertEquals(table.getAllPhysicalTables().size(), 2);
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

  @Test(dataProvider = "nonExistentTableProvider")
  public void testNonExistentTableName(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertFalse(table.isExists(), "The table should not exist");
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

  @Test(dataProvider = "routeExistsProvider")
  public void testRouteExists(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isRouteExists(), "The table should have route");
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

  @Test(dataProvider = "routeNotExistsProvider")
  public void testRouteNotExists(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertFalse(table.isRouteExists(), "The table should not have route");
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
    };
    //@formatter:on
  }

  @Test(dataProvider = "notDisabledTableProvider")
  public void testNotDisabledTable(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertFalse(table.isDisabled(), "The table should not be disabled");
    assertNull(table.getDisabledTables());
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

  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledTable(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertFalse(table.isDisabled(), "The table should be disabled");
    assertNotNull(table.getDisabledTables());
    assertEquals(table.getDisabledTables().size(), 1);
  }

  @DataProvider(name = "disabledTableProvider")
  public static Object[][] disabledTableProvider() {
    //@formatter:off
    return new Object[][] {
        {"hybrid_disabled"},
        {"hybrid_o_disabled_OFFLINE"},
        {"hybrid_r_disabled_REALTIME"},
        {"o_disabled_OFFLINE"},
        {"r_disabled_REALTIME"}
    };
    //@formatter:on
  }

  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledTable(String parameter) {
    ImplicitHybridTable table = ImplicitHybridTable.from(parameter, _routingManager, _tableCache);
    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isDisabled(), "The table should not have route");
    assertNotNull(table.getDisabledTables(), "The table should have disabled tables");
    assertFalse(table.getDisabledTables().isEmpty(), "The table should have disabled tables");
  }

  static class TableNameAndConfig {
    public final String _offlineTableName;
    public final String _realtimeTableName;
    public final TableConfig _offlineTableConfig;
    public final TableConfig _realtimeTableConfig;
    public final BrokerResponse _brokerResponse;

    public TableNameAndConfig(String offlineTableName, String realtimeTableName, TableConfig offlineTableConfig,
        TableConfig realtimeTableConfig) {
      _offlineTableName = offlineTableName;
      _realtimeTableName = realtimeTableName;
      _offlineTableConfig = offlineTableConfig;
      _realtimeTableConfig = realtimeTableConfig;
     _brokerResponse = null;
    }

    public TableNameAndConfig(BrokerResponse brokerResponse) {
      _offlineTableName = null;
      _realtimeTableName = null;
      _offlineTableConfig = null;
      _realtimeTableConfig = null;
      _brokerResponse = brokerResponse;
    }

    boolean similar(HybridTable hybridTable) {
      boolean isEquals = true;

      if (_offlineTableName != null) {
        isEquals &= hybridTable.hasOffline() && hybridTable.isOfflineRouteExists()
            && _offlineTableName.equals(hybridTable.getOfflineTable().getTableNameWithType());
      } else {
        isEquals &= !hybridTable.hasOffline() || !hybridTable.getOfflineTable().isRouteExists();
      }

      if (_realtimeTableName != null) {
        isEquals &= hybridTable.hasRealtime() && hybridTable.isRealtimeRouteExists()
            && _realtimeTableName.equals(hybridTable.getRealtimeTable().getTableNameWithType());
      } else {
        isEquals &= !hybridTable.hasRealtime() || !hybridTable.getRealtimeTable().isRouteExists();
      }

      return isEquals;
    }
  }

  /**
   * This method implements the previous version to check if a table exists. It took as input two pieces of metadata:
   * - TableConfig
   * - Route
   * If both existed, then a physical table is available. This method is used to test the new implementations of
   * HybridTable.isExists and HybridTable.isRouteExists behave in the same way.
   * @param tableName The name of the table to check
   * @return The table name and config
   */
  TableNameAndConfig getTableNameAndConfig(String tableName) {
   String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Get the tables hit by the request
    String offlineTableName = null;
    String realtimeTableName = null;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == TableType.OFFLINE) {
      // Offline table
      if (_routingManager.routingExists(tableName)) {
        offlineTableName = tableName;
      }
    } else if (tableType == TableType.REALTIME) {
      // Realtime table
      if (_routingManager.routingExists(tableName)) {
        realtimeTableName = tableName;
      }
    } else {
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (_routingManager.routingExists(offlineTableNameToCheck)) {
        offlineTableName = offlineTableNameToCheck;
      }
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (_routingManager.routingExists(realtimeTableNameToCheck)) {
        realtimeTableName = realtimeTableNameToCheck;
      }
    }

    TableConfig offlineTableConfig =
        _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    TableConfig realtimeTableConfig =
        _tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));

    if (offlineTableName == null && realtimeTableName == null) {
      // No table matches the request
      if (realtimeTableConfig == null && offlineTableConfig == null) {
        return new TableNameAndConfig(BrokerResponseNative.TABLE_DOES_NOT_EXIST);
      }
      return new TableNameAndConfig(BrokerResponseNative.NO_TABLE_RESULT);
    }

    // Handle query rewrite that can be overridden by the table configs
    if (offlineTableName == null) {
      offlineTableConfig = null;
    }
    if (realtimeTableName == null) {
      realtimeTableConfig = null;
    }

    return new TableNameAndConfig(offlineTableName, realtimeTableName, offlineTableConfig, realtimeTableConfig);
  }

  @DataProvider(name = "tableNameAndConfigSuccessProvider")
  public static Object[][] tableNameAndConfigSuccessProvider() {
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

  @Test(dataProvider = "tableNameAndConfigSuccessProvider")
  public void testTableNameAndConfigSuccess(String tableName) {
    TableNameAndConfig tableNameAndConfig = getTableNameAndConfig(tableName);
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
    assertTrue(tableNameAndConfig.similar(hybridTable), "The table name and config should match the hybrid table");
  }

  @DataProvider(name = "tableNameAndConfigFailureProvider")
  public static Object[][] tableNameAndConfigFailureProvider() {
    //@formatter:off
    return new Object[][] {
        {"non_existent_table"},
        {"non_existent_table_O"},
        {"non_existent_table_R"},
        {"a_OFFLINE"},
        {"c_REALTIME"},
        {"d_REALTIME"},
        {"no_route_table"},
        {"no_route_table_O"},
        {"no_route_table_R"},
        {"no_route_table_O_REALTIME"},
        {"no_route_table_R_OFFLINE"}
    };
    //@formatter:on
  }

  @Test(dataProvider = "tableNameAndConfigFailureProvider")
  public void testTableNameAndConfigFailure(String tableName) {
    TableNameAndConfig tableNameAndConfig = getTableNameAndConfig(tableName);
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);
    assertNotNull(tableNameAndConfig._brokerResponse);
    assertTrue(tableNameAndConfig._brokerResponse == BrokerResponseNative.TABLE_DOES_NOT_EXIST
        || tableNameAndConfig._brokerResponse == BrokerResponseNative.NO_TABLE_RESULT);

    if (tableNameAndConfig._brokerResponse == BrokerResponseNative.TABLE_DOES_NOT_EXIST) {
      assertFalse(hybridTable.isExists(), "The table should not exist");
    } else {
      assertFalse(hybridTable.isRouteExists(), "The table should not have route");
    }
  }

  static class ExceptionOrResponse {
    public final QueryProcessingException _exception;
    public final BrokerResponse _brokerResponse;

    public ExceptionOrResponse(QueryProcessingException exception) {
      _exception = exception;
      _brokerResponse = null;
    }

    public ExceptionOrResponse(BrokerResponse brokerResponse) {
      _exception = null;
      _brokerResponse = brokerResponse;
    }
  }

  ExceptionOrResponse checkTableDisabled(boolean offlineTableDisabled, boolean realtimeTableDisabled,
      TableConfig offlineTableConfig, TableConfig realtimeTableConfig) {
    if (offlineTableDisabled || realtimeTableDisabled) {
      String errorMessage = null;
      if (((realtimeTableConfig != null && offlineTableConfig != null) && (offlineTableDisabled
          && realtimeTableDisabled)) || (offlineTableConfig == null && realtimeTableDisabled) || (
          realtimeTableConfig == null && offlineTableDisabled)) {
        return new ExceptionOrResponse(BrokerResponseNative.TABLE_IS_DISABLED);
      } else if ((realtimeTableConfig != null && offlineTableConfig != null) && realtimeTableDisabled) {
        errorMessage = "Realtime table is disabled in hybrid table";
      } else if ((realtimeTableConfig != null && offlineTableConfig != null) && offlineTableDisabled) {
        errorMessage = "Offline table is disabled in hybrid table";
      }
      return new ExceptionOrResponse(new QueryProcessingException(QueryErrorCode.TABLE_IS_DISABLED, errorMessage));
    }

    return null;
  }

  @Test(dataProvider = "notDisabledTableProvider")
  public void testNotDisabledWithCheckDisabled(String tableName) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(hybridTable.hasOffline() && hybridTable.getOfflineTable().isDisabled(),
            hybridTable.hasRealtime() && hybridTable.getRealtimeTable().isDisabled(),
            hybridTable.hasOffline() ? hybridTable.getOfflineTable().getTableConfig() : null,
            hybridTable.hasRealtime() ? hybridTable.getRealtimeTable().getTableConfig() : null);

    assertNull(exceptionOrResponse);
    assertFalse(hybridTable.isDisabled());
    assertNull(hybridTable.getDisabledTables());
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledWithCheckDisabled(String tableName) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(hybridTable.hasOffline() && hybridTable.getOfflineTable().isDisabled(),
            hybridTable.hasRealtime() && hybridTable.getRealtimeTable().isDisabled(),
            hybridTable.hasOffline() ? hybridTable.getOfflineTable().getTableConfig() : null,
            hybridTable.hasRealtime() ? hybridTable.getRealtimeTable().getTableConfig() : null);

    assertNotNull(exceptionOrResponse);
    assertNull(exceptionOrResponse._brokerResponse);
    assertNotNull(exceptionOrResponse._exception);
    assertFalse(hybridTable.isDisabled());
    assertNotNull(hybridTable.getDisabledTables());
    assertFalse(hybridTable.getDisabledTables().isEmpty());
  }

  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledWithCheckDisabled(String tableName) {
    HybridTable hybridTable = ImplicitHybridTable.from(tableName, _routingManager, _tableCache);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(hybridTable.hasOffline() && hybridTable.getOfflineTable().isDisabled(),
            hybridTable.hasRealtime() && hybridTable.getRealtimeTable().isDisabled(),
            hybridTable.hasOffline() ? hybridTable.getOfflineTable().getTableConfig() : null,
            hybridTable.hasRealtime() ? hybridTable.getRealtimeTable().getTableConfig() : null);

    assertNotNull(exceptionOrResponse);
    assertNull(exceptionOrResponse._exception);
    assertNotNull(exceptionOrResponse._brokerResponse);
    assertTrue(hybridTable.isDisabled());
    assertNotNull(hybridTable.getDisabledTables());
    assertFalse(hybridTable.getDisabledTables().isEmpty());
  }
}
