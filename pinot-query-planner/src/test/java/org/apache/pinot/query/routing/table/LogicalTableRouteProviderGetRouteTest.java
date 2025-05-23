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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class LogicalTableRouteProviderGetRouteTest extends BaseTableRouteTest {
  @Test
  void testWithUnknownTable() {
    String logicalTableName = "testWithUnknownTable";
    when(_tableCache.getLogicalTableConfig(eq(logicalTableName))).thenReturn(null);
    TableRouteInfo routeInfo =
        _logicalTableRouteProvider.getTableRouteInfo(logicalTableName, _tableCache, _routingManager);
    assertFalse(routeInfo.isExists());
    assertFalse(routeInfo.isHybrid());
    assertFalse(routeInfo.isOffline());
    assertFalse(routeInfo.isRealtime());
    assertFalse(routeInfo.isRouteExists());
    assertFalse(routeInfo.isDisabled());
    assertNull(routeInfo.getOfflineTableName());
    assertNull(routeInfo.getRealtimeTableName());
    assertNull(routeInfo.getDisabledTableNames());
    assertNull(routeInfo.getTimeBoundaryInfo());
    assertNull(routeInfo.getOfflineTableConfig());
    assertNull(routeInfo.getRealtimeTableConfig());
    assertNull(routeInfo.getOfflineBrokerRequest());
    assertNull(routeInfo.getRealtimeBrokerRequest());
    assertNull(routeInfo.getUnavailableSegments());

    LogicalTableRouteInfo logicalTableRouteInfo = (LogicalTableRouteInfo) routeInfo;
    assertNull(logicalTableRouteInfo.getOfflineTables());
    assertNull(logicalTableRouteInfo.getRealtimeTables());
  }

  @Test(dataProvider = "offlineTableProvider")
  public void testOfflineTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testOfflineTable");
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isOffline(), "The table should be offline");
    assertNull(routeInfo.getTimeBoundaryInfo(), "The table should not have time boundary info");
  }

  @Test(dataProvider = "realtimeTableProvider")
  public void testRealtimeTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testRealtimeTable");
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isRealtime(), "The table should be realtime");
    assertNull(routeInfo.getTimeBoundaryInfo(), "The table should not have time boundary info");
  }

  @Test(dataProvider = "hybridTableProvider")
  public void testHybridTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testHybridTable");
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isHybrid(), "The table should be hybrid");
    assertNotNull(routeInfo.getTimeBoundaryInfo(), "The table should have time boundary info");
  }

  @Test(dataProvider = "routeExistsProvider")
  public void testRouteExists(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testRouteExists");

    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isRouteExists(), "The table should have route");
  }

  @Test(dataProvider = "routeNotExistsProvider")
  public void testRouteNotExists(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testRouteNotExists");

    assertTrue(routeInfo.isExists(), "The table should exist");
    assertFalse(routeInfo.isRouteExists(), "The table should not have route");
  }

  @DataProvider(name = "offlineTableList")
  public Object[][] offlineTableList() {
    return new Object[][]{
        {ImmutableList.of("b_OFFLINE")}, {ImmutableList.of("b_OFFLINE", "c_OFFLINE")}, {
        ImmutableList.of("b_OFFLINE", "c_OFFLINE", "d_OFFLINE")
    }, {ImmutableList.of("b_OFFLINE", "c_OFFLINE", "d_OFFLINE", "e_OFFLINE")},
    };
  }

  @Test(dataProvider = "notDisabledTableProvider")
  public void testNotDisabledTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testNotDisabledTable");
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertFalse(routeInfo.isDisabled(), "The table should not be disabled");
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testPartiallyDisabledTable");
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertFalse(routeInfo.isDisabled(), "The table should be disabled");
  }

  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testDisabledTable");
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isDisabled(), "The table should not have route");
  }

  @Test(dataProvider = "offlineTableList")
  void testWithOfflineTables(List<String> physicalTableNames) {
    String logicalTableName = "testWithOfflineTables";
    LogicalTableConfig logicalTable = new LogicalTableConfig();
    logicalTable.setTableName(logicalTableName);
    Map<String, PhysicalTableConfig> tableConfigMap = new HashMap<>();
    for (String tableName : physicalTableNames) {
      tableConfigMap.put(tableName, new PhysicalTableConfig());
    }
    logicalTable.setPhysicalTableConfigMap(tableConfigMap);
    logicalTable.setBrokerTenant("brokerTenant");
    logicalTable.setRefOfflineTableName(physicalTableNames.get(0));
    when(_tableCache.getLogicalTableConfig(eq(logicalTableName))).thenReturn(logicalTable);

    TableRouteInfo routeInfo =
        _logicalTableRouteProvider.getTableRouteInfo(logicalTableName, _tableCache, _routingManager);
    assertNotNull(routeInfo.getOfflineTableConfig());
    assertNull(routeInfo.getRealtimeTableConfig());
    assertTrue(routeInfo.isExists());
    assertFalse(routeInfo.isHybrid());
    assertTrue(routeInfo.isOffline());
    assertEquals(routeInfo.getOfflineTableName(), "testWithOfflineTables_OFFLINE");
    assertNull(routeInfo.getRealtimeTableName());
    assertTrue(routeInfo.isRouteExists());
    assertFalse(routeInfo.isDisabled());
    assertNull(routeInfo.getDisabledTableNames());
    assertNull(routeInfo.getTimeBoundaryInfo());
  }

  @DataProvider(name = "realtimeTableList")
  public Object[][] realtimeTableList() {
    return new Object[][]{
        {ImmutableList.of("a_REALTIME")}, {ImmutableList.of("a_REALTIME", "b_REALTIME")}, {
        ImmutableList.of("a_REALTIME", "b_REALTIME", "e_REALTIME")
    }
    };
  }

  @Test(dataProvider = "realtimeTableList")
  void testWithRealtimeTables(List<String> physicalTableNames) {
    String logicalTableName = "testWithRealtimeTables";
    LogicalTableConfig logicalTable = new LogicalTableConfig();
    logicalTable.setTableName(logicalTableName);
    Map<String, PhysicalTableConfig> tableConfigMap = new HashMap<>();
    for (String tableName : physicalTableNames) {
      tableConfigMap.put(tableName, new PhysicalTableConfig());
    }
    logicalTable.setPhysicalTableConfigMap(tableConfigMap);
    logicalTable.setBrokerTenant("brokerTenant");
    logicalTable.setRefRealtimeTableName(physicalTableNames.get(0));
    when(_tableCache.getLogicalTableConfig(eq(logicalTableName))).thenReturn(logicalTable);

    TableRouteInfo routeInfo =
        _logicalTableRouteProvider.getTableRouteInfo(logicalTable.getTableName(), _tableCache, _routingManager);
    assertNull(routeInfo.getOfflineTableConfig());
    assertNotNull(routeInfo.getRealtimeTableConfig());
    assertTrue(routeInfo.isExists());
    assertFalse(routeInfo.isHybrid());
    assertTrue(routeInfo.isRealtime());
    assertNull(routeInfo.getOfflineTableName());
    assertEquals(routeInfo.getRealtimeTableName(), "testWithRealtimeTables_REALTIME");
    assertTrue(routeInfo.isRouteExists());
    assertFalse(routeInfo.isDisabled());
    assertNull(routeInfo.getDisabledTableNames());
    assertNull(routeInfo.getTimeBoundaryInfo());
  }

  @DataProvider(name = "hybridTableName")
  public Object[][] hybridTableName() {
    return new Object[][]{
        {"e"},
    };
  }

  @Test(dataProvider = "hybridTableName")
  void testWithHybridTable(String hybridTableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(hybridTableName, "testWithHybridTable");
    assertNotNull(routeInfo.getOfflineTableConfig());
    assertNotNull(routeInfo.getRealtimeTableConfig());
    assertTrue(routeInfo.isExists());
    assertTrue(routeInfo.isHybrid());
    assertEquals(routeInfo.getOfflineTableName(), "testWithHybridTable_OFFLINE");
    assertEquals(routeInfo.getRealtimeTableName(), "testWithHybridTable_REALTIME");
    assertTrue(routeInfo.isRouteExists());
    assertFalse(routeInfo.isDisabled());
    assertNull(routeInfo.getDisabledTableNames());
    assertNotNull(routeInfo.getTimeBoundaryInfo());
  }

  @Test(dataProvider = "disabledTableProvider")
  void testWithDisabledPhysicalTable(String tableName) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, "testWithDisabledPhysicalTable");
    LogicalTableConfig logicalTableConfig = _tableCache.getLogicalTableConfig("testWithDisabledPhysicalTable");
    assertNotNull(logicalTableConfig);
    assertNotNull(logicalTableConfig.getPhysicalTableConfigMap());
    assertTrue(routeInfo.isExists());
    assertTrue(routeInfo.isDisabled());
    assertNotNull(routeInfo.getDisabledTableNames());
    assertEquals(new HashSet<>(routeInfo.getDisabledTableNames()),
        logicalTableConfig.getPhysicalTableConfigMap().keySet());
  }

  @DataProvider(name = "offlineTableWithOtherTables")
  public Object[][] offlineTableMixedList() {
    return new Object[][]{
        {ImmutableList.of("b_OFFLINE", "a_REALTIME")}, {
        ImmutableList.of("b_OFFLINE", "hybrid_o_disabled_REALTIME")
    }, {ImmutableList.of("b_OFFLINE", "no_route_table_O_REALTIME")}, {
        ImmutableList.of("b_OFFLINE", "no_route_table_R_OFFLINE")
    }, {ImmutableList.of("b_OFFLINE", "o_disabled_REALTIME")}, {ImmutableList.of("b_OFFLINE", "r_disabled_OFFLINE")},
    };
  }

  @Test(dataProvider = "offlineTableWithOtherTables")
  void testWithOfflineTableWithOtherTables(List<String> physicalTableNames) {
    String logicalTableName = "testWithOfflineTableWithOtherTables";
    LogicalTableConfig logicalTable = new LogicalTableConfig();
    logicalTable.setTableName(logicalTableName);
    Map<String, PhysicalTableConfig> tableConfigMap = new HashMap<>();
    for (String tableName : physicalTableNames) {
      tableConfigMap.put(tableName, new PhysicalTableConfig());
      if (logicalTable.getRefOfflineTableName() == null && TableNameBuilder.isOfflineTableResource(tableName)) {
        logicalTable.setRefOfflineTableName(tableName);
      } else if (logicalTable.getRefRealtimeTableName() == null && TableNameBuilder.isRealtimeTableResource(
          tableName)) {
        logicalTable.setRefRealtimeTableName(tableName);
      }
    }
    logicalTable.setPhysicalTableConfigMap(tableConfigMap);
    logicalTable.setBrokerTenant("brokerTenant");
    logicalTable.setTimeBoundaryConfig(new TimeBoundaryConfig("min", Map.of("includedTables", physicalTableNames)));
    when(_tableCache.getLogicalTableConfig(eq(logicalTableName))).thenReturn(logicalTable);

    TableRouteInfo routeInfo =
        _logicalTableRouteProvider.getTableRouteInfo(logicalTable.getTableName(), _tableCache, _routingManager);
    assertTrue(routeInfo.isExists());
    assertFalse(routeInfo.isDisabled());
    assertTrue(routeInfo.isRouteExists());
    assertTrue(routeInfo.hasOffline());
  }

  @DataProvider(name = "realTimeTableWithOtherTables")
  public Object[][] physicalTableMixedList() {
    return new Object[][]{
        {ImmutableList.of("a_REALTIME", "b_OFFLINE")}, {
        ImmutableList.of("a_REALTIME", "hybrid_o_disabled_OFFLINE")
    }, {ImmutableList.of("a_REALTIME", "no_route_table_O_OFFLINE")}, {
        ImmutableList.of("a_REALTIME", "no_route_table_R_REALTIME")
    }, {ImmutableList.of("a_REALTIME", "o_disabled_OFFLINE")}, {ImmutableList.of("a_REALTIME", "r_disabled_REALTIME")},
    };
  }

  @Test(dataProvider = "realTimeTableWithOtherTables")
  void testWithRealTimeTableWithOtherTables(List<String> physicalTableNames) {
    String logicalTableName = "testWithRealTimeTableWithOtherTables";
    LogicalTableConfig logicalTable = new LogicalTableConfig();
    logicalTable.setTableName(logicalTableName);
    Map<String, PhysicalTableConfig> tableConfigMap = new HashMap<>();
    for (String tableName : physicalTableNames) {
      tableConfigMap.put(tableName, new PhysicalTableConfig());
      if (logicalTable.getRefOfflineTableName() == null && TableNameBuilder.isOfflineTableResource(tableName)) {
        logicalTable.setRefOfflineTableName(tableName);
      } else if (logicalTable.getRefRealtimeTableName() == null && TableNameBuilder.isRealtimeTableResource(
          tableName)) {
        logicalTable.setRefRealtimeTableName(tableName);
      }
    }
    logicalTable.setPhysicalTableConfigMap(tableConfigMap);
    logicalTable.setBrokerTenant("brokerTenant");
    logicalTable.setTimeBoundaryConfig(new TimeBoundaryConfig("min", Map.of("includedTables", physicalTableNames)));
    when(_tableCache.getLogicalTableConfig(eq(logicalTableName))).thenReturn(logicalTable);

    TableRouteInfo routeInfo =
        _logicalTableRouteProvider.getTableRouteInfo(logicalTable.getTableName(), _tableCache, _routingManager);
    assertTrue(routeInfo.isExists());
    assertFalse(routeInfo.isDisabled());
    assertTrue(routeInfo.isRouteExists());
    assertTrue(routeInfo.hasRealtime());
  }
}
