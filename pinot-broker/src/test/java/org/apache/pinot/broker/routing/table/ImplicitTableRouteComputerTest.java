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

import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ImplicitTableRouteComputerTest extends BaseTableRouteTest {


  @Test(dataProvider = "offlineTableProvider")
  public void testOfflineTable(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);

    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isOffline(), "The table should be offline");
  }


  @Test(dataProvider = "realtimeTableProvider")
  public void testRealtimeTable(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);

    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isRealtime(), "The table should be realtime");
  }


  @Test(dataProvider = "hybridTableProvider")
  public void testHybridTable(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);

    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isHybrid(), "The table should be hybrid");
  }

  /**
   * Table 'b' has not time boundary. So it is considered a realtime table.
   */
  @Test
  public void testWithNoTimeBoundary() {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer("b");
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isRealtime(), "The table should be realtime");
  }


  @Test(dataProvider = "nonExistentTableProvider")
  public void testNonExistentTableName(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);

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
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isRouteExists(), "The table should have route");
  }


  @Test(dataProvider = "routeNotExistsProvider")
  public void testRouteNotExists(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(table.isExists(), "The table should exist");
    assertFalse(table.isRouteExists(), "The table should not have route");
  }

  @Test(dataProvider = "notDisabledTableProvider")
  public void testNotDisabledTable(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(table.isExists(), "The table should exist");
    assertFalse(table.isDisabled(), "The table should not be disabled");
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledTable(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(table.isExists(), "The table should exist");
    assertFalse(table.isDisabled(), "The table should be disabled");
  }

  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledTable(String parameter) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(parameter);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(table.isExists(), "The table should exist");
    assertTrue(table.isDisabled(), "The table should not have route");
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

    boolean similar(ImplicitTableRouteComputer tableRoute) {
      boolean isEquals = true;

      if (_offlineTableName != null) {
        isEquals &= tableRoute.hasOffline() && tableRoute.isOfflineRouteExists()
            && _offlineTableName.equals(tableRoute.getOfflineTableName());
      } else {
        isEquals &= !tableRoute.hasOffline() || !tableRoute.isOfflineRouteExists();
      }

      if (_realtimeTableName != null) {
        isEquals &= tableRoute.hasRealtime() && tableRoute.isRealtimeRouteExists()
            && _realtimeTableName.equals(tableRoute.getRealtimeTableName());
      } else {
        isEquals &= !tableRoute.hasRealtime() || !tableRoute.isRealtimeRouteExists();
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
        // {"b"}, This table is not a hybrid table because of no time boundary. Do not test it here.
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
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(tableName);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertTrue(tableNameAndConfig.similar(table), "The table name and config should match the hybrid table");
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
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(tableName);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    assertNotNull(tableNameAndConfig._brokerResponse);
    assertTrue(tableNameAndConfig._brokerResponse == BrokerResponseNative.TABLE_DOES_NOT_EXIST
        || tableNameAndConfig._brokerResponse == BrokerResponseNative.NO_TABLE_RESULT);

    if (tableNameAndConfig._brokerResponse == BrokerResponseNative.TABLE_DOES_NOT_EXIST) {
      assertFalse(table.isExists(), "The table should not exist");
    } else {
      assertFalse(table.isRouteExists(), "The table should not have route");
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
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(tableName);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(table.hasOffline() && table.isOfflineTableDisabled(),
            table.hasRealtime() && table.isRealtimeTableDisabled(),
            table.hasOffline() ? table.getOfflineTableConfig() : null,
            table.hasRealtime() ? table.getRealtimeTableConfig() : null);

    assertNull(exceptionOrResponse);
    assertFalse(table.isDisabled());
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledWithCheckDisabled(String tableName) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(tableName);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(table.hasOffline() && table.isOfflineTableDisabled(),
            table.hasRealtime() && table.isRealtimeTableDisabled(),
            table.hasOffline() ? table.getOfflineTableConfig() : null,
            table.hasRealtime() ? table.getRealtimeTableConfig() : null);

    assertNotNull(exceptionOrResponse);
    assertNull(exceptionOrResponse._brokerResponse);
    assertNotNull(exceptionOrResponse._exception);
    assertFalse(table.isDisabled());
  }

  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledWithCheckDisabled(String tableName) {
    ImplicitTableRouteComputer table = new ImplicitTableRouteComputer(tableName);
    table.getTableConfig(_tableCache);
    table.checkRoutes(_routingManager);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(table.hasOffline() && table.isOfflineTableDisabled(),
            table.hasRealtime() && table.isRealtimeTableDisabled(),
            table.hasOffline() ? table.getOfflineTableConfig() : null,
            table.hasRealtime() ? table.getRealtimeTableConfig() : null);

    assertNotNull(exceptionOrResponse);
    assertNull(exceptionOrResponse._exception);
    assertNotNull(exceptionOrResponse._brokerResponse);
    assertTrue(table.isDisabled());
  }
}
