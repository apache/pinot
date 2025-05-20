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

import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.core.transport.TableRouteInfo;
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


/**
 * This test class is used to test the {@link ImplicitHybridTableRouteProvider} getTableRouteInfo function.
 */
public class ImplicitHybridTableRouteProviderGetTableRouteTest extends BaseTableRouteTest {
  @Test(dataProvider = "offlineTableProvider")
  public void testOfflineTable(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isOffline(), "The table should be offline");
  }

  @Test(dataProvider = "realtimeTableProvider")
  public void testRealtimeTable(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isRealtime(), "The table should be realtime");
  }

  @Test(dataProvider = "hybridTableProvider")
  public void testHybridTable(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);

    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isHybrid(), "The table should be hybrid");
  }

  /**
   * Table 'b' has not time boundary. So it is considered a realtime routeInfo.
   */
  @Test
  public void testWithNoTimeBoundary() {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo("b", _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isRealtime(), "The table should be realtime");
  }

  @Test(dataProvider = "nonExistentTableProvider")
  public void testNonExistentTableName(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertFalse(routeInfo.isExists(), "The table should not exist");
  }

  @Test(dataProvider = "routeExistsProvider")
  public void testRouteExists(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isRouteExists(), "The table should have route");
  }

  @Test(dataProvider = "routeNotExistsProvider")
  public void testRouteNotExists(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertFalse(routeInfo.isRouteExists(), "The table should not have route");
  }

  @Test(dataProvider = "notDisabledTableProvider")
  public void testNotDisabledTable(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertFalse(routeInfo.isDisabled(), "The table should not be disabled");
  }

  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledTable(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertFalse(routeInfo.isDisabled(), "The table should be disabled");
  }

  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledTable(String parameter) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(parameter, _tableCache, _routingManager);
    assertTrue(routeInfo.isExists(), "The table should exist");
    assertTrue(routeInfo.isDisabled(), "The table should not have route");
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

    /**
     * Similar if offlineTableName is not null and there is a route in the routing routeInfo. Same for
     * realtimeTableName.
     */
    boolean similar(TableRouteInfo routeInfo) {
      boolean isEquals = true;

      if (_offlineTableName != null) {
        isEquals &= routeInfo.hasOffline() && routeInfo.isOfflineRouteExists() && _offlineTableName.equals(
            routeInfo.getOfflineTableName());
      } else {
        isEquals &= !routeInfo.hasOffline() || !routeInfo.isOfflineRouteExists();
      }

      if (_realtimeTableName != null) {
        isEquals &= routeInfo.hasRealtime() && routeInfo.isRealtimeRouteExists() && _realtimeTableName.equals(
            routeInfo.getRealtimeTableName());
      } else {
        isEquals &= !routeInfo.hasRealtime() || !routeInfo.isRealtimeRouteExists();
      }

      return isEquals;
    }
  }

  /**
   * This method implements the previous version to check if a table exists.
   * It is almost an exact copy-paste of a section from BaseSingleStageBrokerRequestHandler.
   * It takes as input two pieces of metadata:
   * - TableConfig
   * - Route
   * If both existed, then a physical table is available. This method is used to test the new implementations of
   * ImplicitTableRouteInfo and ImplicitTableRouteProvider behave in the same way.
   *
   * If there is an error i.e. either table config or router is not found, then the BrokerResponse with right
   * error code is set.
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

  /**
   * This test checks tables that have a table config and an entry in routing manager.
   * It makes sure that getTableNameAndConfig() behaves the same way as ImplicitTableRouteComputer.
   */
  @Test(dataProvider = "tableNameAndConfigSuccessProvider")
  public void testTableNameAndConfigSuccess(String tableName) {
    TableNameAndConfig tableNameAndConfig = getTableNameAndConfig(tableName);
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(tableName, _tableCache, _routingManager);
    assertTrue(tableNameAndConfig.similar(routeInfo), "The table name and config should match the hybrid table");
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

  /**
   * This test checks tables that do not have a table config or an entry in routing manager.
   */
  @Test(dataProvider = "tableNameAndConfigFailureProvider")
  public void testTableNameAndConfigFailure(String tableName) {
    TableNameAndConfig tableNameAndConfig = getTableNameAndConfig(tableName);
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(tableName, _tableCache, _routingManager);

    // getTableNameAndConfig() returns an error as a BrokerResponse with the right error code.
    assertNotNull(tableNameAndConfig._brokerResponse);
    assertTrue(tableNameAndConfig._brokerResponse == BrokerResponseNative.TABLE_DOES_NOT_EXIST
        || tableNameAndConfig._brokerResponse == BrokerResponseNative.NO_TABLE_RESULT);

    if (tableNameAndConfig._brokerResponse == BrokerResponseNative.TABLE_DOES_NOT_EXIST) {
      assertFalse(routeInfo.isExists(), "The table should not exist");
    } else {
      assertFalse(routeInfo.isRouteExists(), "The table should not have route");
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

  /**
   * This function is almost an exact copy of the section in BaseSingleStageBrokerRequestHandler.doHandleRequest()
   * that checks if a table is disabled and returns the right error code. There are multiple types of responses.
   * Only realtime table which is disabled, then return broker response with error code TABLE_IS_DISABLED.
   * Only offline table which is disabled, then return broker response with error code TABLE_IS_DISABLED.
   * Hybrid table and all tables are disabled, then return broker response with error code TABLE_IS_DISABLED.
   * Hybrid table and one of the tables is disabled, then return exception with error code TABLE_IS_DISABLED.
   * @param offlineTableDisabled is the offline table disabled
   * @param realtimeTableDisabled is the realtime table disabled
   * @param offlineTableConfig Offline Table Config
   * @param realtimeTableConfig Realtime Table Config
   * @return The error code if the table is disabled, null otherwise
   */
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

  /**
   * If a table is not disabled, then checkTableDisabled() should return null.
   * ImplicitTableRouteComputer should not be disabled.
   */
  @Test(dataProvider = "notDisabledTableProvider")
  public void testNotDisabledWithCheckDisabled(String tableName) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(tableName, _tableCache, _routingManager);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(routeInfo.hasOffline() && routeInfo.isOfflineTableDisabled(),
            routeInfo.hasRealtime() && routeInfo.isRealtimeTableDisabled(),
            routeInfo.hasOffline() ? routeInfo.getOfflineTableConfig() : null,
            routeInfo.hasRealtime() ? routeInfo.getRealtimeTableConfig() : null);

    assertNull(exceptionOrResponse);
    assertFalse(routeInfo.isDisabled());
  }

  /**
   * In a hybrid table, if one of the tables is disabled, then checkTableDisabled() should return an exception.
   * ImplicitTableRouteComputer should not be disabled.
   */
  @Test(dataProvider = "partiallyDisabledTableProvider")
  public void testPartiallyDisabledWithCheckDisabled(String tableName) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(tableName, _tableCache, _routingManager);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(routeInfo.hasOffline() && routeInfo.isOfflineTableDisabled(),
            routeInfo.hasRealtime() && routeInfo.isRealtimeTableDisabled(),
            routeInfo.hasOffline() ? routeInfo.getOfflineTableConfig() : null,
            routeInfo.hasRealtime() ? routeInfo.getRealtimeTableConfig() : null);

    assertNotNull(exceptionOrResponse);
    assertNull(exceptionOrResponse._brokerResponse);
    assertNotNull(exceptionOrResponse._exception);
    assertFalse(routeInfo.isDisabled());
  }

  /**
   * If a table is disabled, then checkTableDisabled() should return a broker response with error code
   * TABLE_IS_DISABLED.
   * ImplicitTableRouteComputer should be disabled.
   */
  @Test(dataProvider = "disabledTableProvider")
  public void testDisabledWithCheckDisabled(String tableName) {
    TableRouteInfo routeInfo = _hybridTableRouteProvider.getTableRouteInfo(tableName, _tableCache, _routingManager);

    ExceptionOrResponse exceptionOrResponse =
        checkTableDisabled(routeInfo.hasOffline() && routeInfo.isOfflineTableDisabled(),
            routeInfo.hasRealtime() && routeInfo.isRealtimeTableDisabled(),
            routeInfo.hasOffline() ? routeInfo.getOfflineTableConfig() : null,
            routeInfo.hasRealtime() ? routeInfo.getRealtimeTableConfig() : null);

    assertNotNull(exceptionOrResponse);
    assertNull(exceptionOrResponse._exception);
    assertNotNull(exceptionOrResponse._brokerResponse);
    assertTrue(routeInfo.isDisabled());
  }
}
