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
package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTableUtils {
  private LogicalTableUtils() {
  }

  public static class HybridTableName {
    private final String _offlineTableName;
    private final String _realtimeTableName;
    private final String _rawTableName;

    public HybridTableName(String offlineTableName, String realtimeTableName, String rawTableName) {
      _offlineTableName = offlineTableName;
      _realtimeTableName = realtimeTableName;
      _rawTableName = rawTableName;
    }

    public String getOfflineTableName() {
      return _offlineTableName;
    }

    public String getRealtimeTableName() {
      return _realtimeTableName;
    }

    public String getRawTableName() {
      return _rawTableName;
    }
  }

  public static class HybridTableConfig {
    private final TableConfig _offlineTableConfig;
    private final TableConfig _realtimeTableConfig;

    public HybridTableConfig(TableConfig offlineTableConfig, TableConfig realtimeTableConfig) {
      _offlineTableConfig = offlineTableConfig;
      _realtimeTableConfig = realtimeTableConfig;
    }

    public TableConfig getOfflineTableConfig() {
      return _offlineTableConfig;
    }

    public TableConfig getRealtimeTableConfig() {
      return _realtimeTableConfig;
    }
  }

  public static HybridTableName getHybridTableName(String tableName, RoutingManager routingManager) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

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

    return new HybridTableName(offlineTableName, realtimeTableName, rawTableName);
  }

  public static HybridTableConfig getHybridTableConfig(HybridTableName hybridTableName, TableCache tableCache) {
    TableConfig offlineTableConfig =
        tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(hybridTableName.getRawTableName()));
    TableConfig realtimeTableConfig =
        tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(hybridTableName.getRawTableName()));

    return new HybridTableConfig(offlineTableConfig, realtimeTableConfig);
  }

  public static class Exception extends java.lang.Exception {
    private final int _errorCode;

    Exception(int errorCode, String errorMessage) {
      super(errorMessage);
      _errorCode = errorCode;
    }

    public int getErrorCode() {
      return _errorCode;
    }
  }
}
