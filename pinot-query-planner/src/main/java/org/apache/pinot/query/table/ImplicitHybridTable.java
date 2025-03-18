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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ImplicitHybridTable represents the default definition of a hybrid table - If a table name does not have a type,
 * then it represents a OFFLINE and REALTIME table with the same raw table name.
 * If the table name has a type, then it represents the table with the given type.
 */
public class ImplicitHybridTable implements HybridTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImplicitHybridTable.class);

  private final String _rawTableName;
  private final PhysicalTable _offlineTable;
  private final PhysicalTable _realtimeTable;
  private final TimeBoundaryInfo _timeBoundaryInfo;

  /**
   * A factory method to create an ImplicitHybridTable from the given table name. Precedence is given to the existence
   * of a TableConfig. Other metadata such as routing and disabled status are also obtained and stored.
   * @param tableName Name of the table as specified in the query.
   * @param routingManager RoutingManager of the broker
   * @param tableCache TableCache of the broker
   * @return ImplicitHybridTable
   */
  public static ImplicitHybridTable from(String tableName, RoutingManager routingManager, TableCache tableCache) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    PhysicalTable offlineTable = PhysicalTable.EMPTY;
    PhysicalTable realtimeTable = PhysicalTable.EMPTY;
    TimeBoundaryInfo timeBoundaryInfo = null;

    if (tableType != null) {
      TableConfig tableConfig = tableCache.getTableConfig(tableName);
     if (tableConfig != null) {
       PhysicalTable physicalTable =
           new PhysicalTable(TableNameBuilder.extractRawTableName(tableName), tableName, tableType,
               routingManager.routingExists(tableName), tableConfig,
               routingManager.isTableDisabled(tableName));
       if (tableType == TableType.OFFLINE) {
         offlineTable = physicalTable;
       } else {
         realtimeTable = physicalTable;
       }
     }
    } else {
      // Generate hybrid table with both offline and realtime.
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      TableConfig offlineTableConfig = tableCache.getTableConfig(offlineTableNameToCheck);
      TableConfig realtimeTableConfig = tableCache.getTableConfig(realtimeTableNameToCheck);

      if (offlineTableConfig != null) {
        offlineTable =
            new PhysicalTable(TableNameBuilder.extractRawTableName(offlineTableNameToCheck), offlineTableNameToCheck,
                TableType.OFFLINE, routingManager.routingExists(offlineTableNameToCheck),
                offlineTableConfig, routingManager.isTableDisabled(offlineTableNameToCheck));
      }

      if (realtimeTableConfig != null) {
        realtimeTable =
            new PhysicalTable(TableNameBuilder.extractRawTableName(realtimeTableNameToCheck), realtimeTableNameToCheck,
                TableType.REALTIME, routingManager.routingExists(realtimeTableNameToCheck),
                realtimeTableConfig, routingManager.isTableDisabled(realtimeTableNameToCheck));
      }

      // Get TimeBoundaryInfo. If there is no
      if (!offlineTable.equals(PhysicalTable.EMPTY) && !realtimeTable.equals(PhysicalTable.EMPTY)) {
        // Time boundary info might be null when there is no segment in the offline table, query real-time side only
        timeBoundaryInfo = routingManager.getTimeBoundaryInfo(offlineTable.getTableNameWithType());
        if (timeBoundaryInfo == null) {
          LOGGER.debug("No time boundary info found for hybrid table: {}", tableName);
          offlineTable = PhysicalTable.EMPTY;
        }
      }
    }
    return new ImplicitHybridTable(TableNameBuilder.extractRawTableName(tableName), offlineTable, realtimeTable,
        timeBoundaryInfo);
  }

  private ImplicitHybridTable(String rawTableName, PhysicalTable offlineTable, PhysicalTable realtimeTable,
      TimeBoundaryInfo timeBoundaryInfo) {
    _rawTableName = rawTableName;
    _offlineTable = offlineTable;
    _realtimeTable = realtimeTable;
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  /**
   * Exists if there is at least one table with a TableConfig.
   * @return true if the table exists, false otherwise
   */
  @Override
  public boolean isExists() {
    return !(_offlineTable.equals(PhysicalTable.EMPTY) && _realtimeTable.equals(PhysicalTable.EMPTY));
  }

  /**
   * Route exists if at least one of the physical tables has a route.
   * @return true if a route exists, false otherwise
   */
  @Override
  public boolean isRouteExists() {
    if (isOffline()) {
      return _offlineTable.isRouteExists();
    } else if (isRealtime()) {
      return _realtimeTable.isRouteExists();
    } else {
      return _offlineTable.isRouteExists() || _realtimeTable.isRouteExists();
    }
  }

  @Override
  public boolean isOfflineRouteExists() {
    return _offlineTable.isRouteExists();
  }

  @Override
  public boolean isRealtimeRouteExists() {
    return _realtimeTable.isRouteExists();
  }

  /**
   * Disabled if all physical tables are disabled.
   * @return true if the table is disabled, false
   */
  @Override
  public boolean isDisabled() {
    if (isOffline()) {
      return _offlineTable.isDisabled();
    } else if (isRealtime()) {
      return _realtimeTable.isDisabled();
    } else {
      return _offlineTable.isDisabled() && _realtimeTable.isDisabled();
    }
  }

  /**
   * Hybrid if both offline and realtime table configs are present.
   * @return
   */
  @Override
  public boolean isHybrid() {
    return _offlineTable.getTableConfig() != null && _realtimeTable.getTableConfig() != null;
  }

  /**
   * Offline if offline table config is present and realtime table config is not present.
   * @return true if the table is offline, false otherwise
   */
  @Override
  public boolean isOffline() {
    return _offlineTable.getTableConfig() != null && _realtimeTable.getTableConfig() == null;
  }

  /**
   * Realtime if realtime table config is present and offline table config is not present.
   * @return true if the table is realtime, false otherwise
   */
  @Override
  public boolean isRealtime() {
    return _offlineTable.getTableConfig() == null && _realtimeTable.getTableConfig() != null;
  }

  /**
   * Offline if offline table config is present.
   * @return true if there is an OFFLINE table, false otherwise
   */
  @Override
  public boolean hasOffline() {
    return _offlineTable.getTableConfig() != null;
  }

  /**
   * Realtime if realtime table config is present.
   * @return true if there is a REALTIME table, false otherwise
   */
  @Override
  public boolean hasRealtime() {
    return _realtimeTable.getTableConfig() != null;
  }

  @Override
  @Nullable
  public List<PhysicalTable> getDisabledTables() {
    if (isOffline() && _offlineTable.isDisabled()) {
      return List.of(_offlineTable);
    } else if (isRealtime() && _realtimeTable.isDisabled()) {
      return List.of(_realtimeTable);
    } else if (isHybrid()) {
      if (_offlineTable.isDisabled() && _realtimeTable.isDisabled()) {
        return List.of(_offlineTable, _realtimeTable);
      } else if (_offlineTable.isDisabled()) {
        return List.of(_offlineTable);
      } else if (_realtimeTable.isDisabled()) {
        return List.of(_realtimeTable);
      }
    }

    return null;
  }

  @Override
  public List<PhysicalTable> getAllPhysicalTables() {
    if (isHybrid()) {
      return List.of(_offlineTable, _realtimeTable);
    } else if (isOffline()) {
      return List.of(_offlineTable);
    } else {
      return List.of(_realtimeTable);
    }
  }

  @Override
  @Nullable
  public List<PhysicalTable> getOfflineTables() {
    if (isOffline() || isHybrid()) {
      return List.of(_offlineTable);
    }
    return null;
  }

  @Override
  @Nullable
  public List<PhysicalTable> getRealtimeTables() {
    if (isRealtime() || isHybrid()) {
      return List.of(_realtimeTable);
    }
    return null;
  }

  @Override
  public boolean hasTimeBoundaryInfo() {
    return _timeBoundaryInfo != null;
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  @Nullable
  @Override
  public PhysicalTable getOfflineTable() {
    return _offlineTable.getTableConfig() != null ? _offlineTable : null;
  }

  @Nullable
  @Override
  public PhysicalTable getRealtimeTable() {
    return _realtimeTable.getTableConfig() != null ? _realtimeTable : null;
  }
}
