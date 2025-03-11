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
package org.apache.pinot.query.planner.physical.table;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class HybridTable {
  private final List<PhysicalTable> _offlineTables;
  private final List<PhysicalTable> _realtimeTables;

  public static HybridTable from(String tableName, RoutingManager routingManager, TableCache tableCache)
      throws TableNotFoundException {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != null) {
      HybridTable hybridTable = from(List.of(tableName), routingManager, tableCache);
      if (hybridTable.isEmpty()) {
        // Check route and table config to throw the right exception.
        if (tableCache.getTableConfig(tableName) == null) {
          throw new TableNotFoundException("Table not found for request " + tableName);
        }
        throw new NoSuchElementException("No matches for table " + tableName);
      }
      return hybridTable;
    } else {
      // Generate hybrid table with both offline and realtime.
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      HybridTable hybridTable =
          from(List.of(offlineTableNameToCheck, realtimeTableNameToCheck), routingManager, tableCache);
      if (hybridTable.isEmpty()) {
        // Check route and table config to throw the right exception.
        if (!routingManager.routingExists(offlineTableNameToCheck) && !routingManager.routingExists(
            realtimeTableNameToCheck)) {
          throw new NoSuchElementException("No matches for table " + tableName);
        }
        throw new TableNotFoundException("Table not found for request " + tableName);
      }
      return hybridTable;
    }
  }

  public static HybridTable from(Collection<String> physicalTableNames, RoutingManager routingManager,
      TableCache tableCache)
      throws TableNotFoundException {
    List<PhysicalTable> offlineTables = new ArrayList<>();
    List<PhysicalTable> realtimeTables = new ArrayList<>();
    for (String physicalTableName : physicalTableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(physicalTableName);
      Preconditions.checkNotNull(tableType);
      if (routingManager.routingExists(physicalTableName)) {
        TableConfig tableConfig = tableCache.getTableConfig(physicalTableName);
        if (tableConfig != null) {
          if (tableType == TableType.OFFLINE) {
            offlineTables.add(new PhysicalTable(physicalTableName, physicalTableName, tableType, tableConfig,
                routingManager.isTableDisabled(physicalTableName)));
          } else {
            realtimeTables.add(new PhysicalTable(physicalTableName, physicalTableName, tableType, tableConfig,
                routingManager.isTableDisabled(physicalTableName)));
          }
        }
      }
    }
    return new HybridTable(offlineTables, realtimeTables);
  }

  private HybridTable(List<PhysicalTable> offlineTables, List<PhysicalTable> realtimeTables) {
    _offlineTables = offlineTables;
    _realtimeTables = realtimeTables;
  }

  public List<PhysicalTable> getOfflineTables() {
    return _offlineTables;
  }

  public List<PhysicalTable> getRealtimeTables() {
    return _realtimeTables;
  }

  public boolean isEmpty() {
    return _offlineTables.isEmpty() && _realtimeTables.isEmpty();
  }

  public boolean isDisabled() {
    return _offlineTables.stream().allMatch(PhysicalTable::isDisabled) && _realtimeTables.stream()
        .allMatch(PhysicalTable::isDisabled);
  }

  public List<PhysicalTable> getDisabledTables() {
    List<PhysicalTable> disabledTables = new ArrayList<>();
    _offlineTables.stream().filter(PhysicalTable::isDisabled).forEach(disabledTables::add);
    _realtimeTables.stream().filter(PhysicalTable::isDisabled).forEach(disabledTables::add);
    return disabledTables;
  }

  public boolean isHybrid() {
    return !_offlineTables.isEmpty() && !_realtimeTables.isEmpty();
  }

  public boolean isOffline() {
    return !_offlineTables.isEmpty() && _realtimeTables.isEmpty();
  }

  public boolean isRealtime() {
    return _offlineTables.isEmpty() && !_realtimeTables.isEmpty();
  }
}
