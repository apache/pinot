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
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class HybridTable {
  private final List<PhysicalTable> _offlineTables;
  private final List<PhysicalTable> _realtimeTables;

  public static HybridTable from(String tableName, RoutingManager routingManager) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != null) {
      return from(List.of(tableName), routingManager);
    } else {
      // Generate hybrid table with both offline and realtime.
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      return from(List.of(offlineTableNameToCheck, realtimeTableNameToCheck), routingManager);
    }
  }

  public static HybridTable from(Collection<String> physicalTableNames, RoutingManager routingManager) {
    List<PhysicalTable> offlineTables = new ArrayList<>();
    List<PhysicalTable> realtimeTables = new ArrayList<>();
    for (String physicalTableName : physicalTableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(physicalTableName);
      Preconditions.checkNotNull(tableType);
      if (routingManager.routingExists(physicalTableName)) {
        if (tableType == TableType.OFFLINE) {
          offlineTables.add(new PhysicalTable(physicalTableName, physicalTableName, tableType, null,
              routingManager.isTableDisabled(physicalTableName)));
        } else {
          realtimeTables.add(new PhysicalTable(physicalTableName, physicalTableName, tableType, null,
              routingManager.isTableDisabled(physicalTableName)));
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

  public List<PhysicalTable> getAllPhysicalTables() {
    List<PhysicalTable> allPhysicalTables = new ArrayList<>();
    allPhysicalTables.addAll(_offlineTables);
    allPhysicalTables.addAll(_realtimeTables);
    return allPhysicalTables;
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
