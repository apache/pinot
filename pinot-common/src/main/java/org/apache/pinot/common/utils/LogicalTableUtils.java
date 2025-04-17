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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTableUtils {

  private LogicalTableUtils() {
    // Utility class
  }

  public static LogicalTable fromZNRecord(ZNRecord record)
      throws IOException {
    LogicalTableBuilder builder = new LogicalTableBuilder()
        .setTableName(record.getSimpleField(LogicalTable.LOGICAL_TABLE_NAME_KEY))
        .setBrokerTenant(record.getSimpleField(LogicalTable.BROKER_TENANT_KEY));

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (Map.Entry<String, String> entry : record.getMapField(LogicalTable.PHYSICAL_TABLE_CONFIG_KEY).entrySet()) {
      String physicalTableName = entry.getKey();
      String physicalTableConfigJson = entry.getValue();
      physicalTableConfigMap.put(physicalTableName,
          JsonUtils.stringToObject(physicalTableConfigJson, PhysicalTableConfig.class));
    }
    builder.setPhysicalTableConfigMap(physicalTableConfigMap);
    return builder.build();
  }

  public static ZNRecord toZNRecord(LogicalTable logicalTable)
      throws JsonProcessingException {
    Map<String, String> physicalTableConfigMap = new HashMap<>();
    for (Map.Entry<String, PhysicalTableConfig> entry : logicalTable.getPhysicalTableConfigMap().entrySet()) {
      String physicalTableName = entry.getKey();
      PhysicalTableConfig physicalTableConfig = entry.getValue();
      physicalTableConfigMap.put(physicalTableName, physicalTableConfig.toJsonString());
    }

    ZNRecord record = new ZNRecord(logicalTable.getTableName());
    record.setSimpleField(LogicalTable.LOGICAL_TABLE_NAME_KEY, logicalTable.getTableName());
    record.setSimpleField(LogicalTable.BROKER_TENANT_KEY, logicalTable.getBrokerTenant());
    record.setMapField(LogicalTable.PHYSICAL_TABLE_CONFIG_KEY, physicalTableConfigMap);
    return record;
  }

  public static void validateLogicalTableName(LogicalTable logicalTable, List<String> allPhysicalTables,
      Set<String> allBrokerTenantNames) {
    String tableName = logicalTable.getTableName();
    if (StringUtils.isEmpty(tableName)) {
      throw new IllegalArgumentException("Invalid logical table name. Reason: 'tableName' should not be null or empty");
    }

    if (TableNameBuilder.isOfflineTableResource(tableName) || TableNameBuilder.isRealtimeTableResource(tableName)) {
      throw new IllegalArgumentException(
          "Invalid logical table name. Reason: 'tableName' should not end with _OFFLINE or _REALTIME");
    }

    if (logicalTable.getPhysicalTableConfigMap() == null || logicalTable.getPhysicalTableConfigMap().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'physicalTableConfigMap' should not be null or empty");
    }

    for (Map.Entry<String, PhysicalTableConfig> entry : logicalTable.getPhysicalTableConfigMap().entrySet()) {
      String physicalTableName = entry.getKey();
      PhysicalTableConfig physicalTableConfig = entry.getValue();

      // validate physical table exists
      if (!allPhysicalTables.contains(physicalTableName)) {
        throw new IllegalArgumentException(
            "Invalid logical table. Reason: '" + physicalTableName + "' should be one of the existing tables");
      }
      // validate physical table config is not null
      if (physicalTableConfig == null) {
        throw new IllegalArgumentException(
            "Invalid logical table. Reason: 'physicalTableConfig' should not be null for physical table: "
                + physicalTableName);
      }
    }

    // validate broker tenant
    String brokerTenant = logicalTable.getBrokerTenant();
    if (!allBrokerTenantNames.contains(brokerTenant)) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: '" + brokerTenant + "' should be one of the existing broker tenants");
    }
  }
}
