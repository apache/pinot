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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.ConfigRecord;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTableConfigUtils {

  private LogicalTableConfigUtils() {
    // Utility class
  }

  public static LogicalTableConfig fromZNRecord(ZNRecord znRecord)
      throws IOException {
    return LogicalTableConfigSerDeProvider.getInstance().fromZNRecord(znRecord);
  }

  public static ZNRecord toZNRecord(LogicalTableConfig logicalTableConfig)
      throws JsonProcessingException {
    ConfigRecord record = logicalTableConfig.toConfigRecord();
    ZNRecord znRecord = new ZNRecord(record.getId());
    znRecord.setSimpleFields(new HashMap<>(record.getSimpleFields()));
    for (Map.Entry<String, Map<String, String>> entry : record.getMapFields().entrySet()) {
      znRecord.setMapField(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    return znRecord;
  }

  public static void validateLogicalTableConfig(
      LogicalTableConfig logicalTableConfig,
      Predicate<String> brokerTenantExistsPredicate,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableName = logicalTableConfig.getTableName();
    if (StringUtils.isEmpty(tableName)) {
      throw new IllegalArgumentException("Invalid logical table name. Reason: 'tableName' should not be null or empty");
    }

    if (TableNameBuilder.isOfflineTableResource(tableName) || TableNameBuilder.isRealtimeTableResource(tableName)) {
      throw new IllegalArgumentException(
          "Invalid logical table name. Reason: 'tableName' should not end with _OFFLINE or _REALTIME");
    }

    if (logicalTableConfig.getPhysicalTableConfigMap() == null || logicalTableConfig.getPhysicalTableConfigMap()
        .isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'physicalTableConfigMap' should not be null or empty");
    }

    String databaseName = DatabaseUtils.extractDatabaseFromFullyQualifiedTableName(logicalTableConfig.getTableName());
    Set<String> offlineTableNames = new HashSet<>();
    Set<String> realtimeTableNames = new HashSet<>();

    for (Map.Entry<String, PhysicalTableConfig> entry : logicalTableConfig.getPhysicalTableConfigMap().entrySet()) {
      String physicalTableName = entry.getKey();
      PhysicalTableConfig physicalTableConfig = entry.getValue();

      // validate physical table config is not null
      if (physicalTableConfig == null) {
        throw new IllegalArgumentException(
            "Invalid logical table. Reason: 'physicalTableConfig' should not be null for physical table: "
                + physicalTableName);
      }

      // Skip database and existence validation for multi-cluster physical tables
      if (!physicalTableConfig.isMultiCluster()) {
        // validate database name matches
        String physicalTableDatabaseName = DatabaseUtils.extractDatabaseFromFullyQualifiedTableName(physicalTableName);
        if (!StringUtils.equalsIgnoreCase(databaseName, physicalTableDatabaseName)) {
          throw new IllegalArgumentException(
              "Invalid logical table. Reason: '" + physicalTableName
                  + "' should have the same database name as logical table: " + databaseName + " != "
                  + physicalTableDatabaseName);
        }
      }

      if (TableNameBuilder.isOfflineTableResource(physicalTableName)) {
        offlineTableNames.add(physicalTableName);
      } else if (TableNameBuilder.isRealtimeTableResource(physicalTableName)) {
        realtimeTableNames.add(physicalTableName);
      }
    }

    // validate ref offline table name is offline table type
    if (logicalTableConfig.getRefOfflineTableName() != null
        && !TableNameBuilder.isOfflineTableResource(logicalTableConfig.getRefOfflineTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refOfflineTableName' should be an offline table type");
    }

    // validate ref realtime table name is realtime table type
    if (logicalTableConfig.getRefRealtimeTableName() != null
        && !TableNameBuilder.isRealtimeTableResource(logicalTableConfig.getRefRealtimeTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refRealtimeTableName' should be a realtime table type");
    }

    // validate ref offline table name is not null or empty when offline tables exists
    if (!offlineTableNames.isEmpty() && StringUtils.isEmpty(logicalTableConfig.getRefOfflineTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refOfflineTableName' should not be null or empty when offline table exists");
    }

    // validate ref offline table name is null when offline tables is empty
    if (offlineTableNames.isEmpty() && !StringUtils.isEmpty(logicalTableConfig.getRefOfflineTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refOfflineTableName' should be null or empty when offline tables do not "
              + "exist");
    }

    // validate ref realtime table name is null when realtime tables is empty
    if (realtimeTableNames.isEmpty() && !StringUtils.isEmpty(logicalTableConfig.getRefRealtimeTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refRealtimeTableName' should be null or empty when realtime tables do not "
              + "exist");
    }

    // validate ref realtime table name is not null or empty when realtime tables exists
    if (!realtimeTableNames.isEmpty() && StringUtils.isEmpty(logicalTableConfig.getRefRealtimeTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refRealtimeTableName' should not be null or empty when realtime table "
              + "exists");
    }

    // validate ref offline table name is present in the offline tables
    if (!offlineTableNames.isEmpty() && !offlineTableNames.contains(logicalTableConfig.getRefOfflineTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refOfflineTableName' should be one of the provided offline tables");
    }

    // validate ref realtime table name is present in the realtime tables
    if (!realtimeTableNames.isEmpty() && !realtimeTableNames.contains(logicalTableConfig.getRefRealtimeTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refRealtimeTableName' should be one of the provided realtime tables");
    }

    // validate quota.storage is not set
    QuotaConfig quotaConfig = logicalTableConfig.getQuotaConfig();
    if (quotaConfig != null && !StringUtils.isEmpty(quotaConfig.getStorage())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'quota.storage' should not be set for logical table");
    }

    // validate broker tenant exists
    String brokerTenant = logicalTableConfig.getBrokerTenant();
    if (!brokerTenantExistsPredicate.test(brokerTenant)) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: '" + brokerTenant + "' should be one of the existing broker tenants");
    }

    // Validate schema with same name as logical table exists
    if (!ZKMetadataProvider.isSchemaExists(propertyStore, tableName)) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: Schema with same name as logical table '" + tableName + "' does not exist");
    }

    // validate time boundary config is not null for hybrid tables
    TimeBoundaryConfig timeBoundaryConfig = logicalTableConfig.getTimeBoundaryConfig();
    if (logicalTableConfig.isHybridLogicalTable() && timeBoundaryConfig == null) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'timeBoundaryConfig' should not be null for hybrid logical tables");
    }

    // time boundary strategy should not be null or empty
    if (timeBoundaryConfig != null && StringUtils.isEmpty(timeBoundaryConfig.getBoundaryStrategy())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'timeBoundaryConfig.boundaryStrategy' should not be null or empty");
    }

    // validate time boundary config parameters
    if (timeBoundaryConfig != null
        && (timeBoundaryConfig.getParameters() == null || timeBoundaryConfig.getParameters().isEmpty())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'timeBoundaryConfig.parameters' should not be null or empty");
    }
  }

  public static boolean checkPhysicalTableRefExists(LogicalTableConfig logicalTableConfig, String tableName) {
    Set<String> physicalTableNames = logicalTableConfig.getPhysicalTableConfigMap().keySet();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      return physicalTableNames.contains(offlineTableName) || physicalTableNames.contains(realtimeTableName);
    } else {
      return physicalTableNames.contains(tableName);
    }
  }

  public static void translatePhysicalTableNamesWithDB(LogicalTableConfig logicalTableConfig, HttpHeaders headers) {
    // Translate physical table names to include the database name
    Map<String, PhysicalTableConfig> physicalTableConfigMap = logicalTableConfig.getPhysicalTableConfigMap().entrySet()
        .stream()
        .map(entry -> Map.entry(DatabaseUtils.translateTableName(entry.getKey(), headers), entry.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    logicalTableConfig.setPhysicalTableConfigMap(physicalTableConfigMap);

    // Translate refOfflineTableName and refRealtimeTableName to include the database name
    String refOfflineTableName = logicalTableConfig.getRefOfflineTableName();
    if (refOfflineTableName != null) {
      logicalTableConfig.setRefOfflineTableName(DatabaseUtils.translateTableName(refOfflineTableName, headers));
    }
    String refRealtimeTableName = logicalTableConfig.getRefRealtimeTableName();
    if (refRealtimeTableName != null) {
      logicalTableConfig.setRefRealtimeTableName(DatabaseUtils.translateTableName(refRealtimeTableName, headers));
    }
  }
}
