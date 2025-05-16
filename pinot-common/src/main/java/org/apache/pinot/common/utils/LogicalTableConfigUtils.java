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
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTableConfigUtils {

  private LogicalTableConfigUtils() {
    // Utility class
  }

  public static LogicalTableConfig fromZNRecord(ZNRecord record)
      throws IOException {
    LogicalTableConfigBuilder builder = new LogicalTableConfigBuilder()
        .setTableName(record.getSimpleField(LogicalTableConfig.LOGICAL_TABLE_NAME_KEY))
        .setBrokerTenant(record.getSimpleField(LogicalTableConfig.BROKER_TENANT_KEY));

    if (record.getSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY) != null) {
      builder.setQueryConfig(JsonUtils.stringToObject(record.getSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY),
          QueryConfig.class));
    }
    if (record.getSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY) != null) {
      builder.setQuotaConfig(JsonUtils.stringToObject(record.getSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY),
          QuotaConfig.class));
    }
    if (record.getSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY) != null) {
      builder.setRefOfflineTableName(record.getSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY));
    }
    if (record.getSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY) != null) {
      builder.setRefRealtimeTableName(record.getSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY));
    }
    String timeBoundaryConfigJson = record.getSimpleField(LogicalTableConfig.TIME_BOUNDARY_CONFIG_KEY);
    if (timeBoundaryConfigJson != null) {
      builder.setTimeBoundaryConfig(JsonUtils.stringToObject(timeBoundaryConfigJson, TimeBoundaryConfig.class));
    }

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (Map.Entry<String, String> entry : record.getMapField(LogicalTableConfig.PHYSICAL_TABLE_CONFIG_KEY)
        .entrySet()) {
      String physicalTableName = entry.getKey();
      String physicalTableConfigJson = entry.getValue();
      physicalTableConfigMap.put(physicalTableName,
          JsonUtils.stringToObject(physicalTableConfigJson, PhysicalTableConfig.class));
    }
    builder.setPhysicalTableConfigMap(physicalTableConfigMap);
    return builder.build();
  }

  public static ZNRecord toZNRecord(LogicalTableConfig logicalTableConfig)
      throws JsonProcessingException {
    Map<String, String> physicalTableConfigMap = new HashMap<>();
    for (Map.Entry<String, PhysicalTableConfig> entry : logicalTableConfig.getPhysicalTableConfigMap().entrySet()) {
      String physicalTableName = entry.getKey();
      PhysicalTableConfig physicalTableConfig = entry.getValue();
      physicalTableConfigMap.put(physicalTableName, physicalTableConfig.toJsonString());
    }

    ZNRecord record = new ZNRecord(logicalTableConfig.getTableName());
    record.setSimpleField(LogicalTableConfig.LOGICAL_TABLE_NAME_KEY, logicalTableConfig.getTableName());
    record.setSimpleField(LogicalTableConfig.BROKER_TENANT_KEY, logicalTableConfig.getBrokerTenant());
    record.setMapField(LogicalTableConfig.PHYSICAL_TABLE_CONFIG_KEY, physicalTableConfigMap);

    if (logicalTableConfig.getQueryConfig() != null) {
      record.setSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY, logicalTableConfig.getQueryConfig().toJsonString());
    }
    if (logicalTableConfig.getQuotaConfig() != null) {
      record.setSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY, logicalTableConfig.getQuotaConfig().toJsonString());
    }
    if (logicalTableConfig.getRefOfflineTableName() != null) {
      record.setSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY,
          logicalTableConfig.getRefOfflineTableName());
    }
    if (logicalTableConfig.getRefRealtimeTableName() != null) {
      record.setSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY,
          logicalTableConfig.getRefRealtimeTableName());
    }
    if (logicalTableConfig.getTimeBoundaryConfig() != null) {
      record.setSimpleField(LogicalTableConfig.TIME_BOUNDARY_CONFIG_KEY,
          logicalTableConfig.getTimeBoundaryConfig().toJsonString());
    }
    return record;
  }

  public static void validateLogicalTableConfig(
      LogicalTableConfig logicalTableConfig,
      Predicate<String> physicalTableExistsPredicate,
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

    Set<String> offlineTableNames = new HashSet<>();
    Set<String> realtimeTableNames = new HashSet<>();

    for (Map.Entry<String, PhysicalTableConfig> entry : logicalTableConfig.getPhysicalTableConfigMap().entrySet()) {
      String physicalTableName = entry.getKey();
      PhysicalTableConfig physicalTableConfig = entry.getValue();

      // validate physical table exists
      if (!physicalTableExistsPredicate.test(physicalTableName)) {
        throw new IllegalArgumentException(
            "Invalid logical table. Reason: '" + physicalTableName + "' should be one of the existing tables");
      }
      // validate physical table config is not null
      if (physicalTableConfig == null) {
        throw new IllegalArgumentException(
            "Invalid logical table. Reason: 'physicalTableConfig' should not be null for physical table: "
                + physicalTableName);
      }

      if (TableNameBuilder.isOfflineTableResource(physicalTableName)) {
        offlineTableNames.add(physicalTableName);
      } else if (TableNameBuilder.isRealtimeTableResource(physicalTableName)) {
        realtimeTableNames.add(physicalTableName);
      }
    }

    // validate ref offline table name is not null or empty when offline tables exists
    if (!offlineTableNames.isEmpty() && StringUtils.isEmpty(logicalTableConfig.getRefOfflineTableName())) {
      throw new IllegalArgumentException(
          "Invalid logical table. Reason: 'refOfflineTableName' should not be null or empty when offline table exists");
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
}
