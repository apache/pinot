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
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.JsonUtils;


@AutoService(LogicalTableConfigSerDe.class)
public class DefaultLogicalTableConfigSerDe implements LogicalTableConfigSerDe {

  @Override
  public LogicalTableConfig fromZNRecord(ZNRecord znRecord)
      throws IOException {
    LogicalTableConfig config = createConfig();
    populateFromZNRecord(config, znRecord);
    return config;
  }

  protected LogicalTableConfig createConfig() {
    return new LogicalTableConfig();
  }

  protected void populateFromZNRecord(LogicalTableConfig config, ZNRecord znRecord)
      throws IOException {
    config.setTableName(znRecord.getSimpleField(LogicalTableConfig.LOGICAL_TABLE_NAME_KEY));
    config.setBrokerTenant(znRecord.getSimpleField(LogicalTableConfig.BROKER_TENANT_KEY));

    String queryConfigJson = znRecord.getSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY);
    if (queryConfigJson != null) {
      config.setQueryConfig(JsonUtils.stringToObject(queryConfigJson, QueryConfig.class));
    }
    String quotaConfigJson = znRecord.getSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY);
    if (quotaConfigJson != null) {
      config.setQuotaConfig(JsonUtils.stringToObject(quotaConfigJson, QuotaConfig.class));
    }
    String refOfflineTableName = znRecord.getSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY);
    if (refOfflineTableName != null) {
      config.setRefOfflineTableName(refOfflineTableName);
    }
    String refRealtimeTableName = znRecord.getSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY);
    if (refRealtimeTableName != null) {
      config.setRefRealtimeTableName(refRealtimeTableName);
    }
    String timeBoundaryConfigJson = znRecord.getSimpleField(LogicalTableConfig.TIME_BOUNDARY_CONFIG_KEY);
    if (timeBoundaryConfigJson != null) {
      config.setTimeBoundaryConfig(JsonUtils.stringToObject(timeBoundaryConfigJson, TimeBoundaryConfig.class));
    }

    populatePhysicalTableConfigs(config, znRecord);
  }

  protected void populatePhysicalTableConfigs(LogicalTableConfig config, ZNRecord znRecord)
      throws IOException {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    Map<String, String> physicalTableMapField = znRecord.getMapField(LogicalTableConfig.PHYSICAL_TABLE_CONFIG_KEY);
    if (physicalTableMapField != null) {
      for (Map.Entry<String, String> entry : physicalTableMapField.entrySet()) {
        physicalTableConfigMap.put(entry.getKey(),
            JsonUtils.stringToObject(entry.getValue(), PhysicalTableConfig.class));
      }
    }
    config.setPhysicalTableConfigMap(physicalTableConfigMap);
  }

  @Override
  public ZNRecord toZNRecord(LogicalTableConfig config)
      throws JsonProcessingException {
    ZNRecord znRecord = new ZNRecord(config.getTableName());
    writeToZNRecord(config, znRecord);
    return znRecord;
  }

  protected void writeToZNRecord(LogicalTableConfig config, ZNRecord znRecord)
      throws JsonProcessingException {
    znRecord.setSimpleField(LogicalTableConfig.LOGICAL_TABLE_NAME_KEY, config.getTableName());
    znRecord.setSimpleField(LogicalTableConfig.BROKER_TENANT_KEY, config.getBrokerTenant());

    if (config.getQueryConfig() != null) {
      znRecord.setSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY, config.getQueryConfig().toJsonString());
    }
    if (config.getQuotaConfig() != null) {
      znRecord.setSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY, config.getQuotaConfig().toJsonString());
    }
    if (config.getRefOfflineTableName() != null) {
      znRecord.setSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY, config.getRefOfflineTableName());
    }
    if (config.getRefRealtimeTableName() != null) {
      znRecord.setSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY, config.getRefRealtimeTableName());
    }
    if (config.getTimeBoundaryConfig() != null) {
      znRecord.setSimpleField(LogicalTableConfig.TIME_BOUNDARY_CONFIG_KEY,
          config.getTimeBoundaryConfig().toJsonString());
    }

    Map<String, String> physicalTableConfigMapField = new HashMap<>();
    for (Map.Entry<String, PhysicalTableConfig> entry : config.getPhysicalTableConfigMap().entrySet()) {
      physicalTableConfigMapField.put(entry.getKey(), entry.getValue().toJsonString());
    }
    znRecord.setMapField(LogicalTableConfig.PHYSICAL_TABLE_CONFIG_KEY, physicalTableConfigMapField);
  }
}
