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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.JsonUtils;


public class TableConfigUtils {
  private TableConfigUtils() {
  }

  private static final String FIELD_MISSING_MESSAGE_TEMPLATE = "Mandatory field '%s' is missing";

  public static TableConfig fromZNRecord(ZNRecord znRecord)
      throws IOException {
    Map<String, String> simpleFields = znRecord.getSimpleFields();

    // Mandatory fields
    String tableName = znRecord.getId();

    String tableType = simpleFields.get(TableConfig.TABLE_TYPE_KEY);
    Preconditions.checkState(tableType != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.TABLE_TYPE_KEY);

    String validationConfigString = simpleFields.get(TableConfig.VALIDATION_CONFIG_KEY);
    Preconditions
        .checkState(validationConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.VALIDATION_CONFIG_KEY);
    SegmentsValidationAndRetentionConfig validationConfig =
        JsonUtils.stringToObject(validationConfigString, SegmentsValidationAndRetentionConfig.class);

    String tenantConfigString = simpleFields.get(TableConfig.TENANT_CONFIG_KEY);
    Preconditions.checkState(tenantConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.TENANT_CONFIG_KEY);
    TenantConfig tenantConfig = JsonUtils.stringToObject(tenantConfigString, TenantConfig.class);

    String indexingConfigString = simpleFields.get(TableConfig.INDEXING_CONFIG_KEY);
    Preconditions
        .checkState(indexingConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.INDEXING_CONFIG_KEY);
    IndexingConfig indexingConfig = JsonUtils.stringToObject(indexingConfigString, IndexingConfig.class);

    String customConfigString = simpleFields.get(TableConfig.CUSTOM_CONFIG_KEY);
    Preconditions.checkState(customConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.CUSTOM_CONFIG_KEY);
    TableCustomConfig customConfig = JsonUtils.stringToObject(customConfigString, TableCustomConfig.class);

    // Optional fields
    QuotaConfig quotaConfig = null;
    String quotaConfigString = simpleFields.get(TableConfig.QUOTA_CONFIG_KEY);
    if (quotaConfigString != null) {
      quotaConfig = JsonUtils.stringToObject(quotaConfigString, QuotaConfig.class);
    }

    TableTaskConfig taskConfig = null;
    String taskConfigString = simpleFields.get(TableConfig.TASK_CONFIG_KEY);
    if (taskConfigString != null) {
      taskConfig = JsonUtils.stringToObject(taskConfigString, TableTaskConfig.class);
    }

    RoutingConfig routingConfig = null;
    String routingConfigString = simpleFields.get(TableConfig.ROUTING_CONFIG_KEY);
    if (routingConfigString != null) {
      routingConfig = JsonUtils.stringToObject(routingConfigString, RoutingConfig.class);
    }

    QueryConfig queryConfig = null;
    String queryConfigString = simpleFields.get(TableConfig.QUERY_CONFIG_KEY);
    if (queryConfigString != null) {
      queryConfig = JsonUtils.stringToObject(queryConfigString, QueryConfig.class);
    }

    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap = null;
    String instanceAssignmentConfigMapString = simpleFields.get(TableConfig.INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY);
    if (instanceAssignmentConfigMapString != null) {
      instanceAssignmentConfigMap = JsonUtils.stringToObject(instanceAssignmentConfigMapString,
          new TypeReference<Map<InstancePartitionsType, InstanceAssignmentConfig>>() {
          });
    }

    List<FieldConfig> fieldConfigList = null;
    String fieldConfigListString = simpleFields.get(TableConfig.FIELD_CONFIG_LIST_KEY);
    if (fieldConfigListString != null) {
      fieldConfigList = JsonUtils.stringToObject(fieldConfigListString, new TypeReference<List<FieldConfig>>() {
      });
    }

    UpsertConfig upsertConfig = null;
    String upsertConfigString = simpleFields.get(TableConfig.UPSERT_CONFIG_KEY);
    if (upsertConfigString != null) {
      upsertConfig = JsonUtils.stringToObject(upsertConfigString, UpsertConfig.class);
    }

    IngestionConfig ingestionConfig = null;
    String ingestionConfigString = simpleFields.get(TableConfig.INGESTION_CONFIG_KEY);
    if (ingestionConfigString != null) {
      ingestionConfig = JsonUtils.stringToObject(ingestionConfigString, IngestionConfig.class);
    }

    List<TierConfig> tierConfigList = null;
    String tierConfigListString = simpleFields.get(TableConfig.TIER_CONFIGS_LIST_KEY);
    if (tierConfigListString != null) {
      tierConfigList = JsonUtils.stringToObject(tierConfigListString, new TypeReference<List<TierConfig>>() {
      });
    }

    return new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
        quotaConfig, taskConfig, routingConfig, queryConfig, instanceAssignmentConfigMap, fieldConfigList, upsertConfig,
        ingestionConfig, tierConfigList);
  }

  public static ZNRecord toZNRecord(TableConfig tableConfig)
      throws JsonProcessingException {
    Map<String, String> simpleFields = new HashMap<>();

    // Mandatory fields
    simpleFields.put(TableConfig.TABLE_NAME_KEY, tableConfig.getTableName());
    simpleFields.put(TableConfig.TABLE_TYPE_KEY, tableConfig.getTableType().toString());
    simpleFields.put(TableConfig.VALIDATION_CONFIG_KEY, tableConfig.getValidationConfig().toJsonString());
    simpleFields.put(TableConfig.TENANT_CONFIG_KEY, tableConfig.getTenantConfig().toJsonString());
    simpleFields.put(TableConfig.INDEXING_CONFIG_KEY, tableConfig.getIndexingConfig().toJsonString());
    simpleFields.put(TableConfig.CUSTOM_CONFIG_KEY, tableConfig.getCustomConfig().toJsonString());

    // Optional fields
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    if (quotaConfig != null) {
      simpleFields.put(TableConfig.QUOTA_CONFIG_KEY, quotaConfig.toJsonString());
    }
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    if (taskConfig != null) {
      simpleFields.put(TableConfig.TASK_CONFIG_KEY, taskConfig.toJsonString());
    }
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      simpleFields.put(TableConfig.ROUTING_CONFIG_KEY, routingConfig.toJsonString());
    }
    QueryConfig queryConfig = tableConfig.getQueryConfig();
    if (queryConfig != null) {
      simpleFields.put(TableConfig.QUERY_CONFIG_KEY, queryConfig.toJsonString());
    }
    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        tableConfig.getInstanceAssignmentConfigMap();
    if (instanceAssignmentConfigMap != null) {
      simpleFields
          .put(TableConfig.INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY, JsonUtils.objectToString(instanceAssignmentConfigMap));
    }
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      simpleFields.put(TableConfig.FIELD_CONFIG_LIST_KEY, JsonUtils.objectToString(fieldConfigList));
    }
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null) {
      simpleFields.put(TableConfig.UPSERT_CONFIG_KEY, JsonUtils.objectToString(upsertConfig));
    }
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null) {
      simpleFields.put(TableConfig.INGESTION_CONFIG_KEY, JsonUtils.objectToString(ingestionConfig));
    }
    List<TierConfig> tierConfigList = tableConfig.getTierConfigsList();
    if (tierConfigList != null) {
      simpleFields.put(TableConfig.TIER_CONFIGS_LIST_KEY, JsonUtils.objectToString(tierConfigList));
    }

    ZNRecord znRecord = new ZNRecord(tableConfig.getTableName());
    znRecord.setSimpleFields(simpleFields);
    return znRecord;
  }
}
