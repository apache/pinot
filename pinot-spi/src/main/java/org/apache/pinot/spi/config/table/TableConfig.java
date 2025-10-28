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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;


/**
 * Abstract base class representing table configurations. Implementations must have a default constructor, in order to
 * support creation from a {@link ZNRecord} through {@link #deserializeFromZNRecord(ZNRecord)}. Implementations must
 * also support creation through JSON deserialization via an {@link com.fasterxml.jackson.databind.ObjectReader}.
 */
public abstract class TableConfig extends BaseJsonConfig {
  public static final String TABLE_NAME_KEY = "tableName";
  public static final String TABLE_TYPE_KEY = "tableType";
  public static final String IS_DIM_TABLE_KEY = "isDimTable";
  public static final String VALIDATION_CONFIG_KEY = "segmentsConfig";
  public static final String TENANT_CONFIG_KEY = "tenants";
  public static final String INDEXING_CONFIG_KEY = "tableIndexConfig";
  public static final String CUSTOM_CONFIG_KEY = "metadata";
  public static final String QUOTA_CONFIG_KEY = "quota";
  public static final String TASK_CONFIG_KEY = "task";
  public static final String ROUTING_CONFIG_KEY = "routing";
  public static final String QUERY_CONFIG_KEY = "query";
  public static final String INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY = "instanceAssignmentConfigMap";
  public static final String INSTANCE_PARTITIONS_MAP_CONFIG_KEY = "instancePartitionsMap";
  public static final String SEGMENT_ASSIGNMENT_CONFIG_MAP_KEY = "segmentAssignmentConfigMap";
  public static final String FIELD_CONFIG_LIST_KEY = "fieldConfigList";
  public static final String UPSERT_CONFIG_KEY = "upsertConfig";
  public static final String DEDUP_CONFIG_KEY = "dedupConfig";
  public static final String DIMENSION_TABLE_CONFIG_KEY = "dimensionTableConfig";
  public static final String INGESTION_CONFIG_KEY = "ingestionConfig";
  public static final String TIER_CONFIGS_LIST_KEY = "tierConfigs";
  public static final String TUNER_CONFIG_LIST_KEY = "tunerConfigs";
  public static final String TIER_OVERWRITES_KEY = "tierOverwrites";

  // Double underscore is reserved for real-time segment name delimiter
  public static final String TABLE_NAME_FORBIDDEN_SUBSTRING = "__";

  public abstract TableConfig clone();

  public abstract String getTableName();

  public abstract void setTableName(String tableNameWithType);

  public abstract TableType getTableType();

  public abstract boolean isDimTable();

  public abstract SegmentsValidationAndRetentionConfig getValidationConfig();

  public abstract void setValidationConfig(SegmentsValidationAndRetentionConfig validationConfig);

  public abstract TenantConfig getTenantConfig();

  public abstract void setTenantConfig(TenantConfig tenantConfig);

  public abstract IndexingConfig getIndexingConfig();

  public abstract void setIndexingConfig(IndexingConfig indexingConfig);

  public abstract TableCustomConfig getCustomConfig();

  public abstract void setCustomConfig(TableCustomConfig customConfig);

  @Nullable
  public abstract QuotaConfig getQuotaConfig();

  public abstract void setQuotaConfig(QuotaConfig quotaConfig);

  @Nullable
  public abstract TableTaskConfig getTaskConfig();

  public abstract void setTaskConfig(TableTaskConfig taskConfig);

  @Nullable
  public abstract RoutingConfig getRoutingConfig();

  public abstract void setRoutingConfig(RoutingConfig routingConfig);

  @Nullable
  public abstract QueryConfig getQueryConfig();

  public abstract void setQueryConfig(QueryConfig queryConfig);

  @Nullable
  public abstract Map<String, InstanceAssignmentConfig> getInstanceAssignmentConfigMap();

  public abstract void setInstanceAssignmentConfigMap(
      Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap);

  public abstract Map<InstancePartitionsType, String> getInstancePartitionsMap();

  public abstract void setInstancePartitionsMap(Map<InstancePartitionsType, String> instancePartitionsMap);

  @Nullable
  public abstract List<FieldConfig> getFieldConfigList();

  public abstract void setFieldConfigList(List<FieldConfig> fieldConfigList);

  @Nullable
  public abstract UpsertConfig getUpsertConfig();

  public abstract void setUpsertConfig(UpsertConfig upsertConfig);

  public abstract boolean isUpsertEnabled();

  public abstract DedupConfig getDedupConfig();

  public abstract void setDedupConfig(DedupConfig dedupConfig);

  public abstract boolean isDedupEnabled();

  @Nullable
  public abstract DimensionTableConfig getDimensionTableConfig();

  public abstract void setDimensionTableConfig(DimensionTableConfig dimensionTableConfig);

  @Nullable
  public abstract IngestionConfig getIngestionConfig();

  public abstract void setIngestionConfig(IngestionConfig ingestionConfig);

  @Nullable
  public abstract List<TierConfig> getTierConfigsList();

  public abstract void setTierConfigsList(List<TierConfig> tierConfigsList);

  @Deprecated
  public abstract UpsertConfig.Mode getUpsertMode();

  public abstract List<TunerConfig> getTunerConfigsList();

  public abstract void setTunerConfigsList(List<TunerConfig> tunerConfigsList);

  @Nullable
  public abstract Map<String, SegmentAssignmentConfig> getSegmentAssignmentConfigMap();

  public abstract void setSegmentAssignmentConfigMap(Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap);

  public abstract int getReplication();

  public abstract ZNRecord toZNRecord() throws JsonProcessingException;

  public abstract void deserializeFromZNRecord(ZNRecord znRecord) throws IOException;
}
