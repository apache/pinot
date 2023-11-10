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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


@SuppressWarnings("unused")
public class TableConfig extends BaseJsonConfig {
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
  private static final String TABLE_NAME_FORBIDDEN_SUBSTRING = "__";

  /* MANDATORY FIELDS */

  @JsonPropertyDescription("The name for the table (with type suffix), e.g. \"myTable_OFFLINE\" (mandatory)")
  private final String _tableName;

  @JsonPropertyDescription(value = "The type of the table (OFFLINE|REALTIME) (mandatory)")
  private final TableType _tableType;

  @JsonPropertyDescription("Indicates whether the table is a dimension table or not")
  private final boolean _dimTable;

  private SegmentsValidationAndRetentionConfig _validationConfig;
  private TenantConfig _tenantConfig;
  private IndexingConfig _indexingConfig;

  // TODO: Make TableCustomConfig optional and use another key other than 'metadata'
  private TableCustomConfig _customConfig;

  /* OPTIONAL FIELDS */

  @JsonPropertyDescription("Resource quota associated with this table")
  private QuotaConfig _quotaConfig;

  private TableTaskConfig _taskConfig;
  private RoutingConfig _routingConfig;
  private QueryConfig _queryConfig;
  private Map<String, InstanceAssignmentConfig> _instanceAssignmentConfigMap;

  @JsonPropertyDescription(value = "Point to an existing instance partitions")
  private Map<InstancePartitionsType, String> _instancePartitionsMap;

  private Map<String, SegmentAssignmentConfig> _segmentAssignmentConfigMap;
  private List<FieldConfig> _fieldConfigList;

  @JsonPropertyDescription(value = "upsert related config")
  private UpsertConfig _upsertConfig;

  @JsonPropertyDescription(value = "Dedup related config")
  private DedupConfig _dedupConfig;

  @JsonPropertyDescription(value = "Dimension Table related config")
  private DimensionTableConfig _dimensionTableConfig;

  @JsonPropertyDescription(value = "Config related to ingesting data into the table")
  private IngestionConfig _ingestionConfig;

  @JsonPropertyDescription(value = "Configs for tiers of storage")
  private List<TierConfig> _tierConfigsList;

  @JsonPropertyDescription(value = "Configs for Table config tuner")
  private List<TunerConfig> _tunerConfigList;

  @JsonCreator
  public TableConfig(@JsonProperty(value = TABLE_NAME_KEY, required = true) String tableName,
      @JsonProperty(value = TABLE_TYPE_KEY, required = true) String tableType,
      @JsonProperty(value = VALIDATION_CONFIG_KEY, required = true)
          SegmentsValidationAndRetentionConfig validationConfig,
      @JsonProperty(value = TENANT_CONFIG_KEY, required = true) TenantConfig tenantConfig,
      @JsonProperty(value = INDEXING_CONFIG_KEY, required = true) IndexingConfig indexingConfig,
      @JsonProperty(value = CUSTOM_CONFIG_KEY, required = true) TableCustomConfig customConfig,
      @JsonProperty(QUOTA_CONFIG_KEY) @Nullable QuotaConfig quotaConfig,
      @JsonProperty(TASK_CONFIG_KEY) @Nullable TableTaskConfig taskConfig,
      @JsonProperty(ROUTING_CONFIG_KEY) @Nullable RoutingConfig routingConfig,
      @JsonProperty(QUERY_CONFIG_KEY) @Nullable QueryConfig queryConfig,
      @JsonProperty(INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY) @Nullable
          Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap,
      @JsonProperty(FIELD_CONFIG_LIST_KEY) @Nullable List<FieldConfig> fieldConfigList,
      @JsonProperty(UPSERT_CONFIG_KEY) @Nullable UpsertConfig upsertConfig,
      @JsonProperty(DEDUP_CONFIG_KEY) @Nullable DedupConfig dedupConfig,
      @JsonProperty(DIMENSION_TABLE_CONFIG_KEY) @Nullable DimensionTableConfig dimensionTableConfig,
      @JsonProperty(INGESTION_CONFIG_KEY) @Nullable IngestionConfig ingestionConfig,
      @JsonProperty(TIER_CONFIGS_LIST_KEY) @Nullable List<TierConfig> tierConfigsList,
      @JsonProperty(IS_DIM_TABLE_KEY) boolean dimTable,
      @JsonProperty(TUNER_CONFIG_LIST_KEY) @Nullable List<TunerConfig> tunerConfigList,
      @JsonProperty(INSTANCE_PARTITIONS_MAP_CONFIG_KEY) @Nullable
          Map<InstancePartitionsType, String> instancePartitionsMap,
      @JsonProperty(SEGMENT_ASSIGNMENT_CONFIG_MAP_KEY) @Nullable
          Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap) {
    Preconditions.checkArgument(tableName != null, "'tableName' must be configured");
    Preconditions.checkArgument(!tableName.contains(TABLE_NAME_FORBIDDEN_SUBSTRING),
        "'tableName' cannot contain double underscore ('__')");
    Preconditions.checkArgument(tableType != null, "'tableType' must be configured");
    Preconditions.checkArgument(validationConfig != null, "'segmentsConfig' must be configured");
    Preconditions.checkArgument(tenantConfig != null, "'tenants' must be configured");
    Preconditions.checkArgument(indexingConfig != null, "'tableIndexConfig' must be configured");
    Preconditions.checkArgument(customConfig != null, "'metadata' must be configured");

    // NOTE: Handle lower case table type and raw table name for backward-compatibility
    _tableType = TableType.valueOf(tableType.toUpperCase());
    _tableName = TableNameBuilder.forType(_tableType).tableNameWithType(tableName);
    _validationConfig = validationConfig;
    _tenantConfig = tenantConfig;
    _indexingConfig = indexingConfig;
    _customConfig = customConfig;
    _quotaConfig = quotaConfig;
    _taskConfig = taskConfig;
    _routingConfig = routingConfig;
    _queryConfig = queryConfig;
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
    _fieldConfigList = fieldConfigList;
    _upsertConfig = upsertConfig;
    _dedupConfig = dedupConfig;
    _dimensionTableConfig = dimensionTableConfig;
    _ingestionConfig = ingestionConfig;
    _tierConfigsList = tierConfigsList;
    _dimTable = dimTable;
    _tunerConfigList = tunerConfigList;
    _instancePartitionsMap = instancePartitionsMap;
    _segmentAssignmentConfigMap = segmentAssignmentConfigMap;
  }

  public TableConfig(TableConfig tableConfig) {
    _tableType = tableConfig.getTableType();
    _tableName = tableConfig.getTableName();
    _validationConfig = tableConfig.getValidationConfig();
    _tenantConfig = tableConfig.getTenantConfig();
    _indexingConfig = tableConfig.getIndexingConfig();
    _customConfig = tableConfig.getCustomConfig();
    _quotaConfig = tableConfig.getQuotaConfig();
    _taskConfig = tableConfig.getTaskConfig();
    _routingConfig = tableConfig.getRoutingConfig();
    _queryConfig = tableConfig.getQueryConfig();
    _instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    _fieldConfigList = tableConfig.getFieldConfigList();
    _upsertConfig = tableConfig.getUpsertConfig();
    _dedupConfig = tableConfig.getDedupConfig();
    _dimensionTableConfig = tableConfig.getDimensionTableConfig();
    _ingestionConfig = tableConfig.getIngestionConfig();
    _tierConfigsList = tableConfig.getTierConfigsList();
    _dimTable = tableConfig.isDimTable();
    _tunerConfigList = tableConfig.getTunerConfigsList();
    _instancePartitionsMap = tableConfig.getInstancePartitionsMap();
    _segmentAssignmentConfigMap = tableConfig.getSegmentAssignmentConfigMap();
  }

  @JsonProperty(TABLE_NAME_KEY)
  public String getTableName() {
    return _tableName;
  }

  @JsonProperty(TABLE_TYPE_KEY)
  public TableType getTableType() {
    return _tableType;
  }

  @JsonProperty(IS_DIM_TABLE_KEY)
  public boolean isDimTable() {
    return _dimTable;
  }

  @JsonProperty(VALIDATION_CONFIG_KEY)
  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return _validationConfig;
  }

  public void setValidationConfig(SegmentsValidationAndRetentionConfig validationConfig) {
    _validationConfig = validationConfig;
  }

  @JsonProperty(TENANT_CONFIG_KEY)
  public TenantConfig getTenantConfig() {
    return _tenantConfig;
  }

  public void setTenantConfig(TenantConfig tenantConfig) {
    _tenantConfig = tenantConfig;
  }

  @JsonProperty(INDEXING_CONFIG_KEY)
  public IndexingConfig getIndexingConfig() {
    return _indexingConfig;
  }

  public void setIndexingConfig(IndexingConfig indexingConfig) {
    _indexingConfig = indexingConfig;
  }

  @JsonProperty(CUSTOM_CONFIG_KEY)
  public TableCustomConfig getCustomConfig() {
    return _customConfig;
  }

  public void setCustomConfig(TableCustomConfig customConfig) {
    _customConfig = customConfig;
  }

  @JsonProperty(QUOTA_CONFIG_KEY)
  @Nullable
  public QuotaConfig getQuotaConfig() {
    return _quotaConfig;
  }

  public void setQuotaConfig(QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
  }

  @JsonProperty(TASK_CONFIG_KEY)
  @Nullable
  public TableTaskConfig getTaskConfig() {
    return _taskConfig;
  }

  public void setTaskConfig(TableTaskConfig taskConfig) {
    _taskConfig = taskConfig;
  }

  @JsonProperty(ROUTING_CONFIG_KEY)
  @Nullable
  public RoutingConfig getRoutingConfig() {
    return _routingConfig;
  }

  public void setRoutingConfig(RoutingConfig routingConfig) {
    _routingConfig = routingConfig;
  }

  @JsonProperty(QUERY_CONFIG_KEY)
  @Nullable
  public QueryConfig getQueryConfig() {
    return _queryConfig;
  }

  public void setQueryConfig(QueryConfig queryConfig) {
    _queryConfig = queryConfig;
  }

  @JsonProperty(INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY)
  @Nullable
  public Map<String, InstanceAssignmentConfig> getInstanceAssignmentConfigMap() {
    return _instanceAssignmentConfigMap;
  }

  public void setInstanceAssignmentConfigMap(
      Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap) {
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
  }

  @JsonProperty(INSTANCE_PARTITIONS_MAP_CONFIG_KEY)
  public Map<InstancePartitionsType, String> getInstancePartitionsMap() {
    return _instancePartitionsMap;
  }

  public void setInstancePartitionsMap(Map<InstancePartitionsType, String> instancePartitionsMap) {
    _instancePartitionsMap = instancePartitionsMap;
  }

  @JsonProperty(FIELD_CONFIG_LIST_KEY)
  @Nullable
  public List<FieldConfig> getFieldConfigList() {
    return _fieldConfigList;
  }

  public void setFieldConfigList(List<FieldConfig> fieldConfigList) {
    _fieldConfigList = fieldConfigList;
  }

  @Nullable
  public UpsertConfig getUpsertConfig() {
    return _upsertConfig;
  }

  public void setUpsertConfig(UpsertConfig upsertConfig) {
    _upsertConfig = upsertConfig;
  }

  @Nullable
  public DedupConfig getDedupConfig() {
    return _dedupConfig;
  }

  public void setDedupConfig(DedupConfig dedupConfig) {
    _dedupConfig = dedupConfig;
  }

  @JsonIgnore
  public boolean isDedupEnabled() {
    return _dedupConfig != null && _dedupConfig.isDedupEnabled();
  }

  @Nullable
  public DimensionTableConfig getDimensionTableConfig() {
    return _dimensionTableConfig;
  }

  public void setDimensionTableConfig(DimensionTableConfig dimensionTableConfig) {
    _dimensionTableConfig = dimensionTableConfig;
  }

  @JsonProperty(INGESTION_CONFIG_KEY)
  @Nullable
  public IngestionConfig getIngestionConfig() {
    return _ingestionConfig;
  }

  public void setIngestionConfig(IngestionConfig ingestionConfig) {
    _ingestionConfig = ingestionConfig;
  }

  @JsonProperty(TIER_CONFIGS_LIST_KEY)
  @Nullable
  public List<TierConfig> getTierConfigsList() {
    return _tierConfigsList;
  }

  public void setTierConfigsList(List<TierConfig> tierConfigsList) {
    _tierConfigsList = tierConfigsList;
  }

  @JsonIgnore
  public UpsertConfig.Mode getUpsertMode() {
    return _upsertConfig == null ? UpsertConfig.Mode.NONE : _upsertConfig.getMode();
  }

  @JsonIgnore
  public boolean isUpsertEnabled() {
    return _upsertConfig != null && _upsertConfig.getMode() != UpsertConfig.Mode.NONE;
  }

  @JsonIgnore
  @Nullable
  public List<String> getUpsertComparisonColumns() {
    return _upsertConfig == null ? null : _upsertConfig.getComparisonColumns();
  }

  @JsonIgnore
  @Nullable
  public String getUpsertDeleteRecordColumn() {
    return _upsertConfig == null ? null : _upsertConfig.getDeleteRecordColumn();
  }

  @JsonIgnore
  @Nullable
  public String getOutOfOrderRecordColumn() {
    return _upsertConfig == null ? null : _upsertConfig.getOutOfOrderRecordColumn();
  }

  @JsonIgnore
  public boolean isDropOutOfOrderRecord() {
    return _upsertConfig != null && _upsertConfig.isDropOutOfOrderRecord();
  }

  @JsonProperty(TUNER_CONFIG_LIST_KEY)
  public List<TunerConfig> getTunerConfigsList() {
    return _tunerConfigList;
  }

  public void setTunerConfigsList(List<TunerConfig> tunerConfigList) {
    _tunerConfigList = tunerConfigList;
  }

  @JsonProperty(SEGMENT_ASSIGNMENT_CONFIG_MAP_KEY)
  @Nullable
  public Map<String, SegmentAssignmentConfig> getSegmentAssignmentConfigMap() {
    return _segmentAssignmentConfigMap;
  }

  public void setSegmentAssignmentConfigMap(Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap) {
    _segmentAssignmentConfigMap = segmentAssignmentConfigMap;
  }

  @JsonIgnore
  public int getReplication() {
    if (_tableType == TableType.REALTIME) {
      // Use replicasPerPartition for real-time table if exists
      String replicasPerPartition = _validationConfig.getReplicasPerPartition();
      if (replicasPerPartition != null) {
        return Integer.parseInt(replicasPerPartition);
      }
    }
    return Integer.parseInt(_validationConfig.getReplication());
  }
}
