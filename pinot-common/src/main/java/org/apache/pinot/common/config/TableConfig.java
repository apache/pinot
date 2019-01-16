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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.startree.hll.HllConfig;


@SuppressWarnings("unused")
@ConfigDoc(value = "Configuration for a table", mandatory = true)
@ConfigKey("table")
public class TableConfig {
  public static final String TABLE_NAME_KEY = "tableName";
  public static final String TABLE_TYPE_KEY = "tableType";
  public static final String VALIDATION_CONFIG_KEY = "segmentsConfig";
  public static final String TENANT_CONFIG_KEY = "tenants";
  public static final String INDEXING_CONFIG_KEY = "tableIndexConfig";
  public static final String CUSTOM_CONFIG_KEY = "metadata";
  public static final String QUOTA_CONFIG_KEY = "quota";
  public static final String TASK_CONFIG_KEY = "task";
  public static final String ROUTING_CONFIG_KEY = "routing";

  @ConfigKey("name")
  @ConfigDoc(value = "The name for the table.", mandatory = true, exampleValue = "myTable")
  private String _tableName;

  @ConfigKey("type")
  @ConfigDoc(value = "The type of the table, either realtime or offline", mandatory = true)
  private TableType _tableType;

  @NestedConfig
  private SegmentsValidationAndRetentionConfig _validationConfig;

  @NestedConfig
  private TenantConfig _tenantConfig;

  @NestedConfig
  private IndexingConfig _indexingConfig;

  @NestedConfig
  private TableCustomConfig _customConfig;

  @ConfigKey("quota")
  @ConfigDoc("Resource quota associated with this table")
  private QuotaConfig _quotaConfig;

  @NestedConfig
  private TableTaskConfig _taskConfig;

  @NestedConfig
  private RoutingConfig _routingConfig;

  public TableConfig() {
    // TODO: currently these 2 fields are annotated as non-null. Revisit to see whether that's necessary
    _tenantConfig = new TenantConfig();
    _customConfig = new TableCustomConfig();
  }

  private TableConfig(@Nonnull String tableName, @Nonnull TableType tableType,
      @Nonnull SegmentsValidationAndRetentionConfig validationConfig, @Nonnull TenantConfig tenantConfig,
      @Nonnull IndexingConfig indexingConfig, @Nonnull TableCustomConfig customConfig,
      @Nullable QuotaConfig quotaConfig, @Nullable TableTaskConfig taskConfig, @Nullable RoutingConfig routingConfig) {
    _tableName = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    _tableType = tableType;
    _validationConfig = validationConfig;
    _tenantConfig = tenantConfig;
    _indexingConfig = indexingConfig;
    _customConfig = customConfig;
    _quotaConfig = quotaConfig;
    _taskConfig = taskConfig;
    _routingConfig = routingConfig;
  }

  // For backward compatible
  @Deprecated
  @Nonnull
  public static TableConfig init(@Nonnull String jsonConfigString)
      throws IOException {
    return fromJsonString(jsonConfigString);
  }

  public static TableConfig fromJsonString(String jsonString) throws IOException {
    return fromJSONConfig(JsonUtils.stringToJsonNode(jsonString));
  }

  @Nonnull
  public static TableConfig fromJSONConfig(@Nonnull JsonNode jsonConfig) throws IOException {
    TableType tableType = TableType.valueOf(jsonConfig.get(TABLE_TYPE_KEY).asText().toUpperCase());
    String tableName = TableNameBuilder.forType(tableType).tableNameWithType(jsonConfig.get(TABLE_NAME_KEY).asText());

    SegmentsValidationAndRetentionConfig validationConfig =
        extractChildConfig(jsonConfig, VALIDATION_CONFIG_KEY, SegmentsValidationAndRetentionConfig.class);
    TenantConfig tenantConfig = extractChildConfig(jsonConfig, TENANT_CONFIG_KEY, TenantConfig.class);
    IndexingConfig indexingConfig = extractChildConfig(jsonConfig, INDEXING_CONFIG_KEY, IndexingConfig.class);
    TableCustomConfig customConfig = extractChildConfig(jsonConfig, CUSTOM_CONFIG_KEY, TableCustomConfig.class);
    QuotaConfig quotaConfig = null;
    if (jsonConfig.has(QUOTA_CONFIG_KEY)) {
      quotaConfig = extractChildConfig(jsonConfig, QUOTA_CONFIG_KEY, QuotaConfig.class);
      quotaConfig.validate();
    }
    TableTaskConfig taskConfig = null;
    if (jsonConfig.has(TASK_CONFIG_KEY)) {
      taskConfig = extractChildConfig(jsonConfig, TASK_CONFIG_KEY, TableTaskConfig.class);
    }
    RoutingConfig routingConfig = null;
    if (jsonConfig.has(ROUTING_CONFIG_KEY)) {
      routingConfig = extractChildConfig(jsonConfig, ROUTING_CONFIG_KEY, RoutingConfig.class);
    }

    return new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
        quotaConfig, taskConfig, routingConfig);
  }

  /**
   * Extracts the child config from the table config.
   * <p>
   * NOTE: for historical reason, we support two kinds of nested config values: normal json and serialized json string
   */
  private static <T> T extractChildConfig(JsonNode jsonConfig, String childConfigKey, Class<T> childConfigClass)
      throws IOException {
    JsonNode childConfigNode = jsonConfig.get(childConfigKey);
    if (childConfigNode.isObject()) {
      return JsonUtils.jsonNodeToObject(childConfigNode, childConfigClass);
    } else {
      return JsonUtils.stringToObject(childConfigNode.asText(), childConfigClass);
    }
  }

  @Nonnull
  public static JsonNode toJSONConfig(@Nonnull TableConfig tableConfig) {
    ObjectNode jsonConfig = JsonUtils.newObjectNode();
    jsonConfig.put(TABLE_NAME_KEY, tableConfig._tableName);
    jsonConfig.put(TABLE_TYPE_KEY, tableConfig._tableType.toString());
    jsonConfig.set(VALIDATION_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._validationConfig));
    jsonConfig.set(TENANT_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._tenantConfig));
    jsonConfig.set(INDEXING_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._indexingConfig));
    jsonConfig.set(CUSTOM_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._customConfig));
    if (tableConfig._quotaConfig != null) {
      jsonConfig.set(QUOTA_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._quotaConfig));
    }
    if (tableConfig._taskConfig != null) {
      jsonConfig.set(TASK_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._taskConfig));
    }
    if (tableConfig._routingConfig != null) {
      jsonConfig.set(ROUTING_CONFIG_KEY, JsonUtils.objectToJsonNode(tableConfig._routingConfig));
    }
    return jsonConfig;
  }

  @Nonnull
  public static TableConfig fromZnRecord(@Nonnull ZNRecord znRecord) throws IOException {
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    TableType tableType = TableType.valueOf(simpleFields.get(TABLE_TYPE_KEY).toUpperCase());
    String tableName = TableNameBuilder.forType(tableType).tableNameWithType(simpleFields.get(TABLE_NAME_KEY));
    SegmentsValidationAndRetentionConfig validationConfig =
        JsonUtils.stringToObject(simpleFields.get(VALIDATION_CONFIG_KEY), SegmentsValidationAndRetentionConfig.class);
    TenantConfig tenantConfig = JsonUtils.stringToObject(simpleFields.get(TENANT_CONFIG_KEY), TenantConfig.class);
    IndexingConfig indexingConfig =
        JsonUtils.stringToObject(simpleFields.get(INDEXING_CONFIG_KEY), IndexingConfig.class);
    TableCustomConfig customConfig =
        JsonUtils.stringToObject(simpleFields.get(CUSTOM_CONFIG_KEY), TableCustomConfig.class);
    QuotaConfig quotaConfig = null;
    String quotaConfigString = simpleFields.get(QUOTA_CONFIG_KEY);
    if (quotaConfigString != null) {
      quotaConfig = JsonUtils.stringToObject(quotaConfigString, QuotaConfig.class);
      quotaConfig.validate();
    }
    TableTaskConfig taskConfig = null;
    String taskConfigString = simpleFields.get(TASK_CONFIG_KEY);
    if (taskConfigString != null) {
      taskConfig = JsonUtils.stringToObject(taskConfigString, TableTaskConfig.class);
    }
    String routingConfigString = simpleFields.get(ROUTING_CONFIG_KEY);

    RoutingConfig routingConfig = null;
    if (routingConfigString != null) {
      routingConfig = JsonUtils.stringToObject(routingConfigString, RoutingConfig.class);
    }

    return new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
        quotaConfig, taskConfig, routingConfig);
  }

  @Nonnull
  public static ZNRecord toZnRecord(@Nonnull TableConfig tableConfig) {
    ZNRecord znRecord = new ZNRecord(tableConfig.getTableName());
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(TABLE_NAME_KEY, tableConfig._tableName);
    simpleFields.put(TABLE_TYPE_KEY, tableConfig._tableType.toString());
    try {
      simpleFields.put(VALIDATION_CONFIG_KEY, JsonUtils.objectToString(tableConfig._validationConfig));
      simpleFields.put(TENANT_CONFIG_KEY, JsonUtils.objectToString(tableConfig._tenantConfig));
      simpleFields.put(INDEXING_CONFIG_KEY, JsonUtils.objectToString(tableConfig._indexingConfig));
      simpleFields.put(CUSTOM_CONFIG_KEY, JsonUtils.objectToString(tableConfig._customConfig));
      if (tableConfig._quotaConfig != null) {
        simpleFields.put(QUOTA_CONFIG_KEY, JsonUtils.objectToString(tableConfig._quotaConfig));
      }
      if (tableConfig._taskConfig != null) {
        simpleFields.put(TASK_CONFIG_KEY, JsonUtils.objectToString(tableConfig._taskConfig));
      }
      if (tableConfig._routingConfig != null) {
        simpleFields.put(ROUTING_CONFIG_KEY, JsonUtils.objectToString(tableConfig._routingConfig));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    znRecord.setSimpleFields(simpleFields);
    return znRecord;
  }

  @Nonnull
  public String getTableName() {
    return _tableName;
  }

  public void setTableName(@Nonnull String tableName) {
    _tableName = tableName;
  }

  @Nonnull
  public TableType getTableType() {
    return _tableType;
  }

  public void setTableType(@Nonnull TableType tableType) {
    _tableType = tableType;
  }

  @Nonnull
  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return _validationConfig;
  }

  public void setValidationConfig(@Nonnull SegmentsValidationAndRetentionConfig validationConfig) {
    _validationConfig = validationConfig;
  }

  @Nonnull
  public TenantConfig getTenantConfig() {
    return _tenantConfig;
  }

  public void setTenantConfig(@Nonnull TenantConfig tenantConfig) {
    _tenantConfig = tenantConfig;
  }

  @Nonnull
  public IndexingConfig getIndexingConfig() {
    return _indexingConfig;
  }

  public void setIndexingConfig(@Nonnull IndexingConfig indexingConfig) {
    _indexingConfig = indexingConfig;
  }

  @Nonnull
  public TableCustomConfig getCustomConfig() {
    return _customConfig;
  }

  public void setCustomConfig(@Nonnull TableCustomConfig customConfig) {
    _customConfig = customConfig;
  }

  @Nullable
  public QuotaConfig getQuotaConfig() {
    return _quotaConfig;
  }

  public void setQuotaConfig(@Nullable QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
  }

  @Nullable
  public TableTaskConfig getTaskConfig() {
    return _taskConfig;
  }

  public void setTaskConfig(@Nullable TableTaskConfig taskConfig) {
    _taskConfig = taskConfig;
  }

  @Nullable
  public RoutingConfig getRoutingConfig() {
    return _routingConfig;
  }

  public void setRoutingConfig(RoutingConfig routingConfig) {
    _routingConfig = routingConfig;
  }

  @Nonnull
  public String toJSONConfigString() throws IOException {
    return toJSONConfig(this).toString();
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.objectToPrettyString(toJSONConfig(this));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TableConfig) {
      TableConfig that = (TableConfig) obj;
      return EqualityUtils.isEqual(_tableName, that._tableName)
          && EqualityUtils.isEqual(_tableType, that._tableType)
          && EqualityUtils.isEqual(_validationConfig, that._validationConfig)
          && EqualityUtils.isEqual(_tenantConfig, that._tenantConfig)
          && EqualityUtils.isEqual(_indexingConfig, that._indexingConfig)
          && EqualityUtils.isEqual(_customConfig, that._customConfig)
          && EqualityUtils.isEqual(_quotaConfig, that._quotaConfig)
          && EqualityUtils.isEqual(_taskConfig, that._taskConfig)
          && EqualityUtils.isEqual(_routingConfig, that._routingConfig);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_tableName);
    result = EqualityUtils.hashCodeOf(result, _tableType);
    result = EqualityUtils.hashCodeOf(result, _validationConfig);
    result = EqualityUtils.hashCodeOf(result, _tenantConfig);
    result = EqualityUtils.hashCodeOf(result, _indexingConfig);
    result = EqualityUtils.hashCodeOf(result, _customConfig);
    result = EqualityUtils.hashCodeOf(result, _quotaConfig);
    result = EqualityUtils.hashCodeOf(result, _taskConfig);
    result = EqualityUtils.hashCodeOf(result, _routingConfig);
    return result;
  }

  public static class Builder {
    private static final String DEFAULT_SEGMENT_PUSH_TYPE = "APPEND";
    private static final String REFRESH_SEGMENT_PUSH_TYPE = "REFRESH";
    private static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
    private static final String DEFAULT_STREAM_PARTITION_ASSIGNMENT_STRATEGY = "UniformStreamPartitionAssignment";
    private static final String DEFAULT_NUM_REPLICAS = "1";
    private static final String DEFAULT_LOAD_MODE = "HEAP";
    private static final String MMAP_LOAD_MODE = "MMAP";

    private final TableType _tableType;
    private String _tableName;
    private boolean _isLLC;

    // Validation config related
    private String _timeColumnName;
    private String _timeType;
    private String _retentionTimeUnit;
    private String _retentionTimeValue;
    private String _segmentPushFrequency;
    private String _segmentPushType = DEFAULT_SEGMENT_PUSH_TYPE;
    private String _segmentAssignmentStrategy = DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY;
    private String _schemaName;
    private String _numReplicas = DEFAULT_NUM_REPLICAS;

    // Tenant config related
    private String _brokerTenant;
    private String _serverTenant;
    private TagOverrideConfig _tagOverrideConfig;

    // Indexing config related
    private String _loadMode = DEFAULT_LOAD_MODE;
    private String _segmentVersion;
    private String _sortedColumn;
    private List<String> _invertedIndexColumns;
    private List<String> _noDictionaryColumns;
    private List<String> _onHeapDictionaryColumns;
    private List<String> _bloomFilterColumns;
    private Map<String, String> _streamConfigs;
    private String _streamPartitionAssignmentStrategy = DEFAULT_STREAM_PARTITION_ASSIGNMENT_STRATEGY;

    private TableCustomConfig _customConfig;
    private QuotaConfig _quotaConfig;
    private TableTaskConfig _taskConfig;
    private RoutingConfig _routingConfig;
    private HllConfig _hllConfig;
    private StarTreeIndexSpec _starTreeIndexSpec;

    public Builder(TableType tableType) {
      _tableType = tableType;
    }

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setLLC(boolean isLLC) {
      Preconditions.checkState(_tableType == TableType.REALTIME);
      _isLLC = isLLC;
      return this;
    }

    public Builder setTimeColumnName(String timeColumnName) {
      _timeColumnName = timeColumnName;
      return this;
    }

    public Builder setTimeType(String timeType) {
      _timeType = timeType;
      return this;
    }

    public Builder setRetentionTimeUnit(String retentionTimeUnit) {
      _retentionTimeUnit = retentionTimeUnit;
      return this;
    }

    public Builder setRetentionTimeValue(String retentionTimeValue) {
      _retentionTimeValue = retentionTimeValue;
      return this;
    }

    public Builder setSegmentPushType(String segmentPushType) {
      if (REFRESH_SEGMENT_PUSH_TYPE.equalsIgnoreCase(segmentPushType)) {
        _segmentPushType = REFRESH_SEGMENT_PUSH_TYPE;
      } else {
        _segmentPushType = DEFAULT_SEGMENT_PUSH_TYPE;
      }
      return this;
    }

    public Builder setSegmentPushFrequency(String segmentPushFrequency) {
      _segmentPushFrequency = segmentPushFrequency;
      return this;
    }

    public Builder setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
      _segmentAssignmentStrategy = segmentAssignmentStrategy;
      return this;
    }

    public Builder setSchemaName(String schemaName) {
      _schemaName = schemaName;
      return this;
    }

    public Builder setNumReplicas(int numReplicas) {
      Preconditions.checkArgument(numReplicas > 0);
      _numReplicas = String.valueOf(numReplicas);
      return this;
    }

    public Builder setBrokerTenant(String brokerTenant) {
      _brokerTenant = brokerTenant;
      return this;
    }

    public Builder setServerTenant(String serverTenant) {
      _serverTenant = serverTenant;
      return this;
    }

    public Builder setTagOverrideConfig(TagOverrideConfig tagOverrideConfig) {
      _tagOverrideConfig = tagOverrideConfig;
      return this;
    }

    public Builder setLoadMode(String loadMode) {
      if (MMAP_LOAD_MODE.equalsIgnoreCase(loadMode)) {
        _loadMode = MMAP_LOAD_MODE;
      } else {
        _loadMode = DEFAULT_LOAD_MODE;
      }
      return this;
    }

    public Builder setSegmentVersion(String segmentVersion) {
      _segmentVersion = segmentVersion;
      return this;
    }

    public Builder setSortedColumn(String sortedColumn) {
      _sortedColumn = sortedColumn;
      return this;
    }

    public Builder setInvertedIndexColumns(List<String> invertedIndexColumns) {
      _invertedIndexColumns = invertedIndexColumns;
      return this;
    }

    public Builder setBloomFilterColumns(List<String> bloomFilterColumns) {
      _bloomFilterColumns = bloomFilterColumns;
      return this;
    }

    public Builder setNoDictionaryColumns(List<String> noDictionaryColumns) {
      _noDictionaryColumns = noDictionaryColumns;
      return this;
    }

    public Builder setOnHeapDictionaryColumns(List<String> onHeapDictionaryColumns) {
      _onHeapDictionaryColumns = onHeapDictionaryColumns;
      return this;
    }

    public Builder setStreamPartitionAssignmentStrategy(String streamPartitionAssignmentStrategy) {
      _streamPartitionAssignmentStrategy = streamPartitionAssignmentStrategy;
      return this;
    }

    public Builder setStreamConfigs(Map<String, String> streamConfigs) {
      Preconditions.checkState(_tableType == TableType.REALTIME);
      _streamConfigs = streamConfigs;
      return this;
    }

    public Builder setCustomConfig(TableCustomConfig customConfig) {
      _customConfig = customConfig;
      return this;
    }

    public Builder setQuotaConfig(QuotaConfig quotaConfig) {
      _quotaConfig = quotaConfig;
      return this;
    }

    public Builder setTaskConfig(TableTaskConfig taskConfig) {
      _taskConfig = taskConfig;
      return this;
    }

    public Builder setRoutingConfig(RoutingConfig routingConfig) {
      _routingConfig = routingConfig;
      return this;
    }

    public TableConfig build() {
      // Validation config
      SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
      validationConfig.setTimeColumnName(_timeColumnName);
      validationConfig.setTimeType(_timeType);
      validationConfig.setRetentionTimeUnit(_retentionTimeUnit);
      validationConfig.setRetentionTimeValue(_retentionTimeValue);
      validationConfig.setSegmentPushFrequency(_segmentPushFrequency);
      validationConfig.setSegmentPushType(_segmentPushType);
      validationConfig.setSegmentAssignmentStrategy(_segmentAssignmentStrategy);
      validationConfig.setSchemaName(_schemaName);
      validationConfig.setReplication(_numReplicas);
      if (_isLLC) {
        validationConfig.setReplicasPerPartition(_numReplicas);
      }

      // Tenant config
      TenantConfig tenantConfig = new TenantConfig();
      tenantConfig.setBroker(_brokerTenant);
      tenantConfig.setServer(_serverTenant);
      tenantConfig.setTagOverrideConfig(_tagOverrideConfig);

      // Indexing config
      IndexingConfig indexingConfig = new IndexingConfig();
      indexingConfig.setLoadMode(_loadMode);
      indexingConfig.setSegmentFormatVersion(_segmentVersion);
      if (_sortedColumn != null) {
        indexingConfig.setSortedColumn(Collections.singletonList(_sortedColumn));
      }
      indexingConfig.setInvertedIndexColumns(_invertedIndexColumns);
      indexingConfig.setNoDictionaryColumns(_noDictionaryColumns);
      indexingConfig.setOnHeapDictionaryColumns(_onHeapDictionaryColumns);
      indexingConfig.setStreamConfigs(_streamConfigs);
      indexingConfig.setBloomFilterColumns(_bloomFilterColumns);
      StreamConsumptionConfig streamConsumptionConfig = new StreamConsumptionConfig();
      streamConsumptionConfig.setStreamPartitionAssignmentStrategy(_streamPartitionAssignmentStrategy);
      indexingConfig.setStreamConsumptionConfig(streamConsumptionConfig);
      // TODO: set SegmentPartitionConfig here

      if (_customConfig == null) {
        _customConfig = new TableCustomConfig();
        _customConfig.setCustomConfigs(new HashMap<>());
      }

      return new TableConfig(_tableName, _tableType, validationConfig, tenantConfig, indexingConfig, _customConfig,
          _quotaConfig, _taskConfig, _routingConfig);
    }
  }
}
