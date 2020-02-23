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

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.Null;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitionsType;
import org.apache.pinot.common.config.instance.InstanceAssignmentConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.spi.utils.JsonUtils;


@SuppressWarnings({"Duplicates", "unused"})
public class TableConfig extends BaseJsonConfig {
  public static final String TABLE_NAME_KEY = "tableName";
  public static final String TABLE_TYPE_KEY = "tableType";
  public static final String VALIDATION_CONFIG_KEY = "segmentsConfig";
  public static final String TENANT_CONFIG_KEY = "tenants";
  public static final String INDEXING_CONFIG_KEY = "tableIndexConfig";
  public static final String CUSTOM_CONFIG_KEY = "metadata";
  public static final String QUOTA_CONFIG_KEY = "quota";
  public static final String TASK_CONFIG_KEY = "task";
  public static final String ROUTING_CONFIG_KEY = "routing";
  public static final String INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY = "instanceAssignmentConfigMap";
  public static final String FIELD_CONFIG_LIST_KEY = "fieldConfigList";

  private static final String FIELD_MISSING_MESSAGE_TEMPLATE = "Mandatory field '%s' is missing";

  @JsonPropertyDescription("The name for the table (with type suffix), e.g. \"myTable_OFFLINE\" (mandatory)")
  private String _tableName;

  @JsonPropertyDescription(value = "The type of the table (OFFLINE|REALTIME) (mandatory)")
  private TableType _tableType;

  private SegmentsValidationAndRetentionConfig _validationConfig;
  private TenantConfig _tenantConfig;
  private IndexingConfig _indexingConfig;
  private TableCustomConfig _customConfig;

  @JsonPropertyDescription("Resource quota associated with this table")
  private QuotaConfig _quotaConfig;

  private TableTaskConfig _taskConfig;
  private RoutingConfig _routingConfig;
  private Map<InstancePartitionsType, InstanceAssignmentConfig> _instanceAssignmentConfigMap;

  private List<FieldConfig> _fieldConfigList;

  /**
   * NOTE: DO NOT use this constructor, use builder instead. This constructor is for deserializer only.
   */
  public TableConfig() {
    // TODO: currently these 2 fields are annotated as non-null. Revisit to see whether that's necessary
    _tenantConfig = new TenantConfig(null, null, null);
    _customConfig = new TableCustomConfig(null);
  }

  private TableConfig(String tableName, TableType tableType, SegmentsValidationAndRetentionConfig validationConfig,
      TenantConfig tenantConfig, IndexingConfig indexingConfig, TableCustomConfig customConfig,
      @Nullable QuotaConfig quotaConfig, @Nullable TableTaskConfig taskConfig, @Nullable RoutingConfig routingConfig,
      @Nullable Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap,
      @Nullable List<FieldConfig> fieldConfigList) {
    _tableName = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    _tableType = tableType;
    _validationConfig = validationConfig;
    _tenantConfig = tenantConfig;
    _indexingConfig = indexingConfig;
    _customConfig = customConfig;
    _quotaConfig = quotaConfig;
    _taskConfig = taskConfig;
    _routingConfig = routingConfig;
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
    _fieldConfigList = fieldConfigList;
  }

  public static TableConfig fromJsonString(String jsonString)
      throws IOException {
    return fromJsonConfig(JsonUtils.stringToJsonNode(jsonString));
  }

  public static TableConfig fromJsonConfig(JsonNode jsonConfig)
      throws IOException {
    // Mandatory fields
    JsonNode jsonTableType = jsonConfig.get(TABLE_TYPE_KEY);
    Preconditions
        .checkState(jsonTableType != null && !jsonTableType.isNull(), FIELD_MISSING_MESSAGE_TEMPLATE, TABLE_TYPE_KEY);
    TableType tableType = TableType.valueOf(jsonTableType.asText().toUpperCase());

    JsonNode jsonTableName = jsonConfig.get(TABLE_NAME_KEY);
    Preconditions
        .checkState(jsonTableName != null && !jsonTableName.isNull(), FIELD_MISSING_MESSAGE_TEMPLATE, TABLE_NAME_KEY);
    String tableName = TableNameBuilder.forType(tableType).tableNameWithType(jsonTableName.asText());

    SegmentsValidationAndRetentionConfig validationConfig =
        extractChildConfig(jsonConfig, VALIDATION_CONFIG_KEY, SegmentsValidationAndRetentionConfig.class);
    Preconditions.checkState(validationConfig != null, FIELD_MISSING_MESSAGE_TEMPLATE, VALIDATION_CONFIG_KEY);

    TenantConfig tenantConfig = extractChildConfig(jsonConfig, TENANT_CONFIG_KEY, TenantConfig.class);
    Preconditions.checkState(tenantConfig != null, FIELD_MISSING_MESSAGE_TEMPLATE, TENANT_CONFIG_KEY);

    IndexingConfig indexingConfig = extractChildConfig(jsonConfig, INDEXING_CONFIG_KEY, IndexingConfig.class);
    Preconditions.checkState(indexingConfig != null, FIELD_MISSING_MESSAGE_TEMPLATE, INDEXING_CONFIG_KEY);

    TableCustomConfig customConfig = extractChildConfig(jsonConfig, CUSTOM_CONFIG_KEY, TableCustomConfig.class);
    Preconditions.checkState(customConfig != null, FIELD_MISSING_MESSAGE_TEMPLATE, CUSTOM_CONFIG_KEY);

    // Optional fields
    QuotaConfig quotaConfig = extractChildConfig(jsonConfig, QUOTA_CONFIG_KEY, QuotaConfig.class);
    if (quotaConfig != null) {
      quotaConfig.validate();
    }

    TableTaskConfig taskConfig = extractChildConfig(jsonConfig, TASK_CONFIG_KEY, TableTaskConfig.class);

    RoutingConfig routingConfig = extractChildConfig(jsonConfig, ROUTING_CONFIG_KEY, RoutingConfig.class);

    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        extractChildConfig(jsonConfig, INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY,
            new TypeReference<Map<InstancePartitionsType, InstanceAssignmentConfig>>() {
            });

    List<FieldConfig> fieldConfigList = extractChildConfig(jsonConfig, FIELD_CONFIG_LIST_KEY,
        new TypeReference<List<FieldConfig>>() {});

    return new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
        quotaConfig, taskConfig, routingConfig, instanceAssignmentConfigMap, fieldConfigList);
  }

  /**
   * Extracts the child config from the table config. Returns {@code null} if child config does not exist.
   * <p>
   * NOTE: For historical reason, we support two kinds of nested config values: normal json and serialized json string
   */
  @Nullable
  private static <T> T extractChildConfig(JsonNode jsonConfig, String childConfigKey, Class<T> childConfigClass)
      throws IOException {
    JsonNode childConfigNode = jsonConfig.get(childConfigKey);
    if (childConfigNode == null || childConfigNode.isNull()) {
      return null;
    }
    if (childConfigNode.isObject() || childConfigNode.isContainerNode()) {
      return JsonUtils.jsonNodeToObject(childConfigNode, childConfigClass);
    } else {
      return JsonUtils.stringToObject(childConfigNode.asText(), childConfigClass);
    }
  }

  /**
   * Extracts the child config from the table config. Returns {@code null} if child config does not exist.
   * <p>
   * NOTE: For historical reason, we support two kinds of nested config values: normal json and serialized json string
   */
  @Nullable
  private static <T> T extractChildConfig(JsonNode jsonConfig, String childConfigKey,
      TypeReference<T> childConfigTypeReference)
      throws IOException {
    JsonNode childConfigNode = jsonConfig.get(childConfigKey);
    if (childConfigNode == null || childConfigNode.isNull()) {
      return null;
    }
    if (childConfigNode.isObject() || childConfigNode.isContainerNode()) {
      return JsonUtils.jsonNodeToObject(childConfigNode, childConfigTypeReference);
    } else {
      return JsonUtils.stringToObject(childConfigNode.asText(), childConfigTypeReference);
    }
  }

  public ObjectNode toJsonConfig() {
    validate();

    ObjectNode jsonConfig = JsonUtils.newObjectNode();

    // Mandatory fields
    jsonConfig.put(TABLE_NAME_KEY, _tableName);
    jsonConfig.put(TABLE_TYPE_KEY, _tableType.toString());
    jsonConfig.set(VALIDATION_CONFIG_KEY, JsonUtils.objectToJsonNode(_validationConfig));
    jsonConfig.set(TENANT_CONFIG_KEY, JsonUtils.objectToJsonNode(_tenantConfig));
    jsonConfig.set(INDEXING_CONFIG_KEY, JsonUtils.objectToJsonNode(_indexingConfig));
    jsonConfig.set(CUSTOM_CONFIG_KEY, JsonUtils.objectToJsonNode(_customConfig));

    // Optional fields
    if (_quotaConfig != null) {
      jsonConfig.set(QUOTA_CONFIG_KEY, JsonUtils.objectToJsonNode(_quotaConfig));
    }
    if (_taskConfig != null) {
      jsonConfig.set(TASK_CONFIG_KEY, JsonUtils.objectToJsonNode(_taskConfig));
    }
    if (_routingConfig != null) {
      jsonConfig.set(ROUTING_CONFIG_KEY, JsonUtils.objectToJsonNode(_routingConfig));
    }
    if (_instanceAssignmentConfigMap != null) {
      jsonConfig.set(INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY, JsonUtils.objectToJsonNode(_instanceAssignmentConfigMap));
    }
    if (_fieldConfigList != null) {
      jsonConfig.put(FIELD_CONFIG_LIST_KEY, JsonUtils.objectToJsonNode(_fieldConfigList));
    }

    return jsonConfig;
  }

  @Override
  public JsonNode toJsonNode() {
    return toJsonConfig();
  }

  public String toJsonConfigString() {
    return toJsonConfig().toString();
  }

  public static TableConfig fromZnRecord(ZNRecord znRecord)
      throws IOException {
    Map<String, String> simpleFields = znRecord.getSimpleFields();

    // Mandatory fields
    String tableTypeString = simpleFields.get(TABLE_TYPE_KEY);
    Preconditions.checkState(tableTypeString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TABLE_TYPE_KEY);
    TableType tableType = TableType.valueOf(tableTypeString.toUpperCase());

    String tableNameString = simpleFields.get(TABLE_NAME_KEY);
    Preconditions.checkState(tableNameString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TABLE_NAME_KEY);
    String tableName = TableNameBuilder.forType(tableType).tableNameWithType(tableNameString);

    String validationConfigString = simpleFields.get(VALIDATION_CONFIG_KEY);
    Preconditions.checkState(validationConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, VALIDATION_CONFIG_KEY);
    SegmentsValidationAndRetentionConfig validationConfig =
        JsonUtils.stringToObject(validationConfigString, SegmentsValidationAndRetentionConfig.class);

    String tenantConfigString = simpleFields.get(TENANT_CONFIG_KEY);
    Preconditions.checkState(tenantConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TENANT_CONFIG_KEY);
    TenantConfig tenantConfig = JsonUtils.stringToObject(tenantConfigString, TenantConfig.class);

    String indexingConfigString = simpleFields.get(INDEXING_CONFIG_KEY);
    Preconditions.checkState(indexingConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, INDEXING_CONFIG_KEY);
    IndexingConfig indexingConfig = JsonUtils.stringToObject(indexingConfigString, IndexingConfig.class);

    String customConfigString = simpleFields.get(CUSTOM_CONFIG_KEY);
    Preconditions.checkState(customConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, CUSTOM_CONFIG_KEY);
    TableCustomConfig customConfig = JsonUtils.stringToObject(customConfigString, TableCustomConfig.class);

    // Optional fields
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

    RoutingConfig routingConfig = null;
    String routingConfigString = simpleFields.get(ROUTING_CONFIG_KEY);
    if (routingConfigString != null) {
      routingConfig = JsonUtils.stringToObject(routingConfigString, RoutingConfig.class);
    }

    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap = null;
    String instanceAssignmentConfigMapString = simpleFields.get(INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY);
    if (instanceAssignmentConfigMapString != null) {
      instanceAssignmentConfigMap = JsonUtils.stringToObject(instanceAssignmentConfigMapString,
          new TypeReference<Map<InstancePartitionsType, InstanceAssignmentConfig>>() {
          });
    }

    List<FieldConfig> fieldConfigList = null;
    String fieldConfigListString = simpleFields.get(FIELD_CONFIG_LIST_KEY);
    if (fieldConfigListString != null) {
      fieldConfigList = JsonUtils.stringToObject(fieldConfigListString, new TypeReference<List<FieldConfig>>() {});
    }

    return new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
        quotaConfig, taskConfig, routingConfig, instanceAssignmentConfigMap, fieldConfigList);
  }

  public ZNRecord toZNRecord()
      throws JsonProcessingException {
    validate();

    Map<String, String> simpleFields = new HashMap<>();

    // Mandatory fields
    simpleFields.put(TABLE_NAME_KEY, _tableName);
    simpleFields.put(TABLE_TYPE_KEY, _tableType.toString());
    simpleFields.put(VALIDATION_CONFIG_KEY, JsonUtils.objectToString(_validationConfig));
    simpleFields.put(TENANT_CONFIG_KEY, JsonUtils.objectToString(_tenantConfig));
    simpleFields.put(INDEXING_CONFIG_KEY, JsonUtils.objectToString(_indexingConfig));
    simpleFields.put(CUSTOM_CONFIG_KEY, JsonUtils.objectToString(_customConfig));

    // Optional fields
    if (_quotaConfig != null) {
      simpleFields.put(QUOTA_CONFIG_KEY, JsonUtils.objectToString(_quotaConfig));
    }
    if (_taskConfig != null) {
      simpleFields.put(TASK_CONFIG_KEY, JsonUtils.objectToString(_taskConfig));
    }
    if (_routingConfig != null) {
      simpleFields.put(ROUTING_CONFIG_KEY, JsonUtils.objectToString(_routingConfig));
    }
    if (_instanceAssignmentConfigMap != null) {
      simpleFields.put(INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY, JsonUtils.objectToString(_instanceAssignmentConfigMap));
    }
    if (_fieldConfigList != null) {
      simpleFields.put(FIELD_CONFIG_LIST_KEY, JsonUtils.objectToString(_fieldConfigList));
    }

    ZNRecord znRecord = new ZNRecord(_tableName);
    znRecord.setSimpleFields(simpleFields);
    return znRecord;
  }

  /**
   * Validates the table config.
   * TODO: revisit to see whether all the following fields are mandatory
   */
  public void validate() {
    Preconditions.checkState(_tableName != null, "Table name is missing");
    Preconditions.checkState(_tableType != null, "Table type is missing");
    Preconditions.checkState(_validationConfig != null, "Validation config is missing");
    Preconditions.checkState(_tenantConfig != null, "Tenant config is missing");
    Preconditions.checkState(_indexingConfig != null, "Indexing config is missing");
    Preconditions.checkState(_customConfig != null, "Custom config is missing");
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public void setTableType(TableType tableType) {
    _tableType = tableType;
  }

  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return _validationConfig;
  }

  public void setValidationConfig(SegmentsValidationAndRetentionConfig validationConfig) {
    _validationConfig = validationConfig;
  }

  public TenantConfig getTenantConfig() {
    return _tenantConfig;
  }

  public void setTenantConfig(TenantConfig tenantConfig) {
    _tenantConfig = tenantConfig;
  }

  public IndexingConfig getIndexingConfig() {
    return _indexingConfig;
  }

  public void setIndexingConfig(IndexingConfig indexingConfig) {
    _indexingConfig = indexingConfig;
  }

  public TableCustomConfig getCustomConfig() {
    return _customConfig;
  }

  public void setCustomConfig(TableCustomConfig customConfig) {
    _customConfig = customConfig;
  }

  @Nullable
  public QuotaConfig getQuotaConfig() {
    return _quotaConfig;
  }

  public void setQuotaConfig(QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
  }

  @Nullable
  public TableTaskConfig getTaskConfig() {
    return _taskConfig;
  }

  public void setTaskConfig(TableTaskConfig taskConfig) {
    _taskConfig = taskConfig;
  }

  @Nullable
  public RoutingConfig getRoutingConfig() {
    return _routingConfig;
  }

  public void setRoutingConfig(RoutingConfig routingConfig) {
    _routingConfig = routingConfig;
  }

  @Nullable
  public Map<InstancePartitionsType, InstanceAssignmentConfig> getInstanceAssignmentConfigMap() {
    return _instanceAssignmentConfigMap;
  }

  public void setInstanceAssignmentConfigMap(
      Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap) {
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
  }

  @Nullable
  public List<FieldConfig> getFieldConfigList() {
    return _fieldConfigList;
  }

  public static class Builder {
    private static final String DEFAULT_SEGMENT_PUSH_TYPE = "APPEND";
    private static final String REFRESH_SEGMENT_PUSH_TYPE = "REFRESH";
    private static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
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
    private SegmentPartitionConfig _segmentPartitionConfig;

    private TableCustomConfig _customConfig;
    private QuotaConfig _quotaConfig;
    private TableTaskConfig _taskConfig;
    private RoutingConfig _routingConfig;
    private Map<InstancePartitionsType, InstanceAssignmentConfig> _instanceAssignmentConfigMap;
    private List<FieldConfig> _fieldConfigList;

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

    public Builder setNoDictionaryColumns(List<String> noDictionaryColumns) {
      _noDictionaryColumns = noDictionaryColumns;
      return this;
    }

    public Builder setOnHeapDictionaryColumns(List<String> onHeapDictionaryColumns) {
      _onHeapDictionaryColumns = onHeapDictionaryColumns;
      return this;
    }

    public Builder setBloomFilterColumns(List<String> bloomFilterColumns) {
      _bloomFilterColumns = bloomFilterColumns;
      return this;
    }

    public Builder setStreamConfigs(Map<String, String> streamConfigs) {
      Preconditions.checkState(_tableType == TableType.REALTIME);
      _streamConfigs = streamConfigs;
      return this;
    }

    public Builder setSegmentPartitionConfig(SegmentPartitionConfig segmentPartitionConfig) {
      _segmentPartitionConfig = segmentPartitionConfig;
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

    public Builder setInstanceAssignmentConfigMap(
        Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap) {
      _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
      return this;
    }

    public Builder setFieldConfigList(List<FieldConfig> fieldConfigList) {
      _fieldConfigList = fieldConfigList;
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
      TenantConfig tenantConfig = new TenantConfig(_brokerTenant, _serverTenant, _tagOverrideConfig);

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
      indexingConfig.setBloomFilterColumns(_bloomFilterColumns);
      indexingConfig.setStreamConfigs(_streamConfigs);
      indexingConfig.setSegmentPartitionConfig(_segmentPartitionConfig);

      if (_customConfig == null) {
        _customConfig = new TableCustomConfig(null);
      }

      // eventually this validation will be generic but since we are initially
      // using FieldConfig only for text columns (and migrate to expand its usage
      // soon after), just validate the field config list from text index creation
      // perspective.
      TextIndexConfigValidator.validate(_fieldConfigList, _noDictionaryColumns);

      return new TableConfig(_tableName, _tableType, validationConfig, tenantConfig, indexingConfig, _customConfig,
          _quotaConfig, _taskConfig, _routingConfig, _instanceAssignmentConfigMap, _fieldConfigList);
    }
  }
}
