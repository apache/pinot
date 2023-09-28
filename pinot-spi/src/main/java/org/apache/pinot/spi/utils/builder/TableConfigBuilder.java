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
package org.apache.pinot.spi.utils.builder;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;


public class TableConfigBuilder {
  private static final String DEFAULT_SEGMENT_PUSH_TYPE = "APPEND";
  private static final String REFRESH_SEGMENT_PUSH_TYPE = "REFRESH";
  private static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
  private static final String DEFAULT_DELETED_SEGMENTS_RETENTION_PERIOD = "7d";
  private static final String DEFAULT_NUM_REPLICAS = "1";
  private static final String DEFAULT_LOAD_MODE = "HEAP";
  private static final String MMAP_LOAD_MODE = "MMAP";

  private final TableType _tableType;
  private String _tableName;
  private boolean _isDimTable;

  // Segments config related
  private String _schemaName;
  private String _numReplicas = DEFAULT_NUM_REPLICAS;
  private String _timeColumnName;
  private String _timeType;
  private String _retentionTimeUnit;
  private String _retentionTimeValue;
  private String _deletedSegmentsRetentionPeriod = DEFAULT_DELETED_SEGMENTS_RETENTION_PERIOD;
  @Deprecated
  private String _segmentPushFrequency;
  @Deprecated
  private String _segmentPushType = DEFAULT_SEGMENT_PUSH_TYPE;
  private String _segmentAssignmentStrategy = DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY;
  private String _peerSegmentDownloadScheme;
  private ReplicaGroupStrategyConfig _replicaGroupStrategyConfig;
  private CompletionConfig _completionConfig;
  private String _crypterClassName;

  // Tenant config related
  private String _brokerTenant;
  private String _serverTenant;
  private TagOverrideConfig _tagOverrideConfig;

  // Indexing config related
  private String _loadMode = DEFAULT_LOAD_MODE;
  private String _segmentVersion;
  private String _sortedColumn;
  private List<String> _invertedIndexColumns;
  private boolean _createInvertedIndexDuringSegmentGeneration;
  private List<String> _noDictionaryColumns;
  private List<String> _onHeapDictionaryColumns;
  private List<String> _bloomFilterColumns;
  private List<String> _rangeIndexColumns;
  private Map<String, String> _streamConfigs;
  private SegmentPartitionConfig _segmentPartitionConfig;
  private boolean _nullHandlingEnabled;
  private List<String> _varLengthDictionaryColumns;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private List<String> _jsonIndexColumns;
  private List<String> _vectorIndexColumns;
  private boolean _aggregateMetrics;
  private boolean _optimizeDictionaryForMetrics;
  // This threshold determines if dictionary should be enabled or not for a metric column and is relevant
  // only when _optimizeDictionaryForMetrics is set to true.
  private double _noDictionarySizeRatioThreshold;

  private TableCustomConfig _customConfig;
  private QuotaConfig _quotaConfig;
  private TableTaskConfig _taskConfig;
  private RoutingConfig _routingConfig;
  private QueryConfig _queryConfig;
  private Map<String, InstanceAssignmentConfig> _instanceAssignmentConfigMap;
  private Map<InstancePartitionsType, String> _instancePartitionsMap;
  private Map<String, SegmentAssignmentConfig> _segmentAssignmentConfigMap;
  private List<FieldConfig> _fieldConfigList;

  private UpsertConfig _upsertConfig;
  private DedupConfig _dedupConfig;
  private DimensionTableConfig _dimensionTableConfig;
  private IngestionConfig _ingestionConfig;
  private List<TierConfig> _tierConfigList;
  private List<TunerConfig> _tunerConfigList;
  private JsonNode _tierOverwrites;

  public TableConfigBuilder(TableType tableType) {
    _tableType = tableType;
  }

  public TableConfigBuilder setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public TableConfigBuilder setIsDimTable(boolean isDimTable) {
    _isDimTable = isDimTable;
    return this;
  }

  @Deprecated
  public TableConfigBuilder setLLC(boolean isLLC) {
    Preconditions.checkState(_tableType == TableType.REALTIME);
    Preconditions.checkArgument(isLLC, "Real-time table must use LLC");
    return this;
  }

  public TableConfigBuilder setSchemaName(String schemaName) {
    _schemaName = schemaName;
    return this;
  }

  public TableConfigBuilder setNumReplicas(int numReplicas) {
    Preconditions.checkArgument(numReplicas > 0);
    _numReplicas = String.valueOf(numReplicas);
    return this;
  }

  public TableConfigBuilder setTimeColumnName(String timeColumnName) {
    _timeColumnName = timeColumnName;
    return this;
  }

  public TableConfigBuilder setTimeType(String timeType) {
    _timeType = timeType;
    return this;
  }

  public TableConfigBuilder setRetentionTimeUnit(String retentionTimeUnit) {
    _retentionTimeUnit = retentionTimeUnit;
    return this;
  }

  public TableConfigBuilder setRetentionTimeValue(String retentionTimeValue) {
    _retentionTimeValue = retentionTimeValue;
    return this;
  }

  public TableConfigBuilder setDeletedSegmentsRetentionPeriod(String deletedSegmentsRetentionPeriod) {
    _deletedSegmentsRetentionPeriod = deletedSegmentsRetentionPeriod;
    return this;
  }

  /**
   * @deprecated Use {@code segmentIngestionType} from {@link IngestionConfig#getBatchIngestionConfig()}
   */
  public TableConfigBuilder setSegmentPushType(String segmentPushType) {
    if (REFRESH_SEGMENT_PUSH_TYPE.equalsIgnoreCase(segmentPushType)) {
      _segmentPushType = REFRESH_SEGMENT_PUSH_TYPE;
    } else {
      _segmentPushType = DEFAULT_SEGMENT_PUSH_TYPE;
    }
    return this;
  }

  /**
   * @deprecated Use {@code segmentIngestionFrequency} from {@link IngestionConfig#getBatchIngestionConfig()}
   */
  public TableConfigBuilder setSegmentPushFrequency(String segmentPushFrequency) {
    _segmentPushFrequency = segmentPushFrequency;
    return this;
  }

  public TableConfigBuilder setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
    _segmentAssignmentStrategy = segmentAssignmentStrategy;
    return this;
  }

  public TableConfigBuilder setReplicaGroupStrategyConfig(ReplicaGroupStrategyConfig replicaGroupStrategyConfig) {
    _replicaGroupStrategyConfig = replicaGroupStrategyConfig;
    return this;
  }

  public TableConfigBuilder setCompletionConfig(CompletionConfig completionConfig) {
    _completionConfig = completionConfig;
    return this;
  }

  public TableConfigBuilder setCrypterClassName(String crypterClassName) {
    _crypterClassName = crypterClassName;
    return this;
  }

  public TableConfigBuilder setBrokerTenant(String brokerTenant) {
    _brokerTenant = brokerTenant;
    return this;
  }

  public TableConfigBuilder setServerTenant(String serverTenant) {
    _serverTenant = serverTenant;
    return this;
  }

  public TableConfigBuilder setTagOverrideConfig(TagOverrideConfig tagOverrideConfig) {
    _tagOverrideConfig = tagOverrideConfig;
    return this;
  }

  public TableConfigBuilder setLoadMode(String loadMode) {
    if (MMAP_LOAD_MODE.equalsIgnoreCase(loadMode)) {
      _loadMode = MMAP_LOAD_MODE;
    } else {
      _loadMode = DEFAULT_LOAD_MODE;
    }
    return this;
  }

  public TableConfigBuilder setSegmentVersion(String segmentVersion) {
    _segmentVersion = segmentVersion;
    return this;
  }

  public TableConfigBuilder setSortedColumn(String sortedColumn) {
    _sortedColumn = sortedColumn;
    return this;
  }

  public TableConfigBuilder setInvertedIndexColumns(List<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
    return this;
  }

  public TableConfigBuilder setOptimizeDictionaryForMetrics(boolean optimizeDictionaryForMetrics) {
    _optimizeDictionaryForMetrics = optimizeDictionaryForMetrics;
    return this;
  }

  public TableConfigBuilder setNoDictionarySizeRatioThreshold(double noDictionarySizeRatioThreshold) {
    _noDictionarySizeRatioThreshold = noDictionarySizeRatioThreshold;
    return this;
  }

  public TableConfigBuilder setCreateInvertedIndexDuringSegmentGeneration(
      boolean createInvertedIndexDuringSegmentGeneration) {
    _createInvertedIndexDuringSegmentGeneration = createInvertedIndexDuringSegmentGeneration;
    return this;
  }

  public TableConfigBuilder setNoDictionaryColumns(List<String> noDictionaryColumns) {
    _noDictionaryColumns = noDictionaryColumns;
    return this;
  }

  public TableConfigBuilder setOnHeapDictionaryColumns(List<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = onHeapDictionaryColumns;
    return this;
  }

  public TableConfigBuilder setBloomFilterColumns(List<String> bloomFilterColumns) {
    _bloomFilterColumns = bloomFilterColumns;
    return this;
  }

  public TableConfigBuilder setRangeIndexColumns(List<String> rangeIndexColumns) {
    _rangeIndexColumns = rangeIndexColumns;
    return this;
  }

  public TableConfigBuilder setVarLengthDictionaryColumns(List<String> varLengthDictionaryColumns) {
    _varLengthDictionaryColumns = varLengthDictionaryColumns;
    return this;
  }

  public TableConfigBuilder setStarTreeIndexConfigs(List<StarTreeIndexConfig> starTreeIndexConfigs) {
    _starTreeIndexConfigs = starTreeIndexConfigs;
    return this;
  }

  public TableConfigBuilder setJsonIndexColumns(List<String> jsonIndexColumns) {
    _jsonIndexColumns = jsonIndexColumns;
    return this;
  }

  public TableConfigBuilder setVectorIndexColumns(List<String> vectorIndexColumns) {
    _vectorIndexColumns = vectorIndexColumns;
    return this;
  }


  public TableConfigBuilder setAggregateMetrics(boolean aggregateMetrics) {
    _aggregateMetrics = aggregateMetrics;
    return this;
  }

  public TableConfigBuilder setStreamConfigs(Map<String, String> streamConfigs) {
    Preconditions.checkState(_tableType == TableType.REALTIME);
    _streamConfigs = streamConfigs;
    return this;
  }

  public TableConfigBuilder setSegmentPartitionConfig(SegmentPartitionConfig segmentPartitionConfig) {
    _segmentPartitionConfig = segmentPartitionConfig;
    return this;
  }

  public TableConfigBuilder setNullHandlingEnabled(boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
    return this;
  }

  public TableConfigBuilder setCustomConfig(TableCustomConfig customConfig) {
    _customConfig = customConfig;
    return this;
  }

  public TableConfigBuilder setQuotaConfig(QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
    return this;
  }

  public TableConfigBuilder setTaskConfig(TableTaskConfig taskConfig) {
    _taskConfig = taskConfig;
    return this;
  }

  public TableConfigBuilder setRoutingConfig(RoutingConfig routingConfig) {
    _routingConfig = routingConfig;
    return this;
  }

  public TableConfigBuilder setQueryConfig(QueryConfig queryConfig) {
    _queryConfig = queryConfig;
    return this;
  }

  public TableConfigBuilder setInstanceAssignmentConfigMap(
      Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap) {
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
    return this;
  }

  public TableConfigBuilder setFieldConfigList(List<FieldConfig> fieldConfigList) {
    _fieldConfigList = fieldConfigList;
    return this;
  }

  public TableConfigBuilder setUpsertConfig(UpsertConfig upsertConfig) {
    _upsertConfig = upsertConfig;
    return this;
  }

  public TableConfigBuilder setDedupConfig(DedupConfig dedupConfig) {
    _dedupConfig = dedupConfig;
    return this;
  }

  public TableConfigBuilder setDimensionTableConfig(DimensionTableConfig dimensionTableConfig) {
    _dimensionTableConfig = dimensionTableConfig;
    return this;
  }

  public TableConfigBuilder setPeerSegmentDownloadScheme(String peerSegmentDownloadScheme) {
    _peerSegmentDownloadScheme = peerSegmentDownloadScheme;
    return this;
  }

  public TableConfigBuilder setIngestionConfig(IngestionConfig ingestionConfig) {
    _ingestionConfig = ingestionConfig;
    return this;
  }

  public TableConfigBuilder setTierConfigList(List<TierConfig> tierConfigList) {
    _tierConfigList = tierConfigList;
    return this;
  }

  public TableConfigBuilder setTunerConfigList(List<TunerConfig> tunerConfigList) {
    _tunerConfigList = tunerConfigList;
    return this;
  }

  public TableConfigBuilder setInstancePartitionsMap(Map<InstancePartitionsType, String> instancePartitionsMap) {
    _instancePartitionsMap = instancePartitionsMap;
    return this;
  }

  public TableConfigBuilder setSegmentAssignmentConfigMap(
      Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap) {
    _segmentAssignmentConfigMap = segmentAssignmentConfigMap;
    return this;
  }

  public TableConfigBuilder setTierOverwrites(JsonNode tierOverwrites) {
    _tierOverwrites = tierOverwrites;
    return this;
  }

  public TableConfig build() {
    // Validation config
    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    validationConfig.setTimeColumnName(_timeColumnName);
    validationConfig.setTimeType(_timeType);
    validationConfig.setRetentionTimeUnit(_retentionTimeUnit);
    validationConfig.setRetentionTimeValue(_retentionTimeValue);
    validationConfig.setDeletedSegmentsRetentionPeriod(_deletedSegmentsRetentionPeriod);
    validationConfig.setSegmentPushFrequency(_segmentPushFrequency);
    validationConfig.setSegmentPushType(_segmentPushType);
    validationConfig.setSegmentAssignmentStrategy(_segmentAssignmentStrategy);
    validationConfig.setReplicaGroupStrategyConfig(_replicaGroupStrategyConfig);
    validationConfig.setCompletionConfig(_completionConfig);
    validationConfig.setSchemaName(_schemaName);
    validationConfig.setReplication(_numReplicas);
    validationConfig.setPeerSegmentDownloadScheme(_peerSegmentDownloadScheme);
    validationConfig.setCrypterClassName(_crypterClassName);

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
    indexingConfig.setCreateInvertedIndexDuringSegmentGeneration(_createInvertedIndexDuringSegmentGeneration);
    indexingConfig.setNoDictionaryColumns(_noDictionaryColumns);
    indexingConfig.setOnHeapDictionaryColumns(_onHeapDictionaryColumns);
    indexingConfig.setBloomFilterColumns(_bloomFilterColumns);
    indexingConfig.setRangeIndexColumns(_rangeIndexColumns);
    indexingConfig.setStreamConfigs(_streamConfigs);
    indexingConfig.setSegmentPartitionConfig(_segmentPartitionConfig);
    indexingConfig.setNullHandlingEnabled(_nullHandlingEnabled);
    indexingConfig.setVarLengthDictionaryColumns(_varLengthDictionaryColumns);
    indexingConfig.setStarTreeIndexConfigs(_starTreeIndexConfigs);
    indexingConfig.setJsonIndexColumns(_jsonIndexColumns);
    indexingConfig.setAggregateMetrics(_aggregateMetrics);
    indexingConfig.setOptimizeDictionaryForMetrics(_optimizeDictionaryForMetrics);
    indexingConfig.setNoDictionarySizeRatioThreshold(_noDictionarySizeRatioThreshold);
    indexingConfig.setTierOverwrites(_tierOverwrites);
    indexingConfig.setVectorIndexColumns(_vectorIndexColumns);

    if (_customConfig == null) {
      _customConfig = new TableCustomConfig(null);
    }

    return new TableConfig(_tableName, _tableType.toString(), validationConfig, tenantConfig, indexingConfig,
        _customConfig, _quotaConfig, _taskConfig, _routingConfig, _queryConfig, _instanceAssignmentConfigMap,
        _fieldConfigList, _upsertConfig, _dedupConfig, _dimensionTableConfig, _ingestionConfig, _tierConfigList,
        _isDimTable, _tunerConfigList, _instancePartitionsMap, _segmentAssignmentConfigMap);
  }
}
