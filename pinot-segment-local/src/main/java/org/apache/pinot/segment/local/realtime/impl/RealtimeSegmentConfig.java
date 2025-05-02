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
package org.apache.pinot.segment.local.realtime.impl;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.Schema;


public class RealtimeSegmentConfig {
  private final String _tableNameWithType;
  private final String _segmentName;
  private final String _streamName;
  private final Schema _schema;
  private final String _timeColumnName;
  private final int _capacity;
  private final int _avgNumMultiValues;
  private final Map<String, FieldIndexConfigs> _indexConfigByCol;
  private final SegmentZKMetadata _segmentZKMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  @Nullable
  private final String _partitionColumn;
  @Nullable
  private final PartitionFunction _partitionFunction;
  private final int _partitionId;
  private final boolean _aggregateMetrics;
  @Nullable
  private final List<AggregationConfig> _ingestionAggregationConfigs;
  private final boolean _nullHandlingEnabled;
  @Nullable
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  @Nullable
  private final PartitionDedupMetadataManager _partitionDedupMetadataManager;
  private final String _consumerDir;
  private final TableConfig _tableConfig;
  private final String _instanceId;

  // TODO: Clean up this constructor. Most of these things can be extracted from tableConfig.

  /**
   * @param nullHandlingEnabled whether null handling is enabled by default. This value is only used if
   *                            {@link Schema#isEnableColumnBasedNullHandling()} is false.
   * @param tableConfig
   * @param instanceId
   */
  private RealtimeSegmentConfig(String tableNameWithType, String segmentName, String streamName, Schema schema,
      String timeColumnName, int capacity, int avgNumMultiValues, Map<String, FieldIndexConfigs> indexConfigByCol,
      SegmentZKMetadata segmentZKMetadata, boolean offHeap, PinotDataBufferMemoryManager memoryManager,
      RealtimeSegmentStatsHistory statsHistory, @Nullable String partitionColumn,
      @Nullable PartitionFunction partitionFunction, int partitionId, boolean aggregateMetrics,
      @Nullable List<AggregationConfig> ingestionAggregationConfigs, boolean nullHandlingEnabled,
      @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      @Nullable PartitionDedupMetadataManager partitionDedupMetadataManager, String consumerDir,
      TableConfig tableConfig, String instanceId) {
    _tableNameWithType = tableNameWithType;
    _segmentName = segmentName;
    _streamName = streamName;
    _schema = schema;
    _timeColumnName = timeColumnName;
    _capacity = capacity;
    _avgNumMultiValues = avgNumMultiValues;
    _indexConfigByCol = indexConfigByCol;
    _segmentZKMetadata = segmentZKMetadata;
    _offHeap = offHeap;
    _memoryManager = memoryManager;
    _statsHistory = statsHistory;
    _partitionColumn = partitionColumn;
    _partitionFunction = partitionFunction;
    _partitionId = partitionId;
    _aggregateMetrics = aggregateMetrics;
    _ingestionAggregationConfigs = ingestionAggregationConfigs;
    _tableConfig = tableConfig;
    _instanceId = instanceId;
    _nullHandlingEnabled = nullHandlingEnabled;
    _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
    _partitionDedupMetadataManager = partitionDedupMetadataManager;
    _consumerDir = consumerDir;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getStreamName() {
    return _streamName;
  }

  public Schema getSchema() {
    return _schema;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
  }

  public int getCapacity() {
    return _capacity;
  }

  public int getAvgNumMultiValues() {
    return _avgNumMultiValues;
  }

  public Map<String, FieldIndexConfigs> getIndexConfigByCol() {
    return _indexConfigByCol;
  }

  public SegmentZKMetadata getSegmentZKMetadata() {
    return _segmentZKMetadata;
  }

  public boolean isOffHeap() {
    return _offHeap;
  }

  public PinotDataBufferMemoryManager getMemoryManager() {
    return _memoryManager;
  }

  public RealtimeSegmentStatsHistory getStatsHistory() {
    return _statsHistory;
  }

  @Nullable
  public String getPartitionColumn() {
    return _partitionColumn;
  }

  @Nullable
  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  public int getPartitionId() {
    return _partitionId;
  }

  public boolean aggregateMetrics() {
    return _aggregateMetrics;
  }

  @Nullable
  public List<AggregationConfig> getIngestionAggregationConfigs() {
    return _ingestionAggregationConfigs;
  }

  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  @Nullable
  public PartitionUpsertMetadataManager getPartitionUpsertMetadataManager() {
    return _partitionUpsertMetadataManager;
  }

  @Nullable
  public PartitionDedupMetadataManager getPartitionDedupMetadataManager() {
    return _partitionDedupMetadataManager;
  }

  public String getConsumerDir() {
    return _consumerDir;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public static class Builder {
    private String _tableNameWithType;
    private String _segmentName;
    private String _streamName;
    private Schema _schema;
    private String _timeColumnName;
    private int _capacity;
    private int _avgNumMultiValues;
    private final Map<String, FieldIndexConfigs.Builder> _indexConfigByCol;
    private SegmentZKMetadata _segmentZKMetadata;
    private boolean _offHeap;
    private PinotDataBufferMemoryManager _memoryManager;
    private RealtimeSegmentStatsHistory _statsHistory;
    private String _partitionColumn;
    private PartitionFunction _partitionFunction;
    private int _partitionId;
    private boolean _aggregateMetrics;
    private List<AggregationConfig> _ingestionAggregationConfigs;
    /**
     * Whether null handling is enabled by default. This value is only used if
     * {@link Schema#isEnableColumnBasedNullHandling()} is false.
     */
    private boolean _defaultNullHandlingEnabled;
    private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
    private PartitionDedupMetadataManager _partitionDedupMetadataManager;
    private TableConfig _tableConfig;
    private String _instanceId;
    private String _consumerDir;

    public Builder() {
      _indexConfigByCol = new HashMap<>();
    }

    public Builder(IndexLoadingConfig indexLoadingConfig) {
      this(indexLoadingConfig.getFieldIndexConfigByColName(), indexLoadingConfig.getSortedColumns());
    }

    public Builder(TableConfig tableConfig, Schema schema) {
      this(FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema),
          tableConfig.getIndexingConfig().getSortedColumn());
    }

    public Builder(Map<String, FieldIndexConfigs> indexConfigsByColName, @Nullable List<String> sortedColumns) {
      _indexConfigByCol = Maps.newHashMapWithExpectedSize(indexConfigsByColName.size());
      for (Map.Entry<String, FieldIndexConfigs> entry : indexConfigsByColName.entrySet()) {
        _indexConfigByCol.put(entry.getKey(), new FieldIndexConfigs.Builder(entry.getValue()));
      }
      // Add inverted index to sorted column for 2 reasons:
      // 1. Since sorted index doesn't apply to mutable segment, add inverted index to get better performance
      // 2. When converting mutable segment to immutable segment, we use sorted column's inverted index to accelerate
      //    the index creation
      if (CollectionUtils.isNotEmpty(sortedColumns)) {
        String sortedColumn = sortedColumns.get(0);
        FieldIndexConfigs.Builder builder =
            _indexConfigByCol.computeIfAbsent(sortedColumn, k -> new FieldIndexConfigs.Builder());
        builder.add(StandardIndexes.inverted(), new IndexConfig(false));
      }
    }

    public Builder setTableNameWithType(String tableNameWithType) {
      _tableNameWithType = tableNameWithType;
      return this;
    }

    public Builder setSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder setStreamName(String streamName) {
      _streamName = streamName;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public Builder setTimeColumnName(String timeColumnName) {
      _timeColumnName = timeColumnName;
      return this;
    }

    public Builder setCapacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    public Builder setAvgNumMultiValues(int avgNumMultiValues) {
      _avgNumMultiValues = avgNumMultiValues;
      return this;
    }

    public <C extends IndexConfig> Builder setIndex(Iterable<String> columns, IndexType<C, ?, ?> indexType, C config) {
      for (String column : columns) {
        FieldIndexConfigs.Builder builder =
            _indexConfigByCol.computeIfAbsent(column, (k) -> new FieldIndexConfigs.Builder());
        builder.add(indexType, config);
      }
      return this;
    }

    public <C extends IndexConfig> Builder setIndex(String column, IndexType<C, ?, ?> indexType, C config) {
      FieldIndexConfigs.Builder builder =
          _indexConfigByCol.computeIfAbsent(column, (k) -> new FieldIndexConfigs.Builder());
      builder.add(indexType, config);
      return this;
    }

    public Builder setSegmentZKMetadata(SegmentZKMetadata segmentZKMetadata) {
      _segmentZKMetadata = segmentZKMetadata;
      return this;
    }

    public Builder setOffHeap(boolean offHeap) {
      _offHeap = offHeap;
      return this;
    }

    public Builder setMemoryManager(PinotDataBufferMemoryManager memoryManager) {
      _memoryManager = memoryManager;
      return this;
    }

    public Builder setStatsHistory(RealtimeSegmentStatsHistory statsHistory) {
      _statsHistory = statsHistory;
      return this;
    }

    public Builder setPartitionColumn(String partitionColumn) {
      _partitionColumn = partitionColumn;
      return this;
    }

    public Builder setPartitionFunction(PartitionFunction partitionFunction) {
      _partitionFunction = partitionFunction;
      return this;
    }

    public Builder setPartitionId(int partitionId) {
      _partitionId = partitionId;
      return this;
    }

    public Builder setAggregateMetrics(boolean aggregateMetrics) {
      _aggregateMetrics = aggregateMetrics;
      return this;
    }

    public Builder setIngestionAggregationConfigs(List<AggregationConfig> ingestionAggregationConfigs) {
      _ingestionAggregationConfigs = ingestionAggregationConfigs;
      return this;
    }

    /**
     * Whether null handling is enabled by default. This value is only used if
     * {@link Schema#isEnableColumnBasedNullHandling()} is false.
     */
    @Deprecated
    public Builder setNullHandlingEnabled(boolean nullHandlingEnabled) {
      return setDefaultNullHandlingEnabled(nullHandlingEnabled);
    }

    /**
     * Whether null handling is enabled by default. This value is only used if
     * {@link Schema#isEnableColumnBasedNullHandling()} is false.
     */
    public Builder setDefaultNullHandlingEnabled(boolean defaultNullHandlingEnabled) {
      _defaultNullHandlingEnabled = defaultNullHandlingEnabled;
      return this;
    }

    public Builder setPartitionUpsertMetadataManager(PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
      _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
      return this;
    }

    public Builder setPartitionDedupMetadataManager(PartitionDedupMetadataManager partitionDedupMetadataManager) {
      _partitionDedupMetadataManager = partitionDedupMetadataManager;
      return this;
    }

    public Builder setConsumerDir(String consumerDir) {
      _consumerDir = consumerDir;
      return this;
    }

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setInstanceId(String instanceId) {
      _instanceId = instanceId;
      return this;
    }

    public RealtimeSegmentConfig build() {
      Map<String, FieldIndexConfigs> indexConfigByCol = Maps.newHashMapWithExpectedSize(_indexConfigByCol.size());
      for (Map.Entry<String, FieldIndexConfigs.Builder> entry : _indexConfigByCol.entrySet()) {
        indexConfigByCol.put(entry.getKey(), entry.getValue().build());
      }

      return new RealtimeSegmentConfig(_tableNameWithType, _segmentName, _streamName, _schema, _timeColumnName,
          _capacity, _avgNumMultiValues, Collections.unmodifiableMap(indexConfigByCol), _segmentZKMetadata, _offHeap,
          _memoryManager, _statsHistory, _partitionColumn, _partitionFunction, _partitionId, _aggregateMetrics,
          _ingestionAggregationConfigs, _defaultNullHandlingEnabled, _partitionUpsertMetadataManager,
          _partitionDedupMetadataManager, _consumerDir, _tableConfig, _instanceId);
    }
  }
}
