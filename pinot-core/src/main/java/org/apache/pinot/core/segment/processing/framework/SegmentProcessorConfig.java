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
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Config for configuring the phases of {@link SegmentProcessorFramework}
 */
public class SegmentProcessorConfig {
  private static final MergeType DEFAULT_MERGE_TYPE = MergeType.CONCAT;

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final MergeType _mergeType;
  private final RecordTransformerConfig _recordTransformerConfig;
  private final RecordFilterConfig _recordFilterConfig;
  private final List<PartitionerConfig> _partitionerConfigs;
  private final Map<String, AggregationFunctionType> _aggregationTypes;
  private final SegmentConfig _segmentConfig;

  private SegmentProcessorConfig(TableConfig tableConfig, Schema schema, MergeType mergeType,
      RecordTransformerConfig recordTransformerConfig, RecordFilterConfig recordFilterConfig,
      List<PartitionerConfig> partitionerConfigs, Map<String, AggregationFunctionType> aggregationTypes,
      SegmentConfig segmentConfig) {
    _tableConfig = tableConfig;
    _schema = schema;
    _mergeType = mergeType;
    _recordTransformerConfig = recordTransformerConfig;
    _recordFilterConfig = recordFilterConfig;
    _partitionerConfigs = partitionerConfigs;
    _aggregationTypes = aggregationTypes;
    _segmentConfig = segmentConfig;
  }

  /**
   * The Pinot table config
   */
  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  /**
   * The Pinot schema
   */
  public Schema getSchema() {
    return _schema;
  }

  /**
   * The merge type for the SegmentProcessorFramework
   */
  public MergeType getMergeType() {
    return _mergeType;
  }

  /**
   * The RecordTransformerConfig for the SegmentProcessorFramework's map phase
   */
  public RecordTransformerConfig getRecordTransformerConfig() {
    return _recordTransformerConfig;
  }

  /**
   * The RecordFilterConfig to filter records
   */
  public RecordFilterConfig getRecordFilterConfig() {
    return _recordFilterConfig;
  }

  /**
   * The PartitioningConfig for the SegmentProcessorFramework's map phase
   */
  public List<PartitionerConfig> getPartitionerConfigs() {
    return _partitionerConfigs;
  }

  /**
   * The aggregator types for the SegmentProcessorFramework's reduce phase with ROLLUP merge type
   */
  public Map<String, AggregationFunctionType> getAggregationTypes() {
    return _aggregationTypes;
  }

  /**
   * The SegmentConfig for the SegmentProcessorFramework's reduce phase
   */
  public SegmentConfig getSegmentConfig() {
    return _segmentConfig;
  }

  /**
   * Builder for SegmentProcessorConfig
   */
  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private MergeType _mergeType;
    private RecordTransformerConfig _recordTransformerConfig;
    private RecordFilterConfig _recordFilterConfig;
    private List<PartitionerConfig> _partitionerConfigs;
    private Map<String, AggregationFunctionType> _aggregationTypes;
    private SegmentConfig _segmentConfig;

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public Builder setMergeType(MergeType mergeType) {
      _mergeType = mergeType;
      return this;
    }

    public Builder setRecordTransformerConfig(RecordTransformerConfig recordTransformerConfig) {
      _recordTransformerConfig = recordTransformerConfig;
      return this;
    }

    public Builder setRecordFilterConfig(RecordFilterConfig recordFilterConfig) {
      _recordFilterConfig = recordFilterConfig;
      return this;
    }

    public Builder setPartitionerConfigs(List<PartitionerConfig> partitionerConfigs) {
      _partitionerConfigs = partitionerConfigs;
      return this;
    }

    public Builder setAggregationTypes(Map<String, AggregationFunctionType> aggregationTypes) {
      _aggregationTypes = aggregationTypes;
      return this;
    }

    public Builder setSegmentConfig(SegmentConfig segmentConfig) {
      _segmentConfig = segmentConfig;
      return this;
    }

    public SegmentProcessorConfig build() {
      Preconditions.checkState(_tableConfig != null, "Must provide table config in SegmentProcessorConfig");
      Preconditions.checkState(_schema != null, "Must provide schema in SegmentProcessorConfig");

      if (_recordTransformerConfig == null) {
        _recordTransformerConfig = new RecordTransformerConfig.Builder().build();
      }
      if (_recordFilterConfig == null) {
        _recordFilterConfig = new RecordFilterConfig.Builder().build();
      }
      if (CollectionUtils.isEmpty(_partitionerConfigs)) {
        _partitionerConfigs = Lists.newArrayList(new PartitionerConfig.Builder().build());
      }
      if (_mergeType == null) {
        _mergeType = DEFAULT_MERGE_TYPE;
      }
      if (_aggregationTypes == null) {
        _aggregationTypes = Collections.emptyMap();
      }
      if (_segmentConfig == null) {
        _segmentConfig = new SegmentConfig.Builder().build();
      }
      return new SegmentProcessorConfig(_tableConfig, _schema, _mergeType, _recordTransformerConfig,
          _recordFilterConfig, _partitionerConfigs, _aggregationTypes, _segmentConfig);
    }
  }

  @Override
  public String toString() {
    return "SegmentProcessorConfig{" + "_tableConfig=" + _tableConfig + ", _schema=" + _schema + ", _mergeType="
        + _mergeType + ", _recordTransformerConfig=" + _recordTransformerConfig + ", _recordFilterConfig="
        + _recordFilterConfig + ", _partitionerConfigs=" + _partitionerConfigs + ", _aggregationTypes="
        + _aggregationTypes + ", _segmentConfig=" + _segmentConfig + '}';
  }
}
