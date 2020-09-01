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
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitioningConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Config for configuring the phases of {@link SegmentProcessorFramework}
 */
public class SegmentProcessorConfig {

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final RecordTransformerConfig _recordTransformerConfig;
  private final RecordFilterConfig _recordFilterConfig;
  private final PartitioningConfig _partitioningConfig;
  private final CollectorConfig _collectorConfig;
  private final SegmentConfig _segmentConfig;

  private SegmentProcessorConfig(TableConfig tableConfig, Schema schema,
      RecordTransformerConfig recordTransformerConfig, RecordFilterConfig recordFilterConfig,
      PartitioningConfig partitioningConfig, CollectorConfig collectorConfig, SegmentConfig segmentConfig) {
    _tableConfig = tableConfig;
    _schema = schema;
    _recordTransformerConfig = recordTransformerConfig;
    _recordFilterConfig = recordFilterConfig;
    _partitioningConfig = partitioningConfig;
    _collectorConfig = collectorConfig;
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
  public PartitioningConfig getPartitioningConfig() {
    return _partitioningConfig;
  }

  /**
   * The CollectorConfig for the SegmentProcessorFramework's reduce phase
   */
  public CollectorConfig getCollectorConfig() {
    return _collectorConfig;
  }

  /**
   * The SegmentConfig for the SegmentProcessorFramework's segment generation phase
   */
  public SegmentConfig getSegmentConfig() {
    return _segmentConfig;
  }

  /**
   * Builder for SegmentProcessorConfig
   */
  public static class Builder {
    private TableConfig tableConfig;
    private Schema schema;
    private RecordTransformerConfig recordTransformerConfig;
    private RecordFilterConfig recordFilterConfig;
    private PartitioningConfig partitioningConfig;
    private CollectorConfig collectorConfig;
    private SegmentConfig _segmentConfig;

    public Builder setTableConfig(TableConfig tableConfig) {
      this.tableConfig = tableConfig;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setRecordTransformerConfig(RecordTransformerConfig recordTransformerConfig) {
      this.recordTransformerConfig = recordTransformerConfig;
      return this;
    }

    public Builder setRecordFilterConfig(RecordFilterConfig recordFilterConfig) {
      this.recordFilterConfig = recordFilterConfig;
      return this;
    }

    public Builder setPartitioningConfig(PartitioningConfig partitioningConfig) {
      this.partitioningConfig = partitioningConfig;
      return this;
    }

    public Builder setCollectorConfig(CollectorConfig collectorConfig) {
      this.collectorConfig = collectorConfig;
      return this;
    }

    public Builder setSegmentConfig(SegmentConfig segmentConfig) {
      this._segmentConfig = segmentConfig;
      return this;
    }

    public SegmentProcessorConfig build() {
      Preconditions.checkNotNull(tableConfig, "Must provide table config in SegmentProcessorConfig");
      Preconditions.checkNotNull(schema, "Must provide schema in SegmentProcessorConfig");
      if (recordTransformerConfig == null) {
        recordTransformerConfig = new RecordTransformerConfig.Builder().build();
      }
      if (recordFilterConfig == null) {
        recordFilterConfig = new RecordFilterConfig.Builder().build();
      }
      if (partitioningConfig == null) {
        partitioningConfig = new PartitioningConfig.Builder().build();
      }
      if (collectorConfig == null) {
        collectorConfig = new CollectorConfig.Builder().build();
      }
      if (_segmentConfig == null) {
        _segmentConfig = new SegmentConfig.Builder().build();
      }
      return new SegmentProcessorConfig(tableConfig, schema, recordTransformerConfig, recordFilterConfig,
          partitioningConfig, collectorConfig, _segmentConfig);
    }
  }

  @Override
  public String toString() {
    return "SegmentProcessorConfig{" + "\n_tableConfig=" + _tableConfig + ", \n_schema=" + _schema
        .toSingleLineJsonString() + ", \n_recordFilterConfig=" + _recordFilterConfig + ", \n_recordTransformerConfig="
        + _recordTransformerConfig + ", \n_partitioningConfig=" + _partitioningConfig + ", \n_collectorConfig="
        + _collectorConfig + ", \n_segmentsConfig=" + _segmentConfig + "\n}";
  }
}
