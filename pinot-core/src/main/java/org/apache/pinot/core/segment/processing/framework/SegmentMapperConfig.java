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

import java.util.List;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Config for the mapper phase of SegmentProcessorFramework
 */
public class SegmentMapperConfig {
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final RecordTransformerConfig _recordTransformerConfig;
  private final RecordFilterConfig _recordFilterConfig;
  private final List<PartitionerConfig> _partitionerConfigs;

  public SegmentMapperConfig(TableConfig tableConfig, Schema schema, RecordTransformerConfig recordTransformerConfig,
      RecordFilterConfig recordFilterConfig, List<PartitionerConfig> partitionerConfigs) {
    _tableConfig = tableConfig;
    _schema = schema;
    _recordTransformerConfig = recordTransformerConfig;
    _recordFilterConfig = recordFilterConfig;
    _partitionerConfigs = partitionerConfigs;
  }

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
   * The RecordTransformerConfig for the mapper
   */
  public RecordTransformerConfig getRecordTransformerConfig() {
    return _recordTransformerConfig;
  }

  /**
   * The RecordFilterConfig for the mapper
   */
  public RecordFilterConfig getRecordFilterConfig() {
    return _recordFilterConfig;
  }

  /**
   * The PartitioningConfig for the mapper
   */
  public List<PartitionerConfig> getPartitionerConfigs() {
    return _partitionerConfigs;
  }
}
