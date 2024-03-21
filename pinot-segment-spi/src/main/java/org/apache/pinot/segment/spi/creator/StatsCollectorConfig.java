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
package org.apache.pinot.segment.spi.creator;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Config class for Stats collector, contains all the configs and parameters required to build
 * the stats collector.
 */
public class StatsCollectorConfig {

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final SegmentPartitionConfig _segmentPartitionConfig;
  private final Map<String, FieldConfig> _columnFieldConfigMap;

  /**
   * Constructor for the class.
   * @param schema Data schema
   * @param segmentPartitionConfig Segment partitioning config
   */
  public StatsCollectorConfig(TableConfig tableConfig, Schema schema,
      @Nullable SegmentPartitionConfig segmentPartitionConfig) {
    Preconditions.checkNotNull(tableConfig);
    Preconditions.checkNotNull(schema);
    _tableConfig = tableConfig;
    _schema = schema;
    _segmentPartitionConfig = segmentPartitionConfig;
    _columnFieldConfigMap = new HashMap<>();
    if (tableConfig.getFieldConfigList() != null) {
      for (FieldConfig fieldConfig : tableConfig.getFieldConfigList()) {
        _columnFieldConfigMap.put(fieldConfig.getName(), fieldConfig);
      }
    }
  }

  @Nullable
  public FieldSpec getFieldSpecForColumn(String column) {
    return _schema.getFieldSpecFor(column);
  }

  @Nullable
  public String getPartitionFunctionName(String column) {
    if (_segmentPartitionConfig == null) {
      return null;
    }

    return _segmentPartitionConfig.getFunctionName(column);
  }

  /**
   * Returns the number of partitions for the specified column.
   * If segment partition config does not exist, returns {@link SegmentPartitionConfig#INVALID_NUM_PARTITIONS}.
   *
   * @param column Column for which to to return the number of partitions.
   * @return Number of partitions for the column.
   */
  public int getNumPartitions(String column) {
    return (_segmentPartitionConfig != null) ? _segmentPartitionConfig.getNumPartitions(column)
        : SegmentPartitionConfig.INVALID_NUM_PARTITIONS;
  }

  @Nullable
  public Map<String, String> getPartitionFunctionConfig(String column) {
    return (_segmentPartitionConfig != null) ? _segmentPartitionConfig.getFunctionConfig(column) : null;
  }

  public Schema getSchema() {
    return _schema;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @Nullable
  public FieldConfig getFieldConfigForColumn(String column) {
    return _columnFieldConfigMap.get(column);
  }
}
