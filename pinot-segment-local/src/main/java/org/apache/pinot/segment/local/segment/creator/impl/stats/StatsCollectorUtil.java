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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.ClusterConfigForTable;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Utility class for creating column statistics collectors.
 */
public final class StatsCollectorUtil {
  private StatsCollectorUtil() {
  }

  /**
   * Create a statistics collector for the given column based on its data type.
   *
   * @param columnName Name of the column
   * @param fieldSpec Field specification for the column
   * @param statsCollectorConfig Stats collector configuration
   * @return AbstractColumnStatisticsCollector for the column
   */
  public static AbstractColumnStatisticsCollector createStatsCollector(String columnName, FieldSpec fieldSpec,
      FieldIndexConfigs indexConfig, StatsCollectorConfig statsCollectorConfig) {
    boolean dictionaryEnabled = indexConfig.getConfig(StandardIndexes.dictionary()).isEnabled();
    FieldSpec.DataType storedType = fieldSpec.getDataType().getStoredType();
    if (!dictionaryEnabled && storedType != FieldSpec.DataType.MAP
        && storedType != FieldSpec.DataType.OPEN_STRUCT) {
      if (ClusterConfigForTable.useOptimizedNoDictCollector(statsCollectorConfig.getTableConfig())) {
        return new NoDictColumnStatisticsCollector(columnName, statsCollectorConfig);
      }
    }
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        return new IntColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case LONG:
        return new LongColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case STRING:
        return new StringColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case BYTES:
        return new BytesColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      case MAP:
      case OPEN_STRUCT:
        return new MapColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      default:
        throw new IllegalStateException("Unsupported data type: " + fieldSpec.getDataType());
    }
  }

  /// Creates a scalar stats collector directly from a [FieldSpec], for callers without a
  /// [StatsCollectorConfig]/[Schema] (e.g. OPEN_STRUCT materialized child columns). Skips the
  /// no-dictionary-optimization branch, which requires a TableConfig that synthetic columns lack.
  public static AbstractColumnStatisticsCollector createStatsCollector(FieldSpec fieldSpec,
      @Nullable FieldConfig fieldConfig) {
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        return new IntColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      case LONG:
        return new LongColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      case STRING:
        return new StringColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      case BYTES:
        return new BytesColumnPreIndexStatsCollector(fieldSpec, fieldConfig, null);
      default:
        throw new IllegalStateException("Unsupported stored type for OPEN_STRUCT child: "
            + fieldSpec.getDataType().getStoredType());
    }
  }
}
