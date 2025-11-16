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

import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Utility class for creating column statistics collectors.
 */
public final class StatsCollectorUtil {

  private StatsCollectorUtil() {
    // Utility class
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
      StatsCollectorConfig statsCollectorConfig) {
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
        return new BytesColumnPredIndexStatsCollector(columnName, statsCollectorConfig);
      case MAP:
        return new MapColumnPreIndexStatsCollector(columnName, statsCollectorConfig);
      default:
        throw new IllegalStateException("Unsupported data type: " + fieldSpec.getDataType());
    }
  }
}
