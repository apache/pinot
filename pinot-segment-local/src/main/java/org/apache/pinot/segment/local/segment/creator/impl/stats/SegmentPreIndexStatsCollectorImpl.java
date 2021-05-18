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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPreIndexStatsCollectorImpl implements SegmentPreIndexStatsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreIndexStatsCollectorImpl.class);

  private final StatsCollectorConfig _statsCollectorConfig;
  private Map<String, AbstractColumnStatisticsCollector> columnStatsCollectorMap;

  private int totalDocCount;

  public SegmentPreIndexStatsCollectorImpl(StatsCollectorConfig statsCollectorConfig) {
    _statsCollectorConfig = statsCollectorConfig;
  }

  @Override
  public void init() {
    columnStatsCollectorMap = new HashMap<>();

    Schema dataSchema = _statsCollectorConfig.getSchema();
    for (FieldSpec fieldSpec : dataSchema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      switch (fieldSpec.getDataType().getStoredType()) {
        case INT:
          columnStatsCollectorMap.put(column, new IntColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case LONG:
          columnStatsCollectorMap.put(column, new LongColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case FLOAT:
          columnStatsCollectorMap.put(column, new FloatColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case DOUBLE:
          columnStatsCollectorMap.put(column, new DoubleColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case STRING:
          columnStatsCollectorMap.put(column, new StringColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case BYTES:
          columnStatsCollectorMap.put(column, new BytesColumnPredIndexStatsCollector(column, _statsCollectorConfig));
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + fieldSpec.getDataType());
      }
    }
  }

  @Override
  public void build() {
    for (final String column : columnStatsCollectorMap.keySet()) {
      columnStatsCollectorMap.get(column).seal();
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return columnStatsCollectorMap.get(column);
  }

  @Override
  public void collectRow(GenericRow row) {
    for (Map.Entry<String, Object> columnNameAndValue : row.getFieldToValueMap().entrySet()) {
      final String columnName = columnNameAndValue.getKey();
      final Object value = columnNameAndValue.getValue();

      if (columnStatsCollectorMap.containsKey(columnName)) {
        try {
          columnStatsCollectorMap.get(columnName).collect(value);
        } catch (Exception e) {
          LOGGER.error("Exception while collecting stats for column:{} in row:{}", columnName, row);
          throw e;
        }
      }
    }

    ++totalDocCount;
  }

  @Override
  public int getTotalDocCount() {
    return totalDocCount;
  }

  @Override
  public void logStats() {
    try {
      for (final String column : columnStatsCollectorMap.keySet()) {
        AbstractColumnStatisticsCollector statisticsCollector = columnStatsCollectorMap.get(column);

        LOGGER.info("********** logging for column : " + column + " ********************* ");
        LOGGER.info("min value : " + statisticsCollector.getMinValue());
        LOGGER.info("max value : " + statisticsCollector.getMaxValue());
        LOGGER.info("cardinality : " + statisticsCollector.getCardinality());
        LOGGER.info("length of largest column : " + statisticsCollector.getLengthOfLargestElement());
        LOGGER.info("is sorted : " + statisticsCollector.isSorted());
        LOGGER.info("column type : " + _statsCollectorConfig.getSchema().getFieldSpecFor(column).getDataType());

        if (statisticsCollector.getPartitionFunction() != null) {
          LOGGER.info("partitions: " + statisticsCollector.getPartitions().toString());
        }
        LOGGER.info("***********************************************");
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while logging column stats", e);
    }
  }
}
