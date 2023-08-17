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
  private Map<String, AbstractColumnStatisticsCollector> _columnStatsCollectorMap;
  private int _totalDocCount;

  public SegmentPreIndexStatsCollectorImpl(StatsCollectorConfig statsCollectorConfig) {
    _statsCollectorConfig = statsCollectorConfig;
  }

  @Override
  public void init() {
    _columnStatsCollectorMap = new HashMap<>();

    Schema dataSchema = _statsCollectorConfig.getSchema();
    for (FieldSpec fieldSpec : dataSchema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      switch (fieldSpec.getDataType().getStoredType()) {
        case INT:
          _columnStatsCollectorMap.put(column, new IntColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case LONG:
          _columnStatsCollectorMap.put(column, new LongColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case FLOAT:
          _columnStatsCollectorMap.put(column, new FloatColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case DOUBLE:
          _columnStatsCollectorMap.put(column, new DoubleColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case BIG_DECIMAL:
          _columnStatsCollectorMap.put(column,
              new BigDecimalColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case STRING:
          _columnStatsCollectorMap.put(column, new StringColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case BYTES:
          _columnStatsCollectorMap.put(column, new BytesColumnPredIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case VECTOR:
          _columnStatsCollectorMap.put(column, new VectorColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + fieldSpec.getDataType());
      }
    }
  }

  @Override
  public void build() {
    for (AbstractColumnStatisticsCollector columnStatsCollector : _columnStatsCollectorMap.values()) {
      columnStatsCollector.seal();
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return _columnStatsCollectorMap.get(column);
  }

  @Override
  public void collectRow(GenericRow row) {
    for (Map.Entry<String, Object> columnNameAndValue : row.getFieldToValueMap().entrySet()) {
      final String columnName = columnNameAndValue.getKey();
      final Object value = columnNameAndValue.getValue();

      if (_columnStatsCollectorMap.containsKey(columnName)) {
        try {
          _columnStatsCollectorMap.get(columnName).collect(value);
        } catch (Exception e) {
          LOGGER.error("Exception while collecting stats for column:{} in row:{}", columnName, row);
          throw e;
        }
      }
    }

    _totalDocCount++;
  }

  @Override
  public int getTotalDocCount() {
    return _totalDocCount;
  }

  @Override
  public void logStats() {
    try {
      for (final String column : _columnStatsCollectorMap.keySet()) {
        AbstractColumnStatisticsCollector statisticsCollector = _columnStatsCollectorMap.get(column);

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
