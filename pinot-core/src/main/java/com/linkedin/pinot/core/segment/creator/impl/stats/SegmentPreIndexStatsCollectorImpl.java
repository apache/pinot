/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl.stats;

import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsCollector;


public class SegmentPreIndexStatsCollectorImpl implements SegmentPreIndexStatsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreIndexStatsCollectorImpl.class);

  private final StatsCollectorConfig _statsCollectorConfig;
  private Map<String, AbstractColumnStatisticsCollector> columnStatsCollectorMap;

  private int rawDocCount;
  private int aggregatedDocCount;
  private int totalDocCount;

  public SegmentPreIndexStatsCollectorImpl(StatsCollectorConfig statsCollectorConfig) {
    this._statsCollectorConfig = statsCollectorConfig;
  }

  @Override
  public void init() {
    columnStatsCollectorMap = new HashMap<>();

    Schema dataSchema = _statsCollectorConfig.getSchema();
    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      String column = spec.getName();
      switch (spec.getDataType()) {
        case BOOLEAN:
        case STRING:
          columnStatsCollectorMap.put(spec.getName(), new StringColumnPreIndexStatsCollector(column,
              _statsCollectorConfig));
          break;
        case INT:
          columnStatsCollectorMap.put(spec.getName(), new IntColumnPreIndexStatsCollector(column, _statsCollectorConfig));
          break;
        case LONG:
          columnStatsCollectorMap.put(spec.getName(), new LongColumnPreIndexStatsCollector(column,
              _statsCollectorConfig));
          break;
        case FLOAT:
          columnStatsCollectorMap.put(spec.getName(), new FloatColumnPreIndexStatsCollector(column,
              _statsCollectorConfig));
          break;
        case DOUBLE:
          columnStatsCollectorMap.put(spec.getName(), new DoubleColumnPreIndexStatsCollector(column,
              _statsCollectorConfig));
          break;
        default:
          break;
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
  public void collectRow(GenericRow row) throws Exception {
    collectRow(row, false);
  }

  @Override
  public void collectRow(GenericRow row, boolean isAggregated) throws Exception {
    for (Map.Entry<String, Object> columnNameAndValue : row.getEntrySet()) {
      final String columnName = columnNameAndValue.getKey();
      final Object value = columnNameAndValue.getValue();

      if (columnStatsCollectorMap.containsKey(columnName)) {
        try {
          columnStatsCollectorMap.get(columnName).collect(value, isAggregated);
        } catch (Exception e) {
          LOGGER.error("Exception while collecting stats for column:{} in row:{}", columnName, row);
          throw e;
        }
      }
    }

    ++totalDocCount;
    if (!isAggregated) {
      ++rawDocCount;
    } else {
      ++aggregatedDocCount;
    }
  }

  @Override
  public int getRawDocCount() {
    return rawDocCount;
  }

  @Override
  public int getAggregatedDocCount() {
    return aggregatedDocCount;
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
          LOGGER.info("min partition value: " + statisticsCollector.getPartitionRanges().toString());
        }
        LOGGER.info("***********************************************");
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while logging column stats", e);
    }

  }
}
