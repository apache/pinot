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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsCollector;


/**
 * Nov 7, 2014
 */

public class SegmentPreIndexStatsCollectorImpl implements SegmentPreIndexStatsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreIndexStatsCollectorImpl.class);

  private final Schema dataSchema;
  Map<String, AbstractColumnStatisticsCollector> columnStatsCollectorMap;

  public SegmentPreIndexStatsCollectorImpl(Schema dataSchema) {
    this.dataSchema = dataSchema;
  }

  @Override
  public void init() {
    columnStatsCollectorMap = new HashMap<String, AbstractColumnStatisticsCollector>();

    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      switch (spec.getDataType()) {
        case BOOLEAN:
        case STRING:
          columnStatsCollectorMap.put(spec.getName(), new StringColumnPreIndexStatsCollector(spec));
          break;
        case INT:
          columnStatsCollectorMap.put(spec.getName(), new IntColumnPreIndexStatsCollector(spec));
          break;
        case LONG:
          columnStatsCollectorMap.put(spec.getName(), new LongColumnPreIndexStatsCollector(spec));
          break;
        case FLOAT:
          columnStatsCollectorMap.put(spec.getName(), new FloatColumnPreIndexStatsCollector(spec));
          break;
        case DOUBLE:
          columnStatsCollectorMap.put(spec.getName(), new DoubleColumnPreIndexStatsCollector(spec));
          break;
        default:
          break;
      }
    }
  }

  @Override
  public void build() throws Exception {
    for (final String column : columnStatsCollectorMap.keySet()) {
      columnStatsCollectorMap.get(column).seal();
    }
  }

  @Override
  public AbstractColumnStatisticsCollector getColumnProfileFor(String column) throws Exception {
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
  }

  public static <T> T convertInstanceOfObject(Object o, Class<T> clazz) {
    try {
      return clazz.cast(o);
    } catch (final ClassCastException e) {
      LOGGER.warn("Caught exception while converting instance", e);
      return null;
    }
  }

  @Override
  public void logStats() {
    try {
      for (final String column : columnStatsCollectorMap.keySet()) {
        LOGGER.info("********** logging for column : " + column + " ********************* ");
        LOGGER.info("min value : " + columnStatsCollectorMap.get(column).getMinValue());
        LOGGER.info("max value : " + columnStatsCollectorMap.get(column).getMaxValue());
        LOGGER.info("cardinality : " + columnStatsCollectorMap.get(column).getCardinality());
        LOGGER.info("length of largest column : " + columnStatsCollectorMap.get(column).getLengthOfLargestElement());
        LOGGER.info("is sorted : " + columnStatsCollectorMap.get(column).isSorted());
        LOGGER.info("column type : " + dataSchema.getFieldSpecFor(column).getDataType());
        LOGGER.info("***********************************************");
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while logging column stats", e);
    }

  }
}
