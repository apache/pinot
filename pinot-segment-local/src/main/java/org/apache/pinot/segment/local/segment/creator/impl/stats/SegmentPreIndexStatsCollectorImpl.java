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

import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Map;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPreIndexStatsCollectorImpl implements SegmentPreIndexStatsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreIndexStatsCollectorImpl.class);

  private final StatsCollectorConfig _statsCollectorConfig;
  private Map<String, ColumnStatistics> _columnStatisticsMap;
  private int _totalDocCount;

  public SegmentPreIndexStatsCollectorImpl(StatsCollectorConfig statsCollectorConfig) {
    _statsCollectorConfig = statsCollectorConfig;
  }

  @Override
  public void init() {
    Schema schema = _statsCollectorConfig.getSchema();
    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    _columnStatisticsMap = Maps.newHashMapWithExpectedSize(fieldSpecs.size());
    Map<String, FieldIndexConfigs> indexConfigsByCol =
        FieldIndexConfigsUtil.createIndexConfigsByColName(_statsCollectorConfig.getTableConfig(), schema);
    for (FieldSpec fieldSpec : fieldSpecs) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }
      String column = fieldSpec.getName();
      AbstractColumnStatisticsCollector collector = StatsCollectorUtil.createStatsCollector(
          column, fieldSpec, indexConfigsByCol.get(column), _statsCollectorConfig);
      _columnStatisticsMap.put(column, collector);
    }
  }

  @Override
  public void build() {
    if (_totalDocCount > 0) {
      for (ColumnStatistics columnStatistics : _columnStatisticsMap.values()) {
        ((AbstractColumnStatisticsCollector) columnStatistics).seal();
      }
    } else {
      for (Map.Entry<String, ColumnStatistics> entry : _columnStatisticsMap.entrySet()) {
        ColumnStatistics columnStatistics = entry.getValue();
        entry.setValue(
            new EmptyColumnStatistics(columnStatistics.getFieldSpec(), columnStatistics.getPartitionFunction(),
                columnStatistics.getPartitions()));
      }
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return _columnStatisticsMap.get(column);
  }

  @Override
  public void collectRow(GenericRow row) {
    for (Map.Entry<String, ColumnStatistics> entry : _columnStatisticsMap.entrySet()) {
      String column = entry.getKey();
      ColumnStatistics columnStatistics = entry.getValue();
      ((AbstractColumnStatisticsCollector) columnStatistics).collect(row.getValue(column));
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
      for (final String column : _columnStatisticsMap.keySet()) {
        ColumnStatistics columnStatistics = _columnStatisticsMap.get(column);

        LOGGER.info("********** logging for column : {} ********************* ", column);
        LOGGER.info("min value : {}", columnStatistics.getMinValue());
        LOGGER.info("max value : {}", columnStatistics.getMaxValue());
        LOGGER.info("cardinality : {}", columnStatistics.getCardinality());
        LOGGER.info("length of longest element : {}", columnStatistics.getLengthOfLongestElement());
        LOGGER.info("is sorted : {}", columnStatistics.isSorted());
        LOGGER.info("column type : {}", _statsCollectorConfig.getSchema().getFieldSpecFor(column).getDataType());

        if (columnStatistics.getPartitionFunction() != null) {
          LOGGER.info("partitions: {}", columnStatistics.getPartitions().toString());
        }
        LOGGER.info("***********************************************");
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while logging column stats", e);
    }
  }
}
