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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stats container that efficiently collects statistics from columnar data using ColumnReader instances.
 *
 * <p>This implementation collects statistics by iterating column-wise instead of row-wise,
 * which is more efficient for columnar data sources. It supports:
 * <ul>
 *   <li>Column-wise statistics collection</li>
 *   <li>Existing columns from source data</li>
 *   <li>New columns with default values</li>
 *   <li>Data type conversions during schema evolution</li>
 * </ul>
 *
 * <p>The statistics are collected using the same underlying collectors as the row-based approach
 * (SegmentPreIndexStatsCollectorImpl) but with more efficient column-wise iteration.
 */
public class ColumnarSegmentPreIndexStatsContainer implements SegmentPreIndexStatsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarSegmentPreIndexStatsContainer.class);

  private final Map<String, ColumnReader> _columnReaders;
  private final StatsCollectorConfig _statsCollectorConfig;
  private final Schema _targetSchema;
  private final Map<String, AbstractColumnStatisticsCollector> _columnStatsCollectorMap;
  private int _totalDocCount;

  /**
   * Create a ColumnarSegmentPreIndexStatsContainer.
   *
   * @param columnReaders Map of column name to ColumnReader instances
   * @param statsCollectorConfig Configuration for statistics collection
   */
  public ColumnarSegmentPreIndexStatsContainer(Map<String, ColumnReader> columnReaders,
      StatsCollectorConfig statsCollectorConfig) {
    _columnReaders = columnReaders;
    _statsCollectorConfig = statsCollectorConfig;
    _targetSchema = statsCollectorConfig.getSchema();
    _columnStatsCollectorMap = new HashMap<>(columnReaders.size());
    _totalDocCount = -1; // indicates unset
  }

  /**
   * Initialize stats collectors for all columns in the target schema.
   */
  private void initializeStatsCollectors() {
    for (FieldSpec fieldSpec : _targetSchema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      AbstractColumnStatisticsCollector collector =
          StatsCollectorUtil.createStatsCollector(columnName, fieldSpec, _statsCollectorConfig);
      _columnStatsCollectorMap.put(columnName, collector);
    }
  }

  /**
   * Collect stats by iterating column-wise using the provided ColumnReader instances.
   */
  private void collectColumnStats() {
    LOGGER.info("Collecting stats for {} columns using column-wise iteration", _columnReaders.size());

    for (String columnName : _columnStatsCollectorMap.keySet()) {
      AbstractColumnStatisticsCollector statsCollector = _columnStatsCollectorMap.get(columnName);
      ColumnReader columnReader = _columnReaders.get(columnName);

      if (columnReader == null) {
        throw new RuntimeException("Column reader for column " + columnName + " not found");
      }

      LOGGER.debug("Collecting stats for column: {}", columnName);
      collectStatsFromColumnReader(columnName, columnReader, statsCollector);

      // Seal the stats collector
      statsCollector.seal();
    }
  }

  /**
   * Collect stats from a column reader by iterating over all values using the iterator pattern.
   */
  private void collectStatsFromColumnReader(String columnName, ColumnReader columnReader,
      AbstractColumnStatisticsCollector statsCollector) {
    try {
      // Reset the column reader to start from the beginning
      columnReader.rewind();

      int docCount = 0;
      while (columnReader.hasNext()) {
        Object value = columnReader.next();
        statsCollector.collect(value);
        docCount++;
      }

      // if totalDocCount is unset then set total doc count from the first column
      if (_totalDocCount == -1) {
        _totalDocCount = docCount;
      } else if (_totalDocCount != docCount) {
        // all columns should have same count
        LOGGER.warn("Column {} has {} documents, but expected {} documents",
            columnName, docCount, _totalDocCount);
        throw new RuntimeException("Columns have inconsistent document counts");
      }
    } catch (IOException e) {
      LOGGER.error("Failed to collect stats for column: {}", columnName, e);
      throw new RuntimeException("Failed to collect stats for column: " + columnName, e);
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return _columnStatsCollectorMap.get(column);
  }

  @Override
  public int getTotalDocCount() {
    return _totalDocCount;
  }

  @Override
  public void init() {
    initializeStatsCollectors();
    collectColumnStats();
  }

  @Override
  public void build() {
    // Stats are already collected in constructor
  }

  @Override
  public void logStats() {
    LOGGER.info("Columnar segment stats collection completed for {} columns with {} documents",
        _columnStatsCollectorMap.size(), _totalDocCount);
  }

  @Override
  public void collectRow(GenericRow row) {
    // This method is not used in columnar stats collection as we iterate column-wise
    // instead of row-wise. The stats are collected in the constructor by iterating
    // over each column using ColumnReader instances.
    throw new UnsupportedOperationException(
        "collectRow is not supported in columnar stats collection. Stats are collected column-wise.");
  }
}
