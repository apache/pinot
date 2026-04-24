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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;


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
public class ColumnarSegmentPreIndexStatsContainer implements SegmentPreIndexStatsContainer {
  private final Map<String, ColumnStatistics> _columnStatisticsMap;
  private final int _totalDocCount;

  public ColumnarSegmentPreIndexStatsContainer(StatsCollectorConfig statsCollectorConfig,
      Map<String, ColumnReader> columnReaders) {
    int totalDocs = -1;
    for (ColumnReader columnReader : columnReaders.values()) {
      if (totalDocs < 0) {
        totalDocs = columnReader.getTotalDocs();
      } else {
        Preconditions.checkState(columnReader.getTotalDocs() == totalDocs,
            "ColumnReader totalDocs mismatch for column: %s. Expected: %s, got: %s", columnReader.getColumnName(),
            totalDocs, columnReader.getTotalDocs());
      }
    }
    Preconditions.checkState(totalDocs >= 0, "Total docs must not be negative, got: %s", totalDocs);
    _totalDocCount = totalDocs;

    Schema schema = statsCollectorConfig.getSchema();
    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    _columnStatisticsMap = Maps.newHashMapWithExpectedSize(fieldSpecs.size());
    if (_totalDocCount == 0) {
      for (FieldSpec fieldSpec : fieldSpecs) {
        if (!fieldSpec.isVirtualColumn()) {
          String columnName = fieldSpec.getName();
          PartitionFunction partitionFunction = statsCollectorConfig.getPartitionFunction(columnName);
          Set<Integer> partitions = partitionFunction != null ? Set.of() : null;
          _columnStatisticsMap.put(columnName, new EmptyColumnStatistics(fieldSpec, partitionFunction, partitions));
        }
      }
    } else {
      Map<String, FieldIndexConfigs> indexConfigsByCol =
          FieldIndexConfigsUtil.createIndexConfigsByColName(statsCollectorConfig.getTableConfig(), schema);
      for (FieldSpec fieldSpec : fieldSpecs) {
        if (fieldSpec.isVirtualColumn()) {
          continue;
        }
        String columnName = fieldSpec.getName();
        AbstractColumnStatisticsCollector statsCollector =
            StatsCollectorUtil.createStatsCollector(columnName, fieldSpec, indexConfigsByCol.get(columnName),
                statsCollectorConfig);
        ColumnReader columnReader = columnReaders.get(columnName);
        Preconditions.checkState(columnReader != null, "Failed to find column reader for column: %s", columnName);
        try {
          for (int i = 0; i < _totalDocCount; i++) {
            statsCollector.collect(columnReader.getValue(i));
          }
        } catch (IOException e) {
          throw new RuntimeException("Caught exception collecting stats for column: " + columnName, e);
        }
        statsCollector.seal();
        _columnStatisticsMap.put(columnName, statsCollector);
      }
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return _columnStatisticsMap.get(column);
  }

  @Override
  public int getTotalDocCount() {
    return _totalDocCount;
  }
}
