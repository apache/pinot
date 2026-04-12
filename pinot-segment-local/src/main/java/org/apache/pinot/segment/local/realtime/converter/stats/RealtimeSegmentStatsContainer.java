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
package org.apache.pinot.segment.local.realtime.converter.stats;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.stats.MapColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.map.MutableMapDataSource;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Stats container for an in-memory realtime segment.
 */
public class RealtimeSegmentStatsContainer implements SegmentPreIndexStatsContainer {
  private final Map<String, ColumnStatistics> _columnStatisticsMap = new HashMap<>();
  private final int _totalDocCount;

  public RealtimeSegmentStatsContainer(MutableSegment mutableSegment, @Nullable int[] sortedDocIds,
      @Nullable String sortedColumn, @Nullable RoaringBitmap validDocIds, StatsCollectorConfig statsCollectorConfig) {
    _totalDocCount = validDocIds != null ? validDocIds.getCardinality() : mutableSegment.getNumDocsIndexed();

    for (String columnName : mutableSegment.getPhysicalColumnNames()) {
      DataSource dataSource = mutableSegment.getDataSource(columnName);
      boolean isSortedColumn = columnName.equals(sortedColumn);
      _columnStatisticsMap.put(columnName,
          createColumnStatistics(dataSource, sortedDocIds, isSortedColumn, validDocIds, statsCollectorConfig));
    }
  }

  /**
   * Creates the appropriate {@link ColumnStatistics} for the given data source, dispatching on
   * column type (map, dictionary, or no-dictionary) and whether compaction is active.
   */
  private ColumnStatistics createColumnStatistics(DataSource dataSource, @Nullable int[] sortedDocIds,
      boolean isSortedColumn, @Nullable RoaringBitmap validDocIds, StatsCollectorConfig statsCollectorConfig) {
    Preconditions.checkState(!isSortedColumn || dataSource.getDataSourceMetadata().isSingleValue(),
        "Sorted column must be single-valued, but column '%s' is multi-valued",
        dataSource.getDataSourceMetadata().getFieldSpec().getName());

    if (dataSource instanceof MutableMapDataSource) {
      return createMapColumnStatistics(dataSource, validDocIds, statsCollectorConfig);
    }
    if (validDocIds != null) {
      if (dataSource.getDictionary() != null) {
        return new CompactedColumnStatistics(dataSource, sortedDocIds, isSortedColumn, validDocIds);
      } else {
        return new CompactedNoDictColumnStatistics(dataSource, sortedDocIds, isSortedColumn, validDocIds);
      }
    } else {
      if (dataSource.getDictionary() != null) {
        return new MutableColumnStatistics(dataSource, sortedDocIds, isSortedColumn);
      } else {
        return new MutableNoDictColumnStatistics(dataSource, sortedDocIds, isSortedColumn);
      }
    }
  }

  /**
   * Creates column statistics for map columns.
   */
  private ColumnStatistics createMapColumnStatistics(DataSource dataSource, @Nullable RoaringBitmap validDocIds,
      StatsCollectorConfig statsCollectorConfig) {
    ForwardIndexReader reader = dataSource.getForwardIndex();
    MapColumnPreIndexStatsCollector mapColumnPreIndexStatsCollector =
        new MapColumnPreIndexStatsCollector(dataSource.getColumnName(), statsCollectorConfig);

    if (validDocIds != null) {
      PeekableIntIterator iterator = validDocIds.getIntIterator();
      ForwardIndexReaderContext readerContext = reader.createContext();
      while (iterator.hasNext()) {
        mapColumnPreIndexStatsCollector.collect(reader.getMap(iterator.next(), readerContext));
      }
    } else {
      int numDocs = dataSource.getDataSourceMetadata().getNumDocs();
      ForwardIndexReaderContext readerContext = reader.createContext();
      for (int row = 0; row < numDocs; row++) {
        mapColumnPreIndexStatsCollector.collect(reader.getMap(row, readerContext));
      }
    }

    mapColumnPreIndexStatsCollector.seal();
    return mapColumnPreIndexStatsCollector;
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
