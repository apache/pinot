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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.stats.MapColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.map.MutableMapDataSource;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.roaringbitmap.PeekableIntIterator;


/**
 * Stats container for an in-memory realtime segment.
 */
public class RealtimeSegmentStatsContainer implements SegmentPreIndexStatsContainer {
  private final MutableSegment _mutableSegment;
  private final Map<String, ColumnStatistics> _columnStatisticsMap = new HashMap<>();
  private final int _totalDocCount;

  public RealtimeSegmentStatsContainer(MutableSegment mutableSegment, @Nullable int[] sortedDocIds,
      StatsCollectorConfig statsCollectorConfig) {
    this(mutableSegment, sortedDocIds, statsCollectorConfig, null);
  }

  public RealtimeSegmentStatsContainer(MutableSegment mutableSegment, @Nullable int[] sortedDocIds,
      StatsCollectorConfig statsCollectorConfig, @Nullable RecordReader recordReader) {
    _mutableSegment = mutableSegment;

    // Determine if we're using compacted reader
    boolean isUsingCompactedReader = recordReader instanceof CompactedPinotSegmentRecordReader;

    // Determine the correct total document count based on whether compaction is being used
    if (isUsingCompactedReader && mutableSegment.getValidDocIds() != null) {
      _totalDocCount = mutableSegment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
    } else {
      _totalDocCount = mutableSegment.getNumDocsIndexed();
    }

    // Create all column statistics
    // Determine compaction mode once for all columns
    boolean useCompactedStatistics = isUsingCompactedReader && mutableSegment.getValidDocIds() != null;
    ThreadSafeMutableRoaringBitmap validDocIds = useCompactedStatistics ? mutableSegment.getValidDocIds() : null;

    for (String columnName : mutableSegment.getPhysicalColumnNames()) {
      DataSource dataSource = mutableSegment.getDataSource(columnName);

      // Handle map columns
      if (dataSource instanceof MutableMapDataSource) {
        _columnStatisticsMap.put(columnName,
            createMapColumnStatistics(dataSource, useCompactedStatistics, validDocIds, statsCollectorConfig));
        continue;
      }

      // Handle dictionary columns
      if (dataSource.getDictionary() != null) {
        _columnStatisticsMap.put(columnName,
            createDictionaryColumnStatistics(dataSource, sortedDocIds, useCompactedStatistics, validDocIds));
        continue;
      }

      // Handle no dictionary columns
      _columnStatisticsMap.put(columnName, new MutableNoDictionaryColStatistics(dataSource));
    }
  }

  /**
   * Creates column statistics for map columns.
   */
  private ColumnStatistics createMapColumnStatistics(DataSource dataSource, boolean useCompactedStatistics,
      ThreadSafeMutableRoaringBitmap validDocIds, StatsCollectorConfig statsCollectorConfig) {
    ForwardIndexReader reader = dataSource.getForwardIndex();
    MapColumnPreIndexStatsCollector mapColumnPreIndexStatsCollector =
        new MapColumnPreIndexStatsCollector(dataSource.getColumnName(), statsCollectorConfig);

    if (useCompactedStatistics && validDocIds != null) {
      // COMPACTED: Only process valid documents for commit-time compaction
      PeekableIntIterator iterator = validDocIds.getMutableRoaringBitmap().toRoaringBitmap().getIntIterator();
      ForwardIndexReaderContext readerContext = reader.createContext();
      while (iterator.hasNext()) {
        int docId = iterator.next();
        mapColumnPreIndexStatsCollector.collect(reader.getMap(docId, readerContext));
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

  /**
   * Creates column statistics for dictionary columns.
   */
  private ColumnStatistics createDictionaryColumnStatistics(DataSource dataSource, int[] sortedDocIds,
      boolean useCompactedStatistics, ThreadSafeMutableRoaringBitmap validDocIds) {
    if (useCompactedStatistics) {
      if (dataSource.getForwardIndex().isDictionaryEncoded()) {
        // Safe to use getDictId() - forward index supports dictionary operations
        return new CompactedDictEncodedColumnStatistics(dataSource, sortedDocIds, validDocIds);
      } else {
        // Forward index doesn't support getDictId() - use raw value scanning
        return new CompactedRawIndexDictColumnStatistics(dataSource, sortedDocIds, validDocIds);
      }
    } else {
      // Regular case: non-compacted readers or no valid doc IDs available
      return new MutableColumnStatistics(dataSource, sortedDocIds);
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
