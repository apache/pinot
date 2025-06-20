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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.segment.creator.impl.stats.MapColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.map.MutableMapDataSource;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.readers.RecordReader;


/**
 * Stats container for an in-memory realtime segment.
 */
public class RealtimeSegmentStatsContainer implements SegmentPreIndexStatsContainer {
  private final MutableSegment _mutableSegment;
  private final Map<String, ColumnStatistics> _columnStatisticsMap = new HashMap<>();
  private final int _totalDocCount;

  public RealtimeSegmentStatsContainer(MutableSegment mutableSegment, int[] sortedDocIds,
      StatsCollectorConfig statsCollectorConfig) {
    this(mutableSegment, sortedDocIds, statsCollectorConfig, null);
  }

  public RealtimeSegmentStatsContainer(MutableSegment mutableSegment, int[] sortedDocIds,
      StatsCollectorConfig statsCollectorConfig, RecordReader recordReader) {
    _mutableSegment = mutableSegment;

    // Determine if we're using compacted reader
    boolean isUsingCompactedReader = recordReader != null
        && recordReader.getClass().getSimpleName().equals("CompactedPinotSegmentRecordReader");

    // Determine the correct total document count based on whether compaction is being used
    if (isUsingCompactedReader) {
      // When using CompactedPinotSegmentRecordReader, use the valid document count
      if (mutableSegment.getValidDocIds() != null) {
        _totalDocCount = mutableSegment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
      } else {
        _totalDocCount = mutableSegment.getNumDocsIndexed();
      }
    } else {
      // Use the original total document count for non-compacted readers
      _totalDocCount = mutableSegment.getNumDocsIndexed();
    }

    // Create all column statistics
    for (String columnName : mutableSegment.getPhysicalColumnNames()) {
      DataSource dataSource = mutableSegment.getDataSource(columnName);
      if (dataSource instanceof MutableMapDataSource) {
        ForwardIndexReader reader = dataSource.getForwardIndex();
        MapColumnPreIndexStatsCollector mapColumnPreIndexStatsCollector =
            new MapColumnPreIndexStatsCollector(dataSource.getColumnName(), statsCollectorConfig);
        int numDocs = dataSource.getDataSourceMetadata().getNumDocs();
        ForwardIndexReaderContext readerContext = reader.createContext();
        for (int row = 0; row < numDocs; row++) {
          mapColumnPreIndexStatsCollector.collect(reader.getMap(row, readerContext));
        }
        mapColumnPreIndexStatsCollector.seal();
        _columnStatisticsMap.put(columnName, mapColumnPreIndexStatsCollector);
      } else if (dataSource.getDictionary() != null) {
        // Use compacted column statistics if using compacted reader
        if (isUsingCompactedReader && mutableSegment.getValidDocIds() != null) {
          _columnStatisticsMap.put(columnName,
              new CompactedColumnStatistics(dataSource, sortedDocIds, mutableSegment.getValidDocIds()));
        } else {
          _columnStatisticsMap.put(columnName, new MutableColumnStatistics(dataSource, sortedDocIds));
        }
      } else {
        _columnStatisticsMap.put(columnName, new MutableNoDictionaryColStatistics(dataSource));
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

  /**
   * Custom column statistics that only considers dictionary values used by valid documents during compaction.
   */
  private static class CompactedColumnStatistics extends MutableColumnStatistics {
    private final Set<Integer> _usedDictIds;
    private final int _compactedCardinality;
    private final DataSource _dataSource;
    private final Object _compactedUniqueValues;

    public CompactedColumnStatistics(DataSource dataSource, int[] sortedDocIds,
        ThreadSafeMutableRoaringBitmap validDocIds) {
      super(dataSource, sortedDocIds);
      _dataSource = dataSource;

      String columnName = dataSource.getDataSourceMetadata().getFieldSpec().getName();

      // Find which dictionary IDs are actually used by valid documents
      _usedDictIds = new HashSet<>();
      MutableForwardIndex forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();

      // Iterate through valid document IDs
      int[] validDocIdsArray = validDocIds.getMutableRoaringBitmap().toArray();
      for (int docId : validDocIdsArray) {
        int dictId = forwardIndex.getDictId(docId);
        _usedDictIds.add(dictId);
      }

      _compactedCardinality = _usedDictIds.size();

      // Create compacted unique values array with only used dictionary values
      Dictionary dictionary = dataSource.getDictionary();
      Object originalValues = dictionary.getSortedValues();

      // Sort the used dictionary IDs to maintain sorted order in the compacted array
      List<Integer> sortedUsedDictIds = new ArrayList<>(_usedDictIds);
      Collections.sort(sortedUsedDictIds);

      // Create a compacted array containing only the used dictionary values in sorted order
      if (originalValues instanceof int[]) {
        int[] original = (int[]) originalValues;
        int[] compacted = new int[_compactedCardinality];
        int index = 0;
        for (Integer dictId : sortedUsedDictIds) {
          compacted[index++] = original[dictId];
        }
        _compactedUniqueValues = compacted;
      } else if (originalValues instanceof String[]) {
        String[] original = (String[]) originalValues;
        String[] compacted = new String[_compactedCardinality];
        int index = 0;
        for (Integer dictId : sortedUsedDictIds) {
          compacted[index++] = original[dictId];
        }
        _compactedUniqueValues = compacted;
      } else if (originalValues instanceof long[]) {
        long[] original = (long[]) originalValues;
        long[] compacted = new long[_compactedCardinality];
        int index = 0;
        for (Integer dictId : sortedUsedDictIds) {
          compacted[index++] = original[dictId];
        }
        _compactedUniqueValues = compacted;
      } else if (originalValues instanceof float[]) {
        float[] original = (float[]) originalValues;
        float[] compacted = new float[_compactedCardinality];
        int index = 0;
        for (Integer dictId : sortedUsedDictIds) {
          compacted[index++] = original[dictId];
        }
        _compactedUniqueValues = compacted;
      } else if (originalValues instanceof double[]) {
        double[] original = (double[]) originalValues;
        double[] compacted = new double[_compactedCardinality];
        int index = 0;
        for (Integer dictId : sortedUsedDictIds) {
          compacted[index++] = original[dictId];
        }
        _compactedUniqueValues = compacted;
      } else {
        // For other types (like BigDecimal, bytes), fall back to original values
        // This is safe because the cardinality will still be correct
        _compactedUniqueValues = originalValues;
      }
    }

    @Override
    public int getCardinality() {
      return _compactedCardinality;
    }

    @Override
    public Object getUniqueValuesSet() {
      return _compactedUniqueValues;
    }
  }
}
