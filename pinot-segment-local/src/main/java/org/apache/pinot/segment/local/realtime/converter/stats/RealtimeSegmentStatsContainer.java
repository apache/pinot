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
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;


/**
 * Stats container for an in-memory realtime segment.
 */
public class RealtimeSegmentStatsContainer implements SegmentPreIndexStatsContainer {
  private final MutableSegment _mutableSegment;
  private final Map<String, ColumnStatistics> _columnStatisticsMap = new HashMap<>();

  public RealtimeSegmentStatsContainer(MutableSegment mutableSegment, @Nullable int[] sortedDocIds,
      StatsCollectorConfig statsCollectorConfig) {
    _mutableSegment = mutableSegment;

    // Create all column statistics
    for (String columnName : mutableSegment.getPhysicalColumnNames()) {
      DataSource dataSource = mutableSegment.getDataSource(columnName);
      if (dataSource instanceof MutableMapDataSource) {
        ForwardIndexReader reader = dataSource.getForwardIndex();
        MapColumnPreIndexStatsCollector mapColumnPreIndexStatsCollector =
            new MapColumnPreIndexStatsCollector(dataSource.getColumnName(), statsCollectorConfig);
        int numDocs = dataSource.getDataSourceMetadata().getNumDocs();
        for (int row = 0; row < numDocs; row++) {
          mapColumnPreIndexStatsCollector.collect(reader.getMap(row, reader.createContext()));
        }
        mapColumnPreIndexStatsCollector.seal();
        _columnStatisticsMap.put(columnName, mapColumnPreIndexStatsCollector);
      } else if (dataSource.getDictionary() != null) {
        _columnStatisticsMap.put(columnName, new MutableColumnStatistics(dataSource, sortedDocIds));
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
    return _mutableSegment.getNumDocsIndexed();
  }
}
