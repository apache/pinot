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

package com.linkedin.pinot.core.realtime.converter.stats;

import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentRecordReader;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsContainer;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import java.util.HashMap;
import java.util.Map;


/**
 * Stats container for an in-memory realtime segment.
 */
public class RealtimeSegmentStatsContainer implements SegmentPreIndexStatsContainer {
  private final RealtimeSegmentImpl _realtimeSegment;
  private final RealtimeSegmentRecordReader _realtimeSegmentRecordReader;
  private final Map<String, ColumnStatistics> _columnStatisticsMap = new HashMap<>();

  public RealtimeSegmentStatsContainer(RealtimeSegmentImpl realtimeSegment,
      RealtimeSegmentRecordReader realtimeSegmentRecordReader) {
    _realtimeSegment = realtimeSegment;
    _realtimeSegmentRecordReader = realtimeSegmentRecordReader;

    SegmentPartitionConfig segmentPartitionConfig = _realtimeSegment.getSegmentPartitionConfig();

    // Create all column statistics
    for (String columnName : realtimeSegment.getColumnNames()) {
      ColumnDataSource dataSource = realtimeSegment.getDataSource(columnName);
      if (dataSource.getDataSourceMetadata().hasDictionary()) {
        _columnStatisticsMap.put(columnName, new RealtimeColumnStatistics(realtimeSegment.getDataSource(columnName),
            _realtimeSegmentRecordReader.getSortedDocIdIterationOrder(),
            (segmentPartitionConfig == null) ? null : segmentPartitionConfig.getColumnPartitionMap().get(columnName)));
      } else {
        _columnStatisticsMap.put(columnName, new RealtimeNoDictionaryColStatistics(dataSource));
      }
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) throws Exception {
    return _columnStatisticsMap.get(column);
  }

  @Override
  public int getRawDocCount() {
    return _realtimeSegment.getRawDocumentCount();
  }

  @Override
  public int getAggregatedDocCount() {
    return 0;
  }

  @Override
  public int getTotalDocCount() {
    return getRawDocCount();
  }
}
