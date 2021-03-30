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
package org.apache.pinot.core.realtime.converter.stats;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.realtime.converter.RealtimeSegmentRecordReader;
import org.apache.pinot.core.segment.creator.ColumnStatistics;
import org.apache.pinot.core.segment.creator.SegmentPreIndexStatsContainer;


/**
 * Stats container for an in-memory realtime segment.
 */
public class RealtimeSegmentStatsContainer implements SegmentPreIndexStatsContainer {
  private final MutableSegmentImpl _realtimeSegment;
  private final Map<String, ColumnStatistics> _columnStatisticsMap = new HashMap<>();

  public RealtimeSegmentStatsContainer(MutableSegmentImpl realtimeSegment,
      RealtimeSegmentRecordReader realtimeSegmentRecordReader) {
    _realtimeSegment = realtimeSegment;

    // Create all column statistics
    for (String columnName : realtimeSegment.getPhysicalColumnNames()) {
      DataSource dataSource = realtimeSegment.getDataSource(columnName);
      if (dataSource.getDictionary() != null) {
        _columnStatisticsMap.put(columnName, new MutableColumnStatistics(realtimeSegment.getDataSource(columnName),
            realtimeSegmentRecordReader.getSortedDocIdIterationOrder()));
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
    return _realtimeSegment.getNumDocsIndexed();
  }
}
