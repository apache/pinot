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
package org.apache.pinot.core.query.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.pruner.SegmentPrunerStatistics;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;


public interface TableExecutionInfo {
  /**
   * Check if consuming segments are being queried.
   * @return true if consuming segments are being queried, false otherwise
   */
  boolean hasRealtime();

  SelectedSegmentsInfo getSelectedSegmentsInfo(QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, SegmentPrunerService segmentPrunerService);

  class ConsumingSegmentsInfo {
    private int _numConsumingSegmentsQueried;
    private long _minIndexTimeMs;
    private long _minIngestionTimeMs;
    private long _maxEndTimeMs;

    public ConsumingSegmentsInfo() {
      _numConsumingSegmentsQueried = 0;
      _minIndexTimeMs = Long.MAX_VALUE;
      _minIngestionTimeMs = Long.MAX_VALUE;
      _maxEndTimeMs = Long.MIN_VALUE;
    }

    public ConsumingSegmentsInfo(int numConsumingSegmentsQueried, long minIndexTimeMs, long minIngestionTimeMs,
        long maxEndTimeMs) {
      _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
      _minIndexTimeMs = minIndexTimeMs;
      _minIngestionTimeMs = minIngestionTimeMs;
      _maxEndTimeMs = maxEndTimeMs;
    }

    long getMinConsumingFreshnessTimeMs() {
      long minConsumingFreshnessTimeMs = 0;
      if (getMinIngestionTimeMs() != Long.MAX_VALUE) {
        minConsumingFreshnessTimeMs = getMinIngestionTimeMs();
      } else if (getMinIndexTimeMs() != Long.MAX_VALUE) {
        minConsumingFreshnessTimeMs = getMinIndexTimeMs();
      } else if (getMaxEndTimeMs() != Long.MIN_VALUE) {
        minConsumingFreshnessTimeMs = getMaxEndTimeMs();
      }
      return minConsumingFreshnessTimeMs;
    }

    public int getNumConsumingSegmentsQueried() {
      return _numConsumingSegmentsQueried;
    }

    public long getMinIndexTimeMs() {
      return _minIndexTimeMs;
    }

    public long getMinIngestionTimeMs() {
      return _minIngestionTimeMs;
    }

    public long getMaxEndTimeMs() {
      return _maxEndTimeMs;
    }

    public void aggregate(ConsumingSegmentsInfo other) {
      _numConsumingSegmentsQueried += other.getNumConsumingSegmentsQueried();
      _minIndexTimeMs = Math.min(_minIndexTimeMs, other.getMinIndexTimeMs());
      _minIngestionTimeMs = Math.min(_minIngestionTimeMs, other.getMinIngestionTimeMs());
      _maxEndTimeMs = Math.max(_maxEndTimeMs, other.getMaxEndTimeMs());
    }
  }

  List<IndexSegment> getIndexSegments();

  List<IndexSegment> getCopyOfIndexSegments();

  @Nullable
  Map<IndexSegment, SegmentContext> getProvidedSegmentContexts();

  List<String> getSegmentsToQuery();

  List<String> getOptionalSegments();

  List<SegmentDataManager> getSegmentDataManagers();

  void releaseSegmentDataManagers();

  List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments, Map<String, String> queryOptions);

  List<String> getNotAcquiredSegments();

  List<String> getMissingSegments();

  ConsumingSegmentsInfo getConsumingSegmentsInfo();

  int getNumSegmentsAcquired();

  class SelectedSegmentsInfo {
    public List<IndexSegment> _indexSegments;
    public long _numTotalDocs;
    public SegmentPrunerStatistics _prunerStats;
    public int _numTotalSegments;
    public int _numSelectedSegments;
    public List<SegmentContext> _selectedSegmentContexts;

    public SelectedSegmentsInfo() {
      _indexSegments = new ArrayList<>();
      _numTotalDocs = 0;
      _numTotalSegments = 0;
      _numSelectedSegments = 0;
      _selectedSegmentContexts = new ArrayList<>();
      _prunerStats = new SegmentPrunerStatistics();
      _prunerStats.setInvalidSegments(0);
      _prunerStats.setValuePruned(0);
      _prunerStats.setLimitPruned(0);
    }

    public SelectedSegmentsInfo(List<IndexSegment> indexSegments, long numTotalDocs,
        SegmentPrunerStatistics prunerStats, int numTotalSegments, int numSelectedSegments,
        List<SegmentContext> selectedSegmentContexts) {
      _indexSegments = indexSegments;
      _numTotalDocs = numTotalDocs;
      _prunerStats = prunerStats;
      _numTotalSegments = numTotalSegments;
      _numSelectedSegments = numSelectedSegments;
      _selectedSegmentContexts = selectedSegmentContexts;
    }

    public void aggregate(SelectedSegmentsInfo other) {
      _indexSegments.addAll(other._indexSegments);
      _numTotalDocs += other._numTotalDocs;
      _numTotalSegments += other._numTotalSegments;
      _numSelectedSegments += other._numSelectedSegments;
      _selectedSegmentContexts.addAll(other._selectedSegmentContexts);
      _prunerStats.setInvalidSegments(_prunerStats.getInvalidSegments() + other._prunerStats.getInvalidSegments());
      _prunerStats.setValuePruned(_prunerStats.getValuePruned() + other._prunerStats.getValuePruned());
      _prunerStats.setLimitPruned(_prunerStats.getLimitPruned() + other._prunerStats.getLimitPruned());
    }
  }
}
