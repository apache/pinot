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

import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;


public interface TableExecutionInfo {
  boolean isRealtime();

  class ConsumingSegmentsInfo {
    private final int _numConsumingSegmentsQueried;
    private final long _minIndexTimeMs;
    private final long _minIngestionTimeMs;
    private final long _maxEndTimeMs;

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
  }

  List<IndexSegment> getIndexSegments();

  List<IndexSegment> getCopyOfIndexSegments();

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
}
