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
import java.util.stream.Collectors;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;


public class SingleTableExecutionInfo implements TableExecutionInfo {
  private final TableDataManager _tableDataManager;
  private final List<SegmentDataManager> _segmentDataManagers;
  private final List<IndexSegment> _indexSegments;
  private final Map<IndexSegment, SegmentContext> _providedSegmentContexts;
  private final List<String> _segmentsToQuery;
  private final List<String> _optionalSegments;
  private final List<String> _notAcquiredSegments;

  public SingleTableExecutionInfo(TableDataManager tableDataManager, List<SegmentDataManager> segmentDataManagers,
      List<IndexSegment> indexSegments, Map<IndexSegment, SegmentContext> providedSegmentContexts,
      List<String> segmentsToQuery, List<String> optionalSegments, List<String> notAcquiredSegments) {
    _tableDataManager = tableDataManager;
    _segmentDataManagers = segmentDataManagers;
    _indexSegments = indexSegments;
    _providedSegmentContexts = providedSegmentContexts;
    _segmentsToQuery = segmentsToQuery;
    _optionalSegments = optionalSegments;
    _notAcquiredSegments = notAcquiredSegments;
  }

  public TableDataManager getTableDataManager() {
    return _tableDataManager;
  }

  public List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @Override
  public void releaseSegmentDataManagers() {
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      _tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    return _tableDataManager.getSegmentContexts(selectedSegments, queryOptions);
  }

  @Override
  public List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @Override
  public List<IndexSegment> getCopyOfIndexSegments() {
    return new ArrayList<>(_indexSegments);
  }

  @Override
  public Map<IndexSegment, SegmentContext> getProvidedSegmentContexts() {
    return _providedSegmentContexts;
  }

  public List<String> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }

  @Override
  public List<String> getNotAcquiredSegments() {
    return _notAcquiredSegments;
  }

  @Override
  public List<String> getMissingSegments() {
    return _notAcquiredSegments.stream().filter(segmentName -> !_tableDataManager.isSegmentDeletedRecently(segmentName))
        .collect(Collectors.toList());
  }

  @Override
  public ConsumingSegmentsInfo getConsumingSegmentsInfo() {
    int numConsumingSegmentsQueried = 0;
    long minIndexTimeMs = 0;
    long minIngestionTimeMs = 0;
    long maxEndTimeMs = 0;
    if (_tableDataManager instanceof RealtimeTableDataManager) {
      minIndexTimeMs = Long.MAX_VALUE;
      minIngestionTimeMs = Long.MAX_VALUE;
      maxEndTimeMs = Long.MIN_VALUE;
      for (IndexSegment indexSegment : _indexSegments) {
        SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
        if (indexSegment instanceof MutableSegment) {
          numConsumingSegmentsQueried += 1;
          long indexTimeMs = segmentMetadata.getLastIndexedTimestamp();
          if (indexTimeMs > 0) {
            minIndexTimeMs = Math.min(minIndexTimeMs, indexTimeMs);
          }
          long ingestionTimeMs =
              ((RealtimeTableDataManager) _tableDataManager).getPartitionIngestionTimeMs(indexSegment.getSegmentName());
          if (ingestionTimeMs > 0) {
            minIngestionTimeMs = Math.min(minIngestionTimeMs, ingestionTimeMs);
          }
        } else if (indexSegment instanceof ImmutableSegment) {
          long indexCreationTime = segmentMetadata.getIndexCreationTime();
          if (indexCreationTime > 0) {
            maxEndTimeMs = Math.max(maxEndTimeMs, indexCreationTime);
          } else {
            // NOTE: the endTime may be totally inaccurate based on the value added in the timeColumn
            long endTime = segmentMetadata.getEndTime();
            if (endTime > 0) {
              maxEndTimeMs = Math.max(maxEndTimeMs, endTime);
            }
          }
        }
      }
    }

    return new ConsumingSegmentsInfo(numConsumingSegmentsQueried, minIndexTimeMs, minIngestionTimeMs, maxEndTimeMs);
  }

  @Override
  public int getNumSegmentsAcquired() {
    return _segmentDataManagers.size();
  }

  @Override
  public boolean isRealtime() {
    return _tableDataManager instanceof RealtimeTableDataManager;
  }
}
