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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.pruner.SegmentPrunerStatistics;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SingleTableExecutionInfo implements TableExecutionInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleTableExecutionInfo.class);

  private final TableDataManager _tableDataManager;
  private final List<SegmentDataManager> _segmentDataManagers;
  private final List<IndexSegment> _indexSegments;
  private final Map<IndexSegment, SegmentContext> _providedSegmentContexts;
  private final List<String> _segmentsToQuery;
  private final List<String> _optionalSegments;
  private final List<String> _notAcquiredSegments;

  @Nullable
  public static SingleTableExecutionInfo create(InstanceDataManager instanceDataManager, String tableNameWithType,
      List<String> segmentsToQuery, List<String> optionalSegments, QueryContext queryContext) {
    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      return null;
    }

    List<String> notAcquiredSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers;
    List<IndexSegment> indexSegments;
    Map<IndexSegment, SegmentContext> providedSegmentContexts = null;

    if (!isUpsertTable(tableDataManager)) {
      segmentDataManagers = tableDataManager.acquireSegments(segmentsToQuery, optionalSegments, notAcquiredSegments);
      indexSegments = new ArrayList<>(segmentDataManagers.size());
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        indexSegments.add(segmentDataManager.getSegment());
      }
    } else {
      RealtimeTableDataManager rtdm = (RealtimeTableDataManager) tableDataManager;
      TableUpsertMetadataManager tumm = rtdm.getTableUpsertMetadataManager();
      boolean isUsingConsistencyMode =
          rtdm.getTableUpsertMetadataManager().getContext().getConsistencyMode() != UpsertConfig.ConsistencyMode.NONE;
      if (isUsingConsistencyMode) {
        tumm.lockForSegmentContexts();
      }
      try {
        Set<String> allSegmentsToQuery = new HashSet<>(segmentsToQuery);
        if (optionalSegments == null) {
          optionalSegments = new ArrayList<>();
        } else {
          allSegmentsToQuery.addAll(optionalSegments);
        }
        for (String segmentName : tumm.getNewlyAddedSegments()) {
          if (!allSegmentsToQuery.contains(segmentName)) {
            optionalSegments.add(segmentName);
          }
        }
        segmentDataManagers = tableDataManager.acquireSegments(segmentsToQuery, optionalSegments, notAcquiredSegments);
        indexSegments = new ArrayList<>(segmentDataManagers.size());
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          if (segmentDataManager.hasMultiSegments()) {
            indexSegments.addAll(segmentDataManager.getSegments());
          } else {
            indexSegments.add(segmentDataManager.getSegment());
          }
        }
        if (isUsingConsistencyMode) {
          List<SegmentContext> segmentContexts =
              tableDataManager.getSegmentContexts(indexSegments, queryContext.getQueryOptions());
          providedSegmentContexts = new HashMap<>(segmentContexts.size());
          for (SegmentContext sc : segmentContexts) {
            providedSegmentContexts.put(sc.getIndexSegment(), sc);
          }
        }
      } finally {
        if (isUsingConsistencyMode) {
          tumm.unlockForSegmentContexts();
        }
      }
    }

    return new SingleTableExecutionInfo(tableDataManager, segmentDataManagers, indexSegments, providedSegmentContexts,
        segmentsToQuery, optionalSegments, notAcquiredSegments);
  }

  private static boolean isUpsertTable(TableDataManager tableDataManager) {
    // For upsert table, the server can start to process newly added segments before brokers can add those segments
    // into their routing tables, like newly created consuming segment or newly uploaded segments. We should include
    // those segments in the list of segments for query to process on the server, otherwise, the query will see less
    // than expected valid docs from the upsert table.
    if (tableDataManager instanceof RealtimeTableDataManager) {
      RealtimeTableDataManager rtdm = (RealtimeTableDataManager) tableDataManager;
      return rtdm.isUpsertEnabled();
    }
    return false;
  }

  private SingleTableExecutionInfo(TableDataManager tableDataManager, List<SegmentDataManager> segmentDataManagers,
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

  @Override
  public boolean hasRealtime() {
    return _tableDataManager instanceof RealtimeTableDataManager;
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

  @Nullable
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
  public int getNumSegmentsAcquired() {
    return _segmentDataManagers.size();
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
  public SelectedSegmentsInfo getSelectedSegmentsInfo(QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, SegmentPrunerService segmentPrunerService) {
    // Make a copy of the segments to avoid modifying the original list
    List<IndexSegment> indexSegments = new ArrayList<>(getIndexSegments());
    Map<IndexSegment, SegmentContext> providedSegmentContexts = getProvidedSegmentContexts();

    // Compute total docs for the table before pruning the segments
    long numTotalDocs = 0;
    for (IndexSegment indexSegment : indexSegments) {
      numTotalDocs += indexSegment.getSegmentMetadata().getTotalDocs();
    }

    SegmentPrunerStatistics prunerStats = new SegmentPrunerStatistics();
    List<IndexSegment> selectedSegments =
        selectSegments(indexSegments, queryContext, timerContext, executorService, segmentPrunerService, prunerStats);

    int numTotalSegments = indexSegments.size();
    int numSelectedSegments = selectedSegments.size();
    LOGGER.debug("Matched {} segments after pruning", numSelectedSegments);
    List<SegmentContext> selectedSegmentContexts;
    if (providedSegmentContexts == null) {
      selectedSegmentContexts = getSegmentContexts(selectedSegments, queryContext.getQueryOptions());
    } else {
      selectedSegmentContexts = new ArrayList<>(selectedSegments.size());
      selectedSegments.forEach(s -> selectedSegmentContexts.add(providedSegmentContexts.get(s)));
    }
    return new SelectedSegmentsInfo(indexSegments, numTotalDocs, prunerStats, numTotalSegments, numSelectedSegments,
        selectedSegmentContexts);
  }

  private List<IndexSegment> selectSegments(List<IndexSegment> indexSegments, QueryContext queryContext,
      TimerContext timerContext, ExecutorService executorService, SegmentPrunerService segmentPrunerService,
      SegmentPrunerStatistics prunerStats) {
    List<IndexSegment> selectedSegments;
    if ((queryContext.getFilter() != null && queryContext.getFilter().isConstantFalse()) || (
        queryContext.getHavingFilter() != null && queryContext.getHavingFilter().isConstantFalse())) {
      selectedSegments = Collections.emptyList();
    } else {
      TimerContext.Timer segmentPruneTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.SEGMENT_PRUNING);
      selectedSegments = segmentPrunerService.prune(indexSegments, queryContext, prunerStats, executorService);
      segmentPruneTimer.stopAndRecord();
    }
    return selectedSegments;
  }
}
