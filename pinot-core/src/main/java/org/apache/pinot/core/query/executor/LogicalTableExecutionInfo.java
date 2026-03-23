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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.pruner.SegmentPrunerStatistics;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TableSegmentsContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogicalTableExecutionInfo implements TableExecutionInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableExecutionInfo.class);

  public static LogicalTableExecutionInfo create(InstanceDataManager instanceDataManager,
      ServerQueryRequest queryRequest, QueryContext queryContext)
      throws TableNotFoundException {
    List<TableSegmentsContext> tableSegmentsContexts = queryRequest.getTableSegmentsContexts();
    List<SingleTableExecutionInfo> tableExecutionInfos =
        new ArrayList<>(Objects.requireNonNull(tableSegmentsContexts).size());
    for (TableSegmentsContext tableSegmentsContext : tableSegmentsContexts) {
      SingleTableExecutionInfo singleTableExecutionInfo =
          SingleTableExecutionInfo.create(instanceDataManager, tableSegmentsContext.getTableName(),
              tableSegmentsContext.getSegments(), tableSegmentsContext.getOptionalSegments(), queryContext);
      tableExecutionInfos.add(singleTableExecutionInfo);
    }

    return new LogicalTableExecutionInfo(tableExecutionInfos);
  }

  private final List<SingleTableExecutionInfo> _tableExecutionInfos;

  public LogicalTableExecutionInfo(List<SingleTableExecutionInfo> tableExecutionInfos) {
    _tableExecutionInfos = tableExecutionInfos;
  }

  @Override
  public Schema getSchema() {
    // TODO: Return the schema of the logical table
    return _tableExecutionInfos.get(0).getSchema();
  }

  @Override
  public boolean hasRealtime() {
    return _tableExecutionInfos.stream()
        .anyMatch(tableExecutionInfo -> tableExecutionInfo.getTableDataManager() instanceof RealtimeTableDataManager);
  }

  /**
   * Returns selected segments and contexts for the logical table. Unlike single-table execution, this collects
   * all segments from every physical table, runs segment pruning once on the combined list (cross-table prune),
   * then resolves segment contexts per table. This allows pruners such as SelectionQuerySegmentPruner (ORDER BY +
   * LIMIT) to prune effectively across the logical table rather than per physical table.
   */
  @Override
  public SelectedSegmentsInfo getSelectedSegmentsInfo(QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, SegmentPrunerService segmentPrunerService) {
    // Collect all segments from all physical tables (no pruning yet)
    List<IndexSegment> allSegments = new ArrayList<>();
    Map<IndexSegment, SingleTableExecutionInfo> segmentToTable = new HashMap<>();
    long numTotalDocs = 0;
    for (SingleTableExecutionInfo tableExecutionInfo : _tableExecutionInfos) {
      List<IndexSegment> indexSegments = tableExecutionInfo.getIndexSegments();
      for (IndexSegment segment : indexSegments) {
        allSegments.add(segment);
        segmentToTable.put(segment, tableExecutionInfo);
        numTotalDocs += segment.getSegmentMetadata().getTotalDocs();
      }
    }
    int numTotalSegments = allSegments.size();

    // Constant false shortcut: skip pruning
    SegmentPrunerStatistics prunerStats = new SegmentPrunerStatistics();
    List<IndexSegment> selectedSegments =
        selectSegments(allSegments, queryContext, timerContext, executorService, segmentPrunerService, prunerStats);

    // Build segment contexts for selected segments only, preserving prune order
    List<SegmentContext> selectedSegmentContexts = new ArrayList<>(selectedSegments.size());
    Map<SingleTableExecutionInfo, List<IndexSegment>> tableToSelected = new HashMap<>();
    for (IndexSegment segment : selectedSegments) {
      tableToSelected.computeIfAbsent(segmentToTable.get(segment), k -> new ArrayList<>()).add(segment);
    }
    Map<IndexSegment, SegmentContext> segmentToContext = new HashMap<>();
    for (Map.Entry<SingleTableExecutionInfo, List<IndexSegment>> entry : tableToSelected.entrySet()) {
      SingleTableExecutionInfo tableExecutionInfo = entry.getKey();
      List<IndexSegment> segmentsForTable = entry.getValue();
      Map<IndexSegment, SegmentContext> providedContexts = tableExecutionInfo.getProvidedSegmentContexts();
      if (providedContexts != null) {
        for (IndexSegment segment : segmentsForTable) {
          segmentToContext.put(segment, providedContexts.get(segment));
        }
      } else {
        List<SegmentContext> contexts =
            tableExecutionInfo.getSegmentContexts(segmentsForTable, queryContext.getQueryOptions());
        for (int i = 0; i < segmentsForTable.size(); i++) {
          segmentToContext.put(segmentsForTable.get(i), contexts.get(i));
        }
      }
    }
    for (IndexSegment segment : selectedSegments) {
      selectedSegmentContexts.add(segmentToContext.get(segment));
    }

    LOGGER.debug("Matched {} segments after pruning", selectedSegments.size());
    return new SelectedSegmentsInfo(allSegments, numTotalDocs, prunerStats, numTotalSegments, selectedSegments.size(),
        selectedSegmentContexts);
  }

  @Override
  public List<IndexSegment> getIndexSegments() {
    return _tableExecutionInfos.stream().flatMap(tableExecutionInfo -> tableExecutionInfo.getIndexSegments().stream())
        .collect(Collectors.toList());
  }

  @Nullable
  @Override
  public Map<IndexSegment, SegmentContext> getProvidedSegmentContexts() {
    Map<IndexSegment, SegmentContext> providedSegmentContexts = null;
    for (SingleTableExecutionInfo tableExecutionInfo : _tableExecutionInfos) {
      Map<IndexSegment, SegmentContext> segmentContexts = tableExecutionInfo.getProvidedSegmentContexts();
      if (segmentContexts != null) {
        if (providedSegmentContexts == null) {
          // First time we are getting segment contexts
          providedSegmentContexts = new HashMap<>();
        }
        providedSegmentContexts.putAll(segmentContexts);
      }
    }

    return providedSegmentContexts;
  }

  @Override
  public List<String> getSegmentsToQuery() {
    return _tableExecutionInfos.stream().flatMap(tableExecutionInfo -> tableExecutionInfo.getSegmentsToQuery().stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getOptionalSegments() {
    return _tableExecutionInfos.stream()
        .flatMap(tableExecutionInfo -> tableExecutionInfo.getOptionalSegments().stream()).collect(Collectors.toList());
  }

  @Override
  public List<SegmentDataManager> getSegmentDataManagers() {
    return _tableExecutionInfos.stream()
        .flatMap(tableExecutionInfo -> tableExecutionInfo.getSegmentDataManagers().stream())
        .collect(Collectors.toList());
  }

  @Override
  public void releaseSegmentDataManagers() {
    for (SingleTableExecutionInfo tableExecutionInfo : _tableExecutionInfos) {
      tableExecutionInfo.releaseSegmentDataManagers();
    }
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    return _tableExecutionInfos.stream()
        .flatMap(tableExecutionInfo -> tableExecutionInfo.getSegmentContexts(selectedSegments, queryOptions).stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getNotAcquiredSegments() {
    return _tableExecutionInfos.stream()
        .flatMap(tableExecutionInfo -> tableExecutionInfo.getNotAcquiredSegments().stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getMissingSegments() {
    return _tableExecutionInfos.stream().flatMap(tableExecutionInfo -> tableExecutionInfo.getMissingSegments().stream())
        .collect(Collectors.toList());
  }

  @Override
  public ConsumingSegmentsInfo getConsumingSegmentsInfo() {
    ConsumingSegmentsInfo consumingSegmentsInfo = new ConsumingSegmentsInfo();
    for (SingleTableExecutionInfo tableExecutionInfo : _tableExecutionInfos) {
      consumingSegmentsInfo.aggregate(tableExecutionInfo.getConsumingSegmentsInfo());
    }
    return consumingSegmentsInfo;
  }

  @Override
  public int getNumSegmentsAcquired() {
    return _tableExecutionInfos.stream().mapToInt(SingleTableExecutionInfo::getNumSegmentsAcquired).sum();
  }
}
