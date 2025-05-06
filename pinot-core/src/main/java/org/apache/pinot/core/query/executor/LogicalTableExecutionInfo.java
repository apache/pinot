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
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TableSegmentsContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogicalTableExecutionInfo implements TableExecutionInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableExecutionInfo.class);

  public static LogicalTableExecutionInfo create(InstanceDataManager instanceDataManager,
      ServerQueryRequest queryRequest, QueryContext queryContext)
      throws TableNotFoundException {
    List<TableSegmentsContext> tableSegmentsContextList = queryRequest.getTableSegmentsContexts();
    Objects.requireNonNull(tableSegmentsContextList);
    List<SingleTableExecutionInfo> tableExecutionInfos = new ArrayList<>(tableSegmentsContextList.size());
    for (TableSegmentsContext tableSegmentsContext : tableSegmentsContextList) {
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
  public boolean hasRealtime() {
    return _tableExecutionInfos.stream()
        .anyMatch(tableExecutionInfo -> tableExecutionInfo.getTableDataManager() instanceof RealtimeTableDataManager);
  }

  @Override
  public SelectedSegmentsInfo getSelectedSegmentsInfo(QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, SegmentPrunerService segmentPrunerService) {
    SelectedSegmentsInfo aggregatedSelectedSegmentsInfo = new SelectedSegmentsInfo();

    for (SingleTableExecutionInfo tableExecutionInfo : _tableExecutionInfos) {
      SelectedSegmentsInfo selectedSegmentsInfo =
          tableExecutionInfo.getSelectedSegmentsInfo(queryContext, timerContext, executorService, segmentPrunerService);
      aggregatedSelectedSegmentsInfo.aggregate(selectedSegmentsInfo);
    }

    LOGGER.debug("Matched {} segments after pruning", aggregatedSelectedSegmentsInfo._numSelectedSegments);
    return aggregatedSelectedSegmentsInfo;
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
