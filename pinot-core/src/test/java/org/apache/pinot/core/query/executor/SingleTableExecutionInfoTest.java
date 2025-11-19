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
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.pruner.SegmentPrunerStatistics;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class SingleTableExecutionInfoTest {
  private static final String TABLE_NAME = "testTable";

  @Test
  public void testDistinctWithoutOrderByPrioritizesLatestSegments()
      throws Exception {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT DISTINCT col FROM testTable LIMIT 5");
    SingleTableExecutionInfo executionInfo = createExecutionInfo(List.of(
        createSegmentDataManager("segment_100", 100L),
        createSegmentDataManager("segment_300", 300L),
        createSegmentDataManager("segment_200", 200L)
    ), queryContext);

    List<String> selectedSegments = getSelectedSegmentNames(executionInfo, queryContext);

    assertEquals(selectedSegments, List.of("segment_300", "segment_200", "segment_100"));
  }

  @Test
  public void testDistinctOrderByKeepsOriginalOrderWithoutEarlyTerminationOptions()
      throws Exception {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT DISTINCT col FROM testTable ORDER BY col LIMIT 5");
    SingleTableExecutionInfo executionInfo = createExecutionInfo(List.of(
        createSegmentDataManager("segment_100", 100L),
        createSegmentDataManager("segment_300", 300L),
        createSegmentDataManager("segment_200", 200L)
    ), queryContext);

    List<String> selectedSegments = getSelectedSegmentNames(executionInfo, queryContext);

    assertEquals(selectedSegments, List.of("segment_100", "segment_300", "segment_200"));
  }

  @Test
  public void testDistinctOrderByWithEarlyTerminationOptionsPrioritizesLatestSegments()
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SET \"numRowsWithoutChangeInDistinct\" = 10; "
            + "SELECT DISTINCT col FROM testTable ORDER BY col LIMIT 5");
    SingleTableExecutionInfo executionInfo = createExecutionInfo(List.of(
        createSegmentDataManager("segment_100", 100L),
        createSegmentDataManager("segment_300", 300L),
        createSegmentDataManager("segment_200", 200L)
    ), queryContext);

    List<String> selectedSegments = getSelectedSegmentNames(executionInfo, queryContext);

    assertEquals(selectedSegments, List.of("segment_300", "segment_200", "segment_100"));
  }

  private static SingleTableExecutionInfo createExecutionInfo(List<SegmentDataManager> segmentDataManagers,
      QueryContext queryContext)
      throws TableNotFoundException {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    List<String> segmentNames = segmentDataManagers.stream().map(SegmentDataManager::getSegmentName)
        .collect(Collectors.toList());

    when(instanceDataManager.getTableDataManager(TABLE_NAME)).thenReturn(tableDataManager);
    when(tableDataManager.isUpsertEnabled()).thenReturn(false);
    when(tableDataManager.acquireSegments(eq(segmentNames), isNull(), anyList())).thenReturn(segmentDataManagers);
    when(tableDataManager.getSegmentContexts(anyList(), any())).thenAnswer(invocation -> {
      List<IndexSegment> selectedSegments = invocation.getArgument(0);
      return selectedSegments.stream().map(SegmentContext::new).collect(Collectors.toList());
    });

    return SingleTableExecutionInfo.create(instanceDataManager, TABLE_NAME, segmentNames, null, queryContext);
  }

  private static List<String> getSelectedSegmentNames(SingleTableExecutionInfo executionInfo,
      QueryContext queryContext) {
    SegmentPrunerService segmentPrunerService = mock(SegmentPrunerService.class);
    doAnswer(invocation -> invocation.getArgument(0)).when(segmentPrunerService)
        .prune(anyList(), eq(queryContext), any(SegmentPrunerStatistics.class), any());
    TableExecutionInfo.SelectedSegmentsInfo selectedSegmentsInfo =
        executionInfo.getSelectedSegmentsInfo(queryContext,
            new TimerContext(TABLE_NAME, mock(ServerMetrics.class), System.currentTimeMillis()),
            mock(ExecutorService.class), segmentPrunerService);
    return selectedSegmentsInfo.getSelectedSegmentContexts().stream()
        .map(segmentContext -> segmentContext.getIndexSegment().getSegmentName())
        .collect(Collectors.toList());
  }

  private static SegmentDataManager createSegmentDataManager(String segmentName, long endTimeMs) {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getEndTime()).thenReturn(endTimeMs);
    when(segmentMetadata.getStartTime()).thenReturn(endTimeMs);
    when(segmentMetadata.getLastIndexedTimestamp()).thenReturn(0L);
    when(segmentMetadata.getIndexCreationTime()).thenReturn(0L);
    when(segmentMetadata.getTotalDocs()).thenReturn(1);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentName()).thenReturn(segmentName);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);

    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    when(segmentDataManager.getSegment()).thenReturn(indexSegment);
    return segmentDataManager;
  }
}
