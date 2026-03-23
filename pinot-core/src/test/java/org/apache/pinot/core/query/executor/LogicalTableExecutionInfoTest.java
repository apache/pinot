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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.config.SegmentPrunerConfig;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class LogicalTableExecutionInfoTest {

  private ExecutorService _executorService;
  private SegmentPrunerService _segmentPrunerService;
  private TimerContext _timerContext;

  @BeforeClass
  public void setUp() {
    _executorService = Executors.newFixedThreadPool(2);
    PinotConfiguration prunerConf = new PinotConfiguration();
    prunerConf.setProperty(Server.CLASS, Server.DEFAULT_QUERY_EXECUTOR_PRUNER_CLASS);
    _segmentPrunerService = new SegmentPrunerService(new SegmentPrunerConfig(prunerConf));
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    _timerContext = new TimerContext("logicalTable_OFFLINE", serverMetrics, System.currentTimeMillis());
  }

  @AfterClass
  public void tearDown() {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }

  /**
   * Verifies that for a logical table, all segments from all physical tables are collected and
   * prune is invoked once (cross-table). With LIMIT 5 and segments of 10 docs each, only 1 segment
   * should be selected across the whole logical table.
   */
  @Test
  public void testGetSelectedSegmentsInfoPruneOnceAcrossTables() {
    // 3 tables, 2 segments each = 6 segments total; each segment has 10 docs
    List<SingleTableExecutionInfo> tableExecutionInfos = new ArrayList<>();
    List<IndexSegment> allSegments = new ArrayList<>();
    for (int t = 0; t < 3; t++) {
      List<IndexSegment> tableSegments = new ArrayList<>();
      for (int s = 0; s < 2; s++) {
        IndexSegment seg = mockIndexSegment(10);
        tableSegments.add(seg);
        allSegments.add(seg);
      }
      SingleTableExecutionInfo tableInfo = mockSingleTableExecutionInfo(tableSegments, null);
      tableExecutionInfos.add(tableInfo);
    }

    LogicalTableExecutionInfo logicalTableExecutionInfo = new LogicalTableExecutionInfo(tableExecutionInfos);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM logicalTable LIMIT 5");

    TableExecutionInfo.SelectedSegmentsInfo selectedSegmentsInfo =
        logicalTableExecutionInfo.getSelectedSegmentsInfo(queryContext, _timerContext, _executorService,
            _segmentPrunerService);

    // Cross-table prune: total 6 segments, but LIMIT 5 needs only 1 segment (10 docs >= 5)
    assertEquals(selectedSegmentsInfo.getNumTotalSegments(), 6);
    assertEquals(selectedSegmentsInfo.getNumTotalDocs(), 60);
    assertEquals(selectedSegmentsInfo.getNumSelectedSegments(), 1,
        "Cross-table prune should select only 1 segment for LIMIT 5 when each segment has 10 docs");
    assertEquals(selectedSegmentsInfo.getIndexSegments(), allSegments);
    assertEquals(selectedSegmentsInfo.getSelectedSegmentContexts().size(), 1);
    assertSame(selectedSegmentsInfo.getSelectedSegmentContexts().get(0).getIndexSegment(),
        selectedSegmentsInfo.getIndexSegments().get(0),
        "Selected context should wrap the first (selected) segment");
  }

  /**
   * Verifies that when filter is constant false, prune is not called and no segments are selected.
   */
  @Test
  public void testGetSelectedSegmentsInfoConstantFalseFilterSkipsPrune() {
    List<IndexSegment> tableSegments = new ArrayList<>();
    tableSegments.add(mockIndexSegment(10));
    SingleTableExecutionInfo tableInfo = mockSingleTableExecutionInfo(tableSegments, null);
    LogicalTableExecutionInfo logicalTableExecutionInfo =
        new LogicalTableExecutionInfo(Collections.singletonList(tableInfo));

    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM t WHERE 1=0 LIMIT 5");

    TableExecutionInfo.SelectedSegmentsInfo selectedSegmentsInfo =
        logicalTableExecutionInfo.getSelectedSegmentsInfo(queryContext, _timerContext, _executorService,
            _segmentPrunerService);

    assertEquals(selectedSegmentsInfo.getNumTotalSegments(), 1);
    assertEquals(selectedSegmentsInfo.getNumSelectedSegments(), 0);
    assertTrue(selectedSegmentsInfo.getSelectedSegmentContexts().isEmpty());
  }

  /**
   * Verifies that when a table has provided segment contexts (e.g. upsert), they are used instead of
   * calling getSegmentContexts.
   */
  @Test
  public void testGetSelectedSegmentsInfoUsesProvidedSegmentContextsWhenPresent() {
    List<IndexSegment> tableSegments = new ArrayList<>();
    IndexSegment seg1 = mockIndexSegment(10);
    IndexSegment seg2 = mockIndexSegment(10);
    tableSegments.add(seg1);
    tableSegments.add(seg2);
    Map<IndexSegment, SegmentContext> providedContexts = new HashMap<>();
    providedContexts.put(seg1, new SegmentContext(seg1));
    providedContexts.put(seg2, new SegmentContext(seg2));
    SingleTableExecutionInfo tableInfo = mockSingleTableExecutionInfo(tableSegments, providedContexts);
    LogicalTableExecutionInfo logicalTableExecutionInfo =
        new LogicalTableExecutionInfo(Collections.singletonList(tableInfo));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM t LIMIT 5");
    TableExecutionInfo.SelectedSegmentsInfo selectedSegmentsInfo =
        logicalTableExecutionInfo.getSelectedSegmentsInfo(queryContext, _timerContext, _executorService,
            _segmentPrunerService);

    assertEquals(selectedSegmentsInfo.getNumSelectedSegments(), 1);
    assertEquals(selectedSegmentsInfo.getSelectedSegmentContexts().size(), 1);
    assertSame(selectedSegmentsInfo.getSelectedSegmentContexts().get(0).getIndexSegment(), seg1);
  }

  /**
   * Verifies that selected segment contexts are in the same order as the pruned segment list.
   */
  @Test
  public void testGetSelectedSegmentsInfoSegmentContextOrderPreserved() {
    // 2 tables, 2 segments each; LIMIT 25 so we need 3 segments (10+10+10 >= 25)
    List<SingleTableExecutionInfo> tableExecutionInfos = new ArrayList<>();
    List<IndexSegment> allSegments = new ArrayList<>();
    for (int t = 0; t < 2; t++) {
      List<IndexSegment> tableSegments = new ArrayList<>();
      for (int s = 0; s < 2; s++) {
        IndexSegment seg = mockIndexSegment(10);
        tableSegments.add(seg);
        allSegments.add(seg);
      }
      tableExecutionInfos.add(mockSingleTableExecutionInfo(tableSegments, null));
    }

    LogicalTableExecutionInfo logicalTableExecutionInfo = new LogicalTableExecutionInfo(tableExecutionInfos);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM logicalTable LIMIT 25");

    TableExecutionInfo.SelectedSegmentsInfo selectedSegmentsInfo =
        logicalTableExecutionInfo.getSelectedSegmentsInfo(queryContext, _timerContext, _executorService,
            _segmentPrunerService);

    assertEquals(selectedSegmentsInfo.getNumSelectedSegments(), 3);
    List<SegmentContext> contexts = selectedSegmentsInfo.getSelectedSegmentContexts();
    assertEquals(contexts.size(), 3);
    // Order must match prune output: first 3 segments (indices 0, 1, 2)
    for (int i = 0; i < 3; i++) {
      assertSame(contexts.get(i).getIndexSegment(), allSegments.get(i));
    }
  }

  private static final String ORDER_BY_COLUMN = "orderByCol";

  /**
   * Verifies that for ORDER BY col DESC LIMIT 5, the pruner selects exactly two segments that have
   * overlapping min/max (order-by column) and prunes the rest. Segments have 10 docs each; two segments
   * overlap in range so both are kept; all others are out of range.
   * <p>The pruner keeps an extra segment only when its max &gt; current min (strict). So Seg2 uses max=101.
   */
  @Test
  public void testGetSelectedSegmentsInfoOrderByLimitTwoSegmentsOverlap() {
    // Seg1: [100, 100] 10 docs - first in DESC order, covers LIMIT 10
    // Seg2: [90, 101] 10 docs - overlaps (max 101 > 100), kept
    // Seg3, Seg4, Seg5: [1, 50] 10 docs each - max 50 < 100, pruned
    IndexSegment seg1 = mockIndexSegmentWithOrderByColumn(ORDER_BY_COLUMN, 100L, 100L, 10);
    IndexSegment seg2 = mockIndexSegmentWithOrderByColumn(ORDER_BY_COLUMN, 90L, 101L, 10);
    IndexSegment seg3 = mockIndexSegmentWithOrderByColumn(ORDER_BY_COLUMN, 1L, 50L, 10);
    IndexSegment seg4 = mockIndexSegmentWithOrderByColumn(ORDER_BY_COLUMN, 1L, 50L, 10);
    IndexSegment seg5 = mockIndexSegmentWithOrderByColumn(ORDER_BY_COLUMN, 1L, 50L, 10);

    List<IndexSegment> allSegments = new ArrayList<>();
    allSegments.add(seg1);
    allSegments.add(seg2);
    allSegments.add(seg3);
    allSegments.add(seg4);
    allSegments.add(seg5);

    SingleTableExecutionInfo tableInfo = mockSingleTableExecutionInfo(allSegments, null);
    LogicalTableExecutionInfo logicalTableExecutionInfo =
        new LogicalTableExecutionInfo(Collections.singletonList(tableInfo));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT * FROM logicalTable ORDER BY " + ORDER_BY_COLUMN + " DESC LIMIT 5");
    queryContext.setSchema(mock(Schema.class));

    TableExecutionInfo.SelectedSegmentsInfo selectedSegmentsInfo =
        logicalTableExecutionInfo.getSelectedSegmentsInfo(queryContext, _timerContext, _executorService,
            _segmentPrunerService);

    assertEquals(selectedSegmentsInfo.getNumTotalSegments(), 5);
    assertEquals(selectedSegmentsInfo.getNumTotalDocs(), 50);
    assertEquals(selectedSegmentsInfo.getNumSelectedSegments(), 2,
        "ORDER BY DESC LIMIT 10 with overlapping ranges should select exactly 2 segments");
    List<SegmentContext> contexts = selectedSegmentsInfo.getSelectedSegmentContexts();
    assertEquals(contexts.size(), 2);
    assertSame(contexts.get(0).getIndexSegment(), seg1, "First selected should be Seg1 (min=100)");
    assertSame(contexts.get(1).getIndexSegment(), seg2, "Second selected should be Seg2 (min=90, max>100 overlaps)");
  }

  private static IndexSegment mockIndexSegment(int totalDocs) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(metadata.getTotalDocs()).thenReturn(totalDocs);
    when(indexSegment.getSegmentMetadata()).thenReturn(metadata);
    return indexSegment;
  }

  /**
   * Mocks an IndexSegment with min/max for the order-by column so SelectionQuerySegmentPruner can prune by range.
   * Used for ORDER BY + LIMIT tests.
   */
  private static IndexSegment mockIndexSegmentWithOrderByColumn(String orderByColumn, Comparable<?> minValue,
      Comparable<?> maxValue, int totalDocs) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq(orderByColumn), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSourceMetadata.getMinValue()).thenReturn(minValue);
    when(dataSourceMetadata.getMaxValue()).thenReturn(maxValue);
    return indexSegment;
  }

  private static SingleTableExecutionInfo mockSingleTableExecutionInfo(List<IndexSegment> indexSegments,
      Map<IndexSegment, SegmentContext> providedContexts) {
    SingleTableExecutionInfo tableInfo = mock(SingleTableExecutionInfo.class);
    when(tableInfo.getIndexSegments()).thenReturn(new ArrayList<>(indexSegments));
    when(tableInfo.getProvidedSegmentContexts()).thenReturn(providedContexts);
    when(tableInfo.getSegmentContexts(any(), any())).thenAnswer(invocation -> {
      List<IndexSegment> segments = invocation.getArgument(0);
      List<SegmentContext> contexts = new ArrayList<>(segments.size());
      for (IndexSegment seg : segments) {
        contexts.add(new SegmentContext(seg));
      }
      return contexts;
    });
    when(tableInfo.getSchema()).thenReturn(mock(Schema.class));
    return tableInfo;
  }
}
