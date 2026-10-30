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
package org.apache.pinot.core.plan.maker;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.prefetch.DefaultFetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// With prefetch enabled, [InstancePlanMakerImplV2] runs the fetch lifecycle only for segments for which the
/// [FetchPlanner] plans a fetch context. A segment for which the planner returns null (it needs no fetch) is planned
/// and executed as without prefetch.
public class InstancePlanMakerPrefetchTest {
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_DOCS = 100;
  // Every tenth document matches
  private static final String QUERY = "SELECT COUNT(*), SUM(m) FROM testTable WHERE d = 'v3'";
  // A selection-only query is streamed; the limit keeps every matching row of both segments
  private static final String STREAMING_QUERY = "SELECT m FROM testTable WHERE d = 'v3' LIMIT 1000";
  private static final long MATCHES_PER_SEGMENT = NUM_DOCS / 10;
  private static final double SUM_PER_SEGMENT = 3 + 13 + 23 + 33 + 43 + 53 + 63 + 73 + 83 + 93;

  private File _tempDir;
  private ImmutableSegment _segment;
  private ExecutorService _executorService;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("instance-plan-maker-prefetch-test").toFile();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("d", DataType.STRING)
        .addMetric("m", DataType.LONG)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      GenericRow row = new GenericRow();
      row.putValue("d", "v" + (i % 10));
      row.putValue("m", (long) i);
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(_tempDir.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segment =
        ImmutableSegmentLoader.load(new File(_tempDir, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, schema));
    _executorService = QueryThreadContext.contextAwareExecutorService(Executors.newFixedThreadPool(2));
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdownNow();
    _segment.destroy();
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testPrefetchRunsFetchLifecycleOnlyForPlannedSegments()
      throws Exception {
    IndexSegment local = spy(_segment);
    IndexSegment remote = spy(_segment);

    assertResult(execute(plannerSkipping(Set.of(local)), List.of(local, remote), true), 2);

    verifyFetchLifecycle(remote);
    verifyNoFetch(local);
  }

  /// Streaming selection has its own start/stop and per-segment fetch lifecycle; only the remote segment gets it.
  @Test
  public void testStreamingSelectionRunsFetchLifecycleOnlyForPlannedSegments()
      throws Exception {
    IndexSegment local = spy(_segment);
    IndexSegment remote = spy(_segment);

    List<Long> streamedValues = executeStreaming(plannerSkipping(Set.of(local)), List.of(local, remote));

    // Each segment streams every matching value once
    List<Long> expectedValues = new ArrayList<>();
    for (long value = 3; value < NUM_DOCS; value += 10) {
      expectedValues.add(value);
      expectedValues.add(value);
    }
    assertEquals(streamedValues, expectedValues);
    verifyFetchLifecycle(remote);
    verifyNoFetch(local);
  }

  @Test
  public void testPrefetchWithoutPlannedSegmentsRunsNoFetchLifecycle()
      throws Exception {
    IndexSegment first = spy(_segment);
    IndexSegment second = spy(_segment);

    assertResult(execute(plannerSkipping(Set.of(first, second)), List.of(first, second), true), 2);

    verifyNoFetch(first);
    verifyNoFetch(second);
  }

  @Test
  public void testDisabledPrefetchRunsNoFetchLifecycle()
      throws Exception {
    IndexSegment segment = spy(_segment);

    assertResult(execute(plannerSkipping(Set.of()), List.of(segment), false), 1);

    verifyNoFetch(segment);
  }

  /// The default planner never returns null, so every segment keeps the fetch lifecycle as before.
  @Test
  public void testDefaultPlannerRunsFetchLifecycleForEverySegment()
      throws Exception {
    IndexSegment first = spy(_segment);
    IndexSegment second = spy(_segment);

    assertResult(execute(new DefaultFetchPlanner(), List.of(first, second), true), 2);

    for (IndexSegment segment : List.of(first, second)) {
      verify(segment).prefetch(any());
      verify(segment).acquire(any());
      verify(segment, atLeastOnce()).release(any());
    }
  }

  /// A planner that plans with the default planner, except for the given segments, which need no fetch.
  private static FetchPlanner plannerSkipping(Set<IndexSegment> segmentsWithoutFetch) {
    FetchPlanner defaultPlanner = new DefaultFetchPlanner();
    return new FetchPlanner() {
      @Override
      public FetchContext planFetchForPruning(IndexSegment indexSegment, QueryContext queryContext) {
        return segmentsWithoutFetch.contains(indexSegment) ? null
            : defaultPlanner.planFetchForPruning(indexSegment, queryContext);
      }

      @Override
      public FetchContext planFetchForProcessing(IndexSegment indexSegment, QueryContext queryContext) {
        return segmentsWithoutFetch.contains(indexSegment) ? null
            : defaultPlanner.planFetchForProcessing(indexSegment, queryContext);
      }
    };
  }

  private DataTable execute(FetchPlanner fetchPlanner, List<IndexSegment> segments, boolean enablePrefetch)
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(QUERY);
    queryContext.setEndTimeMs(System.currentTimeMillis() + 60_000);
    queryContext.setEnablePrefetch(enablePrefetch);
    List<SegmentContext> segmentContexts = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      InstanceResponseBlock responseBlock = planMakerWith(fetchPlanner)
          .makeInstancePlan(segmentContexts, queryContext, _executorService).execute();
      assertTrue(responseBlock.getExceptions().isEmpty(), "Query failed: " + responseBlock.getExceptions());
      return responseBlock.toDataTable();
    }
  }

  /// Runs the streaming selection with prefetch enabled, and returns the streamed values of `m` in ascending order.
  private List<Long> executeStreaming(FetchPlanner fetchPlanner, List<IndexSegment> segments)
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(STREAMING_QUERY);
    queryContext.setEndTimeMs(System.currentTimeMillis() + 60_000);
    queryContext.setEnablePrefetch(true);
    List<SegmentContext> segmentContexts = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    // The streamer is invoked on the response thread, not by the combine workers
    List<Long> streamedValues = new ArrayList<>();
    ResultsBlockStreamer streamer = block -> {
      for (Object[] row : ((SelectionResultsBlock) block).getRows()) {
        streamedValues.add(((Number) row[0]).longValue());
      }
    };
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      InstanceResponseBlock responseBlock = planMakerWith(fetchPlanner)
          .makeStreamingInstancePlan(segmentContexts, queryContext, _executorService, streamer).execute();
      assertTrue(responseBlock.getExceptions().isEmpty(), "Query failed: " + responseBlock.getExceptions());
    }
    Collections.sort(streamedValues);
    return streamedValues;
  }

  /// The plan maker takes the registered planner at construction.
  private static InstancePlanMakerImplV2 planMakerWith(FetchPlanner fetchPlanner) {
    try (MockedStatic<FetchPlannerRegistry> registry = mockStatic(FetchPlannerRegistry.class)) {
      registry.when(FetchPlannerRegistry::getPlanner).thenReturn(fetchPlanner);
      return new InstancePlanMakerImplV2();
    }
  }

  private static void assertResult(DataTable dataTable, int numSegments) {
    assertEquals(dataTable.getNumberOfRows(), 1);
    assertEquals(dataTable.getLong(0, 0), MATCHES_PER_SEGMENT * numSegments);
    assertEquals(dataTable.getDouble(0, 1), SUM_PER_SEGMENT * numSegments);
  }

  /// The segment is prefetched, acquired and released with one fetch context planned for the filter column.
  private static void verifyFetchLifecycle(IndexSegment segment) {
    ArgumentCaptor<FetchContext> fetchContext = ArgumentCaptor.forClass(FetchContext.class);
    verify(segment).prefetch(fetchContext.capture());
    verify(segment).acquire(fetchContext.getValue());
    verify(segment, atLeastOnce()).release(fetchContext.getValue());
    assertTrue(fetchContext.getValue().getColumnToIndexList().containsKey("d"));
  }

  private static void verifyNoFetch(IndexSegment segment) {
    verify(segment, never()).prefetch(any());
    verify(segment, never()).acquire(any());
    verify(segment, never()).release(any());
  }
}
