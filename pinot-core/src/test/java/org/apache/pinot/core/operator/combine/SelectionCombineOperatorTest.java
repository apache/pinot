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
package org.apache.pinot.core.operator.combine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Test for {@link SelectionOnlyCombineOperator} and {@link SelectionOrderByCombineOperator}.
 */
public class SelectionCombineOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SelectionCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  // Create (MAX_NUM_THREADS_PER_QUERY * 2) segments so that each thread needs to process 2 segments
  private static final int NUM_SEGMENTS = QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * 2;
  private static final int NUM_CONSUMING_SEGMENTS = NUM_SEGMENTS / 2;
  private static final String REALTIME_TABLE_NAME = RAW_TABLE_NAME + "_REALTIME";
  private static final int NUM_RECORDS_PER_SEGMENT = 100;

  private static final String INT_COLUMN = "intColumn";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT).build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS / 2; i++) {
      _indexSegments.add(createOfflineSegment(i));
    }

    for (int i = NUM_CONSUMING_SEGMENTS; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(createRealtimeSegment(i));
    }
  }

  private IndexSegment createRealtimeSegment(int index)
      throws Exception {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);

    String segmentName = SEGMENT_NAME_PREFIX + index;

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(REALTIME_TABLE_NAME).setSegmentName(segmentName).setSchema(SCHEMA).setCapacity(100000)
        .setAvgNumMultiValues(2).setSegmentZKMetadata(new SegmentZKMetadata(segmentName))
        .setMemoryManager(new DirectMemoryManager(segmentName)).setStatsHistory(statsHistory).setAggregateMetrics(false)
        .setNullHandlingEnabled(true).setIngestionAggregationConfigs(Collections.emptyList()).build();
    MutableSegment mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
    int baseValue = index * NUM_RECORDS_PER_SEGMENT / 2;
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, baseValue + i);
      mutableSegmentImpl.index(record, null);
    }
    return mutableSegmentImpl;
  }

  private IndexSegment createOfflineSegment(int index)
      throws Exception {
    int baseValue = index * NUM_RECORDS_PER_SEGMENT / 2;
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, baseValue + i);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    String segmentName = SEGMENT_NAME_PREFIX + index;
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), ReadMode.mmap);
  }

  @Test
  public void testSelectionLimit0() {
    SelectionResultsBlock combineResult = getCombineResult("SELECT * FROM testTable LIMIT 0");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    assertTrue(combineResult.getRows().isEmpty());
    assertEquals(combineResult.getNumDocsScanned(), 0);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), 0);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumSegmentsMatched(), 0);
    assertEquals(combineResult.getNumConsumingSegmentsProcessed(), 0);
    assertEquals(combineResult.getNumConsumingSegmentsMatched(), 0);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
  }

  @Test
  public void testSelectionOnly() {
    SelectionResultsBlock combineResult = getCombineResult("SELECT * FROM testTable");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    assertEquals(combineResult.getRows().size(), 10);
    // Should early-terminate after processing the result of the first segment. Each thread should process at most 1
    // segment.
    long numDocsScanned = combineResult.getNumDocsScanned();
    assertTrue(numDocsScanned >= 10 && numDocsScanned <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * 10);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsProcessed(), NUM_CONSUMING_SEGMENTS);
    int numSegmentsMatched = combineResult.getNumSegmentsMatched();
    assertTrue(numSegmentsMatched >= 1 && numSegmentsMatched <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY);
    // The check below depends on the order of segment processing. When segments# <= 10 (the value of
    // CombinePlanNode.TARGET_NUM_PLANS_PER_THREAD to be specific), the segments are processed in the order as they
    // are prepared, which is OFFLINE segments followed by RT segments and this case makes the value here equal to 0.
    // But when segments# > 10, the segments are processed in a different order and some RT segments can be processed
    // ahead of the other OFFLINE segments, but no more than TaskUtils.MAX_NUM_THREADS_PER_QUERY for sure
    // as each thread only processes one segment.
    int numConsumingSegmentsMatched = combineResult.getNumConsumingSegmentsMatched();
    if (NUM_SEGMENTS <= 10) {
      assertEquals(numConsumingSegmentsMatched, 0, "numSegments: " + NUM_SEGMENTS);
    } else {
      assertTrue(numConsumingSegmentsMatched >= 0
          && numConsumingSegmentsMatched <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY, String
          .format("numConsumingSegmentsMatched: %d, maxThreadsPerQuery: %d, numSegments: %d",
              combineResult.getNumConsumingSegmentsMatched(), QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY,
              NUM_SEGMENTS));
    }
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);

    combineResult = getCombineResult("SELECT * FROM testTable LIMIT 10000");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    assertEquals(combineResult.getRows().size(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    // Should not early-terminate
    numDocsScanned = combineResult.getNumDocsScanned();
    assertEquals(numDocsScanned, NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumSegmentsMatched(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsProcessed(), NUM_CONSUMING_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsMatched(), NUM_CONSUMING_SEGMENTS);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
  }

  @Test
  public void testSelectionOrderBy() {
    SelectionResultsBlock combineResult = getCombineResult("SELECT * FROM testTable ORDER BY intColumn");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    List<Object[]> rows = combineResult.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      assertEquals((int) rows.get(i)[0], i);
    }
    // Should early-terminate after processing the result of the first segment. Each thread should process at most 1
    // segment.
    long numDocsScanned = combineResult.getNumDocsScanned();
    // Need to scan 10 documents per segment because 'intColumn' is sorted
    assertTrue(numDocsScanned >= 10 && numDocsScanned <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * 10);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsProcessed(), NUM_CONSUMING_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsMatched(), 0);
    int numSegmentsMatched = combineResult.getNumSegmentsMatched();
    assertTrue(numSegmentsMatched >= 1 && numSegmentsMatched <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);

    combineResult = getCombineResult("SELECT * FROM testTable ORDER BY intColumn DESC");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    rows = combineResult.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 10);
    int expectedValue = NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT / 2 + 49;
    for (int i = 0; i < 10; i++) {
      assertEquals((int) rows.get(i)[0], expectedValue - i);
    }
    // Should early-terminate after processing the result of the first segment. Each thread should process at most 1
    // segment.
    numDocsScanned = combineResult.getNumDocsScanned();
    assertTrue(numDocsScanned >= NUM_RECORDS_PER_SEGMENT
        && numDocsScanned <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsProcessed(), NUM_CONSUMING_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsMatched(), NUM_CONSUMING_SEGMENTS);
    numSegmentsMatched = combineResult.getNumSegmentsMatched();
    assertTrue(numSegmentsMatched >= 1 && numSegmentsMatched <= QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);

    combineResult = getCombineResult("SELECT * FROM testTable ORDER BY intColumn DESC LIMIT 10000");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    rows = combineResult.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    // Should not early-terminate
    numDocsScanned = combineResult.getNumDocsScanned();
    assertEquals(numDocsScanned, NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumSegmentsMatched(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsProcessed(), NUM_CONSUMING_SEGMENTS);
    assertEquals(combineResult.getNumConsumingSegmentsMatched(), NUM_CONSUMING_SEGMENTS);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
  }

  private SelectionResultsBlock getCombineResult(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(indexSegment, queryContext));
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
    return (SelectionResultsBlock) combinePlanNode.run().nextBlock();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
