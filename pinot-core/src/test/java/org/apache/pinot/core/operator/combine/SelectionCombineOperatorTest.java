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
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.CommonConstants.Server;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Test for {@link SelectionOnlyCombineOperator} and {@link SelectionOrderByCombineOperator}.
 */
public class SelectionCombineOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SelectionCombineEarlyTerminationTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  // Create (MAX_NUM_THREADS_PER_QUERY * 2) segments so that each thread needs to process 2 segments
  private static final int NUM_SEGMENTS = CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY * 2;
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
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(createSegment(i));
    }
  }

  private IndexSegment createSegment(int index)
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
    IntermediateResultsBlock combineResult = getCombineResult("SELECT * FROM testTable LIMIT 0");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    assertNotNull(combineResult.getSelectionResult());
    assertTrue(combineResult.getSelectionResult().isEmpty());
    assertEquals(combineResult.getNumDocsScanned(), 0);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), 0);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumSegmentsMatched(), 0);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
  }

  @Test
  public void testSelectionOnly() {
    IntermediateResultsBlock combineResult = getCombineResult("SELECT * FROM testTable");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    assertNotNull(combineResult.getSelectionResult());
    assertEquals(combineResult.getSelectionResult().size(), 10);
    // Should early-terminate after processing the result of the first segment. Each thread should process at most 1
    // segment.
    long numDocsScanned = combineResult.getNumDocsScanned();
    assertTrue(numDocsScanned >= 10 && numDocsScanned <= CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY * 10);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    int numSegmentsMatched = combineResult.getNumSegmentsMatched();
    assertTrue(numSegmentsMatched >= 1 && numSegmentsMatched <= CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);

    combineResult = getCombineResult("SELECT * FROM testTable LIMIT 10000");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    assertNotNull(combineResult.getSelectionResult());
    assertEquals(combineResult.getSelectionResult().size(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    // Should not early-terminate
    numDocsScanned = combineResult.getNumDocsScanned();
    assertEquals(numDocsScanned, NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumSegmentsMatched(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
  }

  @Test
  public void testSelectionOrderBy() {
    IntermediateResultsBlock combineResult = getCombineResult("SELECT * FROM testTable ORDER BY intColumn");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) combineResult.getSelectionResult();
    assertNotNull(selectionResult);
    assertEquals(selectionResult.size(), 10);
    int expectedValue = 9;
    while (!selectionResult.isEmpty()) {
      assertEquals((int) selectionResult.poll()[0], expectedValue--);
    }
    // Should early-terminate after processing the result of the first segment. Each thread should process at most 1
    // segment.
    long numDocsScanned = combineResult.getNumDocsScanned();
    assertTrue(numDocsScanned >= NUM_RECORDS_PER_SEGMENT
        && numDocsScanned <= CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    int numSegmentsMatched = combineResult.getNumSegmentsMatched();
    assertTrue(numSegmentsMatched >= 1 && numSegmentsMatched <= CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);

    combineResult = getCombineResult("SELECT * FROM testTable ORDER BY intColumn DESC");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    selectionResult = (PriorityQueue<Object[]>) combineResult.getSelectionResult();
    assertNotNull(selectionResult);
    assertEquals(selectionResult.size(), 10);
    expectedValue = NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT / 2 + 40;
    while (!selectionResult.isEmpty()) {
      assertEquals((int) selectionResult.poll()[0], expectedValue++);
    }
    // Should early-terminate after processing the result of the first segment. Each thread should process at most 1
    // segment.
    numDocsScanned = combineResult.getNumDocsScanned();
    assertTrue(numDocsScanned >= NUM_RECORDS_PER_SEGMENT
        && numDocsScanned <= CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    numSegmentsMatched = combineResult.getNumSegmentsMatched();
    assertTrue(numSegmentsMatched >= 1 && numSegmentsMatched <= CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);

    combineResult = getCombineResult("SELECT * FROM testTable ORDER BY intColumn DESC LIMIT 10000");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    selectionResult = (PriorityQueue<Object[]>) combineResult.getSelectionResult();
    assertNotNull(selectionResult);
    assertEquals(selectionResult.size(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    // Should not early-terminate
    numDocsScanned = combineResult.getNumDocsScanned();
    assertEquals(numDocsScanned, NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
    assertEquals(combineResult.getNumEntriesScannedInFilter(), 0);
    assertEquals(combineResult.getNumEntriesScannedPostFilter(), numDocsScanned);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumSegmentsMatched(), NUM_SEGMENTS);
    assertEquals(combineResult.getNumTotalDocs(), NUM_SEGMENTS * NUM_RECORDS_PER_SEGMENT);
  }

  private IntermediateResultsBlock getCombineResult(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromPQL(query);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(indexSegment, queryContext));
    }
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR,
        System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS,
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, null,
        InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD);
    return combinePlanNode.run().nextBlock();
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
