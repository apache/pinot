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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SortedGroupByCombineOperatorsTest {
  /**
   * Test for {@link SortedGroupByCombineOperator} and {@link SequentialSortedGroupByCombineOperator}.
   */
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SortedGroupByCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  // Create (MAX_NUM_THREADS_PER_QUERY * 2) segments so that each thread needs to process 2 segments
  private static final int NUM_SEGMENTS = QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * 2;
  private static final int NUM_RECORDS_PER_SEGMENT = 100;
  private static final int NUM_RECORDS_PER_SMALL_SEGMENT = 50;

  private static final String INT_COLUMN = "intColumn";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT).build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

  private List<IndexSegment> _indexSegments;

  // ----
  // Pairwise combine
  @Test
  public void testSafeTrimPairWiseCombineLimit0() {
    GroupByResultsBlock combineResult = getPairWiseCombineResult(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 0");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 0);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
  }

  @Test
  public void testSafeTrimPairWiseOneSegmentOnly() {
    GroupByResultsBlock combineResult = getPairWiseCombineResultSingleBlock(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), 1);
    for (int i = 0; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
  }

  @Test
  public void testSafeTrimPairWiseCombine() {
    GroupByResultsBlock combineResult = getPairWiseCombineResult(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    for (int i = 0; i < 50; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
    for (int i = 50; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 2L);
    }
  }

  @Test
  public void testSafeTrimPairWiseEmptyCombine() {
    GroupByResultsBlock combineResult = getPairWiseCombineResult(
        "SELECT intColumn, COUNT(*) FROM testTable WHERE intColumn < 0 "
            + "GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 0);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
  }

  @Test
  public void testSafeTrimPairWiseCombineServerReturnFinal() {
    GroupByResultsBlock combineResult = getPairWiseCombineResultServerReturnFinal(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    for (int i = 0; i < 50; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
    for (int i = 50; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 2L);
    }
  }

  @Test
  public void testSafeTrimPairWiseLargeSmallSegments() {
    GroupByResultsBlock combineResult = getPairWiseCombineResultLargeSmallBlocks(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), 2);
    for (int i = 0; i < 50; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
    for (int i = 50; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 2L);
    }
  }

  // ----
  // Sequential combine
  @Test
  public void testSafeTrimSequentialCombineLimit0() {
    GroupByResultsBlock combineResult = getSequentialCombineResult(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 0");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 0);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
  }

  @Test
  public void testSafeTrimSequentialCombine() {
    GroupByResultsBlock combineResult = getSequentialCombineResult(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    for (int i = 0; i < 50; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
    for (int i = 50; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 2L);
    }
  }

  @Test
  public void testSafeTrimSequentialOneSegmentOnly() {
    GroupByResultsBlock combineResult = getSequentialCombineResultSingleBlock(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), 1);
    for (int i = 0; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
  }

  @Test
  public void testSafeTrimSequentialLargeSmallSegments() {
    GroupByResultsBlock combineResult = getSequentialCombineResultLargeSmallBlocks(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), 2);
    for (int i = 0; i < 50; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
    for (int i = 50; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 2L);
    }
  }

  @Test
  public void testSafeSequentialEmptyCombine() {
    GroupByResultsBlock combineResult = getSequentialCombineResult(
        "SELECT intColumn, COUNT(*) FROM testTable WHERE intColumn < 0 "
            + "GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 0);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
  }

  @Test
  public void testSafeTrimSequentialCombineServerReturnFinal() {
    GroupByResultsBlock combineResult = getSequentialCombineResultServerReturnFinal(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn ORDER BY intColumn LIMIT 100");
    assertEquals(combineResult.getDataSchema(),
        new DataSchema(new String[]{INT_COLUMN, "count(*)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG}));
    assertEquals(combineResult.getRows().size(), 100);
    assertEquals(combineResult.getNumSegmentsProcessed(), NUM_SEGMENTS);
    for (int i = 0; i < 50; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 1L);
    }
    for (int i = 50; i < 100; i++) {
      Object[] row = combineResult.getRows().get(i);
      assertEquals(row[0], i);
      assertEquals(row[1], 2L);
    }
  }

  // ----
  // Utils
  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(createOfflineSegment(i));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
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

  private IndexSegment createOfflineSmallSegment(int index)
      throws Exception {
    int baseValue = index * NUM_RECORDS_PER_SEGMENT / 2;
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SMALL_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SMALL_SEGMENT; i++) {
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

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getPairWiseCombineResult(String query) {
    // ensure pair-wise execution
    query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; " + query;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
    BaseCombineOperator combineOperator = combinePlanNode.run();
    Preconditions.checkState(combineOperator instanceof SortedGroupByCombineOperator);
    return (GroupByResultsBlock) combineOperator.nextBlock();
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getSequentialCombineResult(String query) {
    // ensure pair-wise execution
    query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000000; " + query;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
    BaseCombineOperator combineOperator = combinePlanNode.run();
    Preconditions.checkState(combineOperator instanceof SequentialSortedGroupByCombineOperator);
    return (GroupByResultsBlock) combineOperator.nextBlock();
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getPairWiseCombineResultSingleBlock(String query) {
    // ensure pair-wise execution
    try {
      query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; " + query;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
      IndexSegment indexSegment = createOfflineSegment(0);
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
      queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
      CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
      BaseCombineOperator combineOperator = combinePlanNode.run();
      Preconditions.checkState(combineOperator instanceof SortedGroupByCombineOperator);
      return (GroupByResultsBlock) combineOperator.nextBlock();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getSequentialCombineResultSingleBlock(String query) {
    // ensure pair-wise execution
    try {
      query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000000; " + query;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
      IndexSegment indexSegment = createOfflineSegment(0);
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
      queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
      CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
      BaseCombineOperator combineOperator = combinePlanNode.run();
      Preconditions.checkState(combineOperator instanceof SequentialSortedGroupByCombineOperator);
      return (GroupByResultsBlock) combineOperator.nextBlock();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getPairWiseCombineResultLargeSmallBlocks(String query) {
    // ensure pair-wise execution
    try {
      query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; " + query;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
      IndexSegment indexSegment1 = createOfflineSegment(0);
      IndexSegment indexSegment2 = createOfflineSmallSegment(1);
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment1), queryContext));
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment2), queryContext));
      queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
      CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
      BaseCombineOperator combineOperator = combinePlanNode.run();
      Preconditions.checkState(combineOperator instanceof SortedGroupByCombineOperator);
      return (GroupByResultsBlock) combineOperator.nextBlock();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getSequentialCombineResultLargeSmallBlocks(String query) {
    // ensure pair-wise execution
    try {
      query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000000; " + query;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
      IndexSegment indexSegment1 = createOfflineSegment(0);
      IndexSegment indexSegment2 = createOfflineSmallSegment(1);
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment1), queryContext));
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment2), queryContext));
      queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
      CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
      BaseCombineOperator combineOperator = combinePlanNode.run();
      Preconditions.checkState(combineOperator instanceof SequentialSortedGroupByCombineOperator);
      return (GroupByResultsBlock) combineOperator.nextBlock();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getPairWiseCombineResultServerReturnFinal(String query) {
    // ensure pair-wise execution
    query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; "
        + "SET serverReturnFinalResult=true; "
        + query;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
    BaseCombineOperator combineOperator = combinePlanNode.run();
    Preconditions.checkState(combineOperator instanceof SortedGroupByCombineOperator);
    return (GroupByResultsBlock) combineOperator.nextBlock();
  }

  @SuppressWarnings({"rawTypes"})
  private GroupByResultsBlock getSequentialCombineResultServerReturnFinal(String query) {
    // ensure pair-wise execution
    query = "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000000; "
        + "SET serverReturnFinalResult=true; "
        + query;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
    }
    queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
    BaseCombineOperator combineOperator = combinePlanNode.run();
    Preconditions.checkState(combineOperator instanceof SequentialSortedGroupByCombineOperator);
    return (GroupByResultsBlock) combineOperator.nextBlock();
  }
}
