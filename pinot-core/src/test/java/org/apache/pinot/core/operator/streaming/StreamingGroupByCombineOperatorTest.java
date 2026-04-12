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
package org.apache.pinot.core.operator.streaming;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
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
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Test for {@link StreamingGroupByCombineOperator}.
public class StreamingGroupByCombineOperatorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "StreamingGroupByCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  private static final int NUM_SEGMENTS = 4;
  // Each segment has 100 rows with 50 distinct group keys (groupColumn values 0..49)
  // Each group key appears twice per segment with intColumn values that sum to a known total
  private static final int NUM_RECORDS_PER_SEGMENT = 100;
  private static final int NUM_DISTINCT_GROUPS = 50;

  private static final String GROUP_COLUMN = "groupColumn";
  private static final String INT_COLUMN = "intColumn";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(GROUP_COLUMN, FieldSpec.DataType.INT)
      .addMetric(INT_COLUMN, FieldSpec.DataType.LONG)
      .build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(createOfflineSegment(i));
    }
  }

  /// Creates a segment where each of 50 group keys appears twice.
  /// For group key g, the two intColumn values are (g + 1) and (g + 1), so sum per group per segment = 2 * (g + 1).
  private IndexSegment createOfflineSegment(int index)
      throws Exception {
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_DISTINCT_GROUPS; i++) {
      for (int j = 0; j < 2; j++) {
        GenericRow record = new GenericRow();
        record.putValue(GROUP_COLUMN, i);
        record.putValue(INT_COLUMN, (long) (i + 1));
        records.add(record);
      }
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
  public void testStreamingGroupByProducesMultipleBlocks() {
    // Use a very low flush threshold (10 groups) to force multiple flushes with 50 distinct groups
    int flushThreshold = 10;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT groupColumn, SUM(intColumn) FROM testTable GROUP BY groupColumn");
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    List<Operator> operators = buildOperators(queryContext);
    StreamingGroupByCombineOperator combineOperator =
        new StreamingGroupByCombineOperator(operators, queryContext, EXECUTOR, flushThreshold);

    // Collect all blocks
    List<GroupByResultsBlock> dataBlocks = new ArrayList<>();
    combineOperator.start();
    try {
      BaseResultsBlock block = combineOperator.nextBlock();
      while (!(block instanceof MetadataResultsBlock)) {
        assertNull(block.getErrorMessages(), "Expected no errors but got: " + block.getErrorMessages());
        assertTrue(block instanceof GroupByResultsBlock, "Expected GroupByResultsBlock but got: " + block.getClass());
        dataBlocks.add((GroupByResultsBlock) block);
        block = combineOperator.nextBlock();
      }
    } finally {
      combineOperator.stop();
    }

    // With 50 distinct groups and a flush threshold of 10, we should get multiple data blocks
    assertTrue(dataBlocks.size() > 1,
        "Expected multiple data blocks but got " + dataBlocks.size());

    // Merge all partial results to verify correctness
    // Each group key g should have SUM(intColumn) = NUM_SEGMENTS * 2 * (g + 1)
    Map<Integer, Double> groupSums = new HashMap<>();
    for (GroupByResultsBlock dataBlock : dataBlocks) {
      List<Object[]> rows = dataBlock.getRows();
      for (Object[] row : rows) {
        int groupKey = (int) row[0];
        double sum = ((Number) row[1]).doubleValue();
        groupSums.merge(groupKey, sum, Double::sum);
      }
    }

    assertEquals(groupSums.size(), NUM_DISTINCT_GROUPS, "Expected " + NUM_DISTINCT_GROUPS + " distinct groups");
    for (int g = 0; g < NUM_DISTINCT_GROUPS; g++) {
      double expectedSum = NUM_SEGMENTS * 2.0 * (g + 1);
      assertEquals(groupSums.get(g), expectedSum, 0.001,
          "Incorrect sum for group " + g);
    }
  }

  @Test
  public void testHighThresholdProducesSingleBlock() {
    // With a threshold higher than total groups, should produce a single data block
    int flushThreshold = 10000;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT groupColumn, SUM(intColumn) FROM testTable GROUP BY groupColumn");
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    List<Operator> operators = buildOperators(queryContext);
    StreamingGroupByCombineOperator combineOperator =
        new StreamingGroupByCombineOperator(operators, queryContext, EXECUTOR, flushThreshold);

    List<GroupByResultsBlock> dataBlocks = new ArrayList<>();
    combineOperator.start();
    try {
      BaseResultsBlock block = combineOperator.nextBlock();
      while (!(block instanceof MetadataResultsBlock)) {
        assertNull(block.getErrorMessages());
        dataBlocks.add((GroupByResultsBlock) block);
        block = combineOperator.nextBlock();
      }
    } finally {
      combineOperator.stop();
    }

    // Should get exactly 1 data block since threshold > total distinct groups
    assertEquals(dataBlocks.size(), 1, "Expected single data block with high threshold");
    assertEquals(dataBlocks.get(0).getRows().size(), NUM_DISTINCT_GROUPS);
  }

  @Test
  public void testCountAggregation() {
    int flushThreshold = 10;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT groupColumn, COUNT(*) FROM testTable GROUP BY groupColumn");
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    List<Operator> operators = buildOperators(queryContext);
    StreamingGroupByCombineOperator combineOperator =
        new StreamingGroupByCombineOperator(operators, queryContext, EXECUTOR, flushThreshold);

    Map<Integer, Long> groupCounts = new HashMap<>();
    combineOperator.start();
    try {
      BaseResultsBlock block = combineOperator.nextBlock();
      while (!(block instanceof MetadataResultsBlock)) {
        assertNull(block.getErrorMessages());
        for (Object[] row : ((GroupByResultsBlock) block).getRows()) {
          int groupKey = (int) row[0];
          long count = (long) row[1];
          groupCounts.merge(groupKey, count, Long::sum);
        }
        block = combineOperator.nextBlock();
      }
    } finally {
      combineOperator.stop();
    }

    assertEquals(groupCounts.size(), NUM_DISTINCT_GROUPS);
    for (int g = 0; g < NUM_DISTINCT_GROUPS; g++) {
      // Each group appears 2 times per segment * NUM_SEGMENTS
      assertEquals(groupCounts.get(g).longValue(), NUM_SEGMENTS * 2L,
          "Incorrect count for group " + g);
    }
  }

  private List<Operator> buildOperators(QueryContext queryContext) {
    List<Operator> operators = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      PlanNode planNode = PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext);
      operators.add(planNode.run());
    }
    return operators;
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
