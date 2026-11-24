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
package org.apache.pinot.core.query.aggregation.function;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class Sum0AggregationFunctionTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("col");

  @Test
  public void testFactoryCreatesSum0() {
    for (String fnName : new String[]{"SUM0(col)", "$SUM0(col)"}) {
      for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
        FunctionContext functionContext = RequestContextUtils.getExpression(fnName).getFunction();
        AggregationFunction<?, ?> function =
            AggregationFunctionFactory.getAggregationFunction(functionContext, nullHandlingEnabled);
        assertTrue(function instanceof Sum0AggregationFunction,
            "Expected Sum0AggregationFunction for " + fnName + ", got: " + function.getClass());
        assertEquals(function.getType(), AggregationFunctionType.SUM0);
      }
    }
  }

  @Test
  public void testEmptyInputReturnsZero() {
    for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
      Sum0AggregationFunction function = new Sum0AggregationFunction(List.of(COLUMN), nullHandlingEnabled);

      AggregationResultHolder aggHolder = function.createAggregationResultHolder();
      assertEquals(function.extractAggregationResult(aggHolder), 0.0);
      assertEquals(function.extractFinalResult(null), 0.0);
      assertEquals(function.extractFinalResult(function.extractAggregationResult(aggHolder)), 0.0);

      GroupByResultHolder groupByHolder = function.createGroupByResultHolder(4, 4);
      groupByHolder.ensureCapacity(2);
      assertEquals(function.extractGroupByResult(groupByHolder, 0), 0.0);
      assertEquals(function.extractGroupByResult(groupByHolder, 1), 0.0);
    }
  }

  @Test
  public void testAllNullBlockSkipsNullsWithNonZeroDefault() {
    int numDocs = 5;
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, numDocs);
    BlockValSet blockValSet = SyntheticBlockValSets.Int.create(numDocs, allNull, () -> 999);

    Sum0AggregationFunction nullAware = new Sum0AggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder nullAwareHolder = nullAware.createAggregationResultHolder();
    nullAware.aggregate(numDocs, nullAwareHolder, Map.of(COLUMN, blockValSet));
    assertEquals(nullAware.extractAggregationResult(nullAwareHolder), 0.0);
    assertEquals(nullAware.extractFinalResult(nullAware.extractAggregationResult(nullAwareHolder)), 0.0);

    Sum0AggregationFunction nullUnaware = new Sum0AggregationFunction(List.of(COLUMN), false);
    AggregationResultHolder nullUnawareHolder = nullUnaware.createAggregationResultHolder();
    nullUnaware.aggregate(numDocs, nullUnawareHolder, Map.of(COLUMN, blockValSet));
    assertEquals(nullUnaware.extractAggregationResult(nullUnawareHolder), 5.0 * 999.0);
  }

  @Test
  public void testGroupBySvWithNullsAndEmptyGroups() {
    // 4 rows: values [10, 999 (null), 20, 999 (null)], groups [0, 0, 1, 2]
    double[] values = {10.0, 999.0, 20.0, 999.0};
    int[] groupKeys = {0, 0, 1, 2};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(1, 3);

    // nullHandlingEnabled = true: Group 0 -> 10 (null skipped), Group 1 -> 20, Group 2 (all-null) -> 0.0,
    // Group 3 (untouched) -> 0.0.
    int[] idx = {0};
    BlockValSet blockValSet = SyntheticBlockValSets.Double.create(values.length, nullBitmap, () -> values[idx[0]++]);
    Sum0AggregationFunction nullAware = new Sum0AggregationFunction(List.of(COLUMN), true);
    GroupByResultHolder nullAwareHolder = nullAware.createGroupByResultHolder(4, 4);
    nullAwareHolder.ensureCapacity(4);
    nullAware.aggregateGroupBySV(values.length, groupKeys, nullAwareHolder, Map.of(COLUMN, blockValSet));
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 0), 10.0);
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 1), 20.0);
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 2), 0.0);
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 3), 0.0);

    // nullHandlingEnabled = false: underlying defaults (999.0) are summed; untouched Group 3 stays 0.0.
    int[] idxDisabled = {0};
    BlockValSet blockValSetDisabled =
        SyntheticBlockValSets.Double.create(values.length, nullBitmap, () -> values[idxDisabled[0]++]);
    Sum0AggregationFunction nullUnaware = new Sum0AggregationFunction(List.of(COLUMN), false);
    GroupByResultHolder nullUnawareHolder = nullUnaware.createGroupByResultHolder(4, 4);
    nullUnawareHolder.ensureCapacity(4);
    nullUnaware.aggregateGroupBySV(values.length, groupKeys, nullUnawareHolder, Map.of(COLUMN, blockValSetDisabled));
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 0), 1009.0);
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 1), 20.0);
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 2), 999.0);
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 3), 0.0);
  }

  @Test
  public void testSvGroupByMvWithNullsAndEmptyGroups() {
    // 4 rows: values [10, 999 (null), 20, 999 (null)], MV group keys [{0, 1}, {0, 2}, {1}, {2}]
    double[] values = {10.0, 999.0, 20.0, 999.0};
    int[][] groupKeysArray = {{0, 1}, {0, 2}, {1}, {2}};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(1, 3);

    // nullHandlingEnabled = true: Group 0 -> 10, Group 1 -> 30, Group 2 (all-null) -> 0.0, Group 3 (untouched) -> 0.0.
    int[] idx = {0};
    BlockValSet blockValSet = SyntheticBlockValSets.Double.create(values.length, nullBitmap, () -> values[idx[0]++]);
    Sum0AggregationFunction nullAware = new Sum0AggregationFunction(List.of(COLUMN), true);
    GroupByResultHolder nullAwareHolder = nullAware.createGroupByResultHolder(4, 4);
    nullAwareHolder.ensureCapacity(4);
    nullAware.aggregateGroupByMV(values.length, groupKeysArray, nullAwareHolder, Map.of(COLUMN, blockValSet));
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 0), 10.0);
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 1), 30.0);
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 2), 0.0);
    assertEquals(nullAware.extractGroupByResult(nullAwareHolder, 3), 0.0);

    // nullHandlingEnabled = false: null defaults (999.0) are summed into their groups; untouched Group 3 stays 0.0.
    int[] idxDisabled = {0};
    BlockValSet blockValSetDisabled =
        SyntheticBlockValSets.Double.create(values.length, nullBitmap, () -> values[idxDisabled[0]++]);
    Sum0AggregationFunction nullUnaware = new Sum0AggregationFunction(List.of(COLUMN), false);
    GroupByResultHolder nullUnawareHolder = nullUnaware.createGroupByResultHolder(4, 4);
    nullUnawareHolder.ensureCapacity(4);
    nullUnaware.aggregateGroupByMV(values.length, groupKeysArray, nullUnawareHolder,
        Map.of(COLUMN, blockValSetDisabled));
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 0), 1009.0);
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 1), 30.0);
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 2), 1998.0);
    assertEquals(nullUnaware.extractGroupByResult(nullUnawareHolder, 3), 0.0);
  }

  @Test
  public void testGroupByMvWithNulls() {
    double[][] mvValues = {{1.5, 2.5}, {100.0}, {3.0}, {}};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(1);
    BlockValSet blockValSet = SyntheticBlockValSets.DoubleMV.create(nullBitmap, mvValues);

    Sum0AggregationFunction nullAware = new Sum0AggregationFunction(List.of(COLUMN), true);
    GroupByResultHolder holderSv = nullAware.createGroupByResultHolder(4, 4);
    holderSv.ensureCapacity(4);
    nullAware.aggregateGroupBySV(mvValues.length, new int[]{0, 1, 0, 3}, holderSv, Map.of(COLUMN, blockValSet));
    assertEquals(nullAware.extractGroupByResult(holderSv, 0), 7.0);
    assertEquals(nullAware.extractGroupByResult(holderSv, 1), 0.0);
    assertEquals(nullAware.extractGroupByResult(holderSv, 2), 0.0);
    assertEquals(nullAware.extractGroupByResult(holderSv, 3), 0.0);

    GroupByResultHolder holderMv = nullAware.createGroupByResultHolder(4, 4);
    holderMv.ensureCapacity(4);
    nullAware.aggregateGroupByMV(mvValues.length, new int[][]{{0, 2}, {1, 2}, {0}, {3}}, holderMv,
        Map.of(COLUMN, blockValSet));
    assertEquals(nullAware.extractGroupByResult(holderMv, 0), 7.0);
    assertEquals(nullAware.extractGroupByResult(holderMv, 1), 0.0);
    assertEquals(nullAware.extractGroupByResult(holderMv, 2), 4.0);
    assertEquals(nullAware.extractGroupByResult(holderMv, 3), 0.0);

    // nullHandlingEnabled = false: null MV row (100.0) is included in the sum.
    Sum0AggregationFunction nullUnaware = new Sum0AggregationFunction(List.of(COLUMN), false);
    GroupByResultHolder holderSvDisabled = nullUnaware.createGroupByResultHolder(4, 4);
    holderSvDisabled.ensureCapacity(4);
    nullUnaware.aggregateGroupBySV(mvValues.length, new int[]{0, 1, 0, 3}, holderSvDisabled,
        Map.of(COLUMN, blockValSet));
    assertEquals(nullUnaware.extractGroupByResult(holderSvDisabled, 0), 7.0);
    assertEquals(nullUnaware.extractGroupByResult(holderSvDisabled, 1), 100.0);
    assertEquals(nullUnaware.extractGroupByResult(holderSvDisabled, 2), 0.0);
    assertEquals(nullUnaware.extractGroupByResult(holderSvDisabled, 3), 0.0);

    GroupByResultHolder holderMvDisabled = nullUnaware.createGroupByResultHolder(4, 4);
    holderMvDisabled.ensureCapacity(4);
    nullUnaware.aggregateGroupByMV(mvValues.length, new int[][]{{0, 2}, {1, 2}, {0}, {3}}, holderMvDisabled,
        Map.of(COLUMN, blockValSet));
    assertEquals(nullUnaware.extractGroupByResult(holderMvDisabled, 0), 7.0);
    assertEquals(nullUnaware.extractGroupByResult(holderMvDisabled, 1), 100.0);
    assertEquals(nullUnaware.extractGroupByResult(holderMvDisabled, 2), 104.0);
    assertEquals(nullUnaware.extractGroupByResult(holderMvDisabled, 3), 0.0);
  }

  @Test
  public void testMergeWithNullAndZero() {
    Sum0AggregationFunction function = new Sum0AggregationFunction(List.of(COLUMN), true);
    assertEquals(AggregationFunctionUtils.merge(function, null, null), null);
    assertEquals(function.extractFinalResult(AggregationFunctionUtils.merge(function, null, null)), 0.0);
    assertEquals(AggregationFunctionUtils.merge(function, null, 5.0), 5.0);
    assertEquals(AggregationFunctionUtils.merge(function, 0.0, 5.0), 5.0);
    assertEquals(function.merge(12.5, 7.5), 20.0);
  }

  @Test
  public void testStarTreeServesSum0AndReturnsZeroOnNoMatch()
      throws Exception {
    File tempDir = new File(FileUtils.getTempDirectory(), "Sum0StarTreeTest_" + System.nanoTime());
    IndexSegment segment = null;
    try {
      Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
          .addSingleValueDimension("dim", DataType.STRING)
          .addMetric("metric", DataType.LONG)
          .build();
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

      GenericRow r1 = new GenericRow();
      r1.putValue("dim", "a");
      r1.putValue("metric", 10L);
      GenericRow r2 = new GenericRow();
      r2.putValue("dim", "a");
      r2.putValue("metric", 25L);
      GenericRow r3 = new GenericRow();
      r3.putValue("dim", "b");
      r3.putValue("metric", 40L);

      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(tempDir.getPath());
      config.setSegmentName("seg0");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(List.of(r1, r2, r3)));
      driver.build();

      StarTreeIndexConfig starTreeConfig =
          new StarTreeIndexConfig(List.of("dim"), null, List.of("SUM__metric"), null, 1);
      File indexDir = new File(tempDir, "seg0");
      try (MultipleTreesBuilder builder = new MultipleTreesBuilder(List.of(starTreeConfig), false, indexDir,
          MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
        builder.build();
      }
      segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
      SegmentContext segmentContext = new SegmentContext(segment);

      // Matching filter is served by the SUM__metric star-tree and returns 35.0.
      QueryContext matchQuery = QueryContextConverterUtils.getQueryContext(
          "SET enableNullHandling=true; SELECT SUM0(metric) FROM testTable WHERE dim = 'a'");
      FilterPlanNode matchFilter = new FilterPlanNode(segmentContext, matchQuery);
      matchFilter.run();
      assertNotNull(StarTreeUtils.createStarTreeBasedAggregationInfo(segment, matchQuery,
          matchQuery.getAggregationFunctions(), matchQuery.getFilter(), matchFilter.getPredicateEvaluators()));
      AggregationResultsBlock matchBlock = new AggregationPlanNode(segmentContext, matchQuery).run().nextBlock();
      assertEquals(matchBlock.getResults().get(0), 35.0);

      // Non-matching filter returns 0.0 (not null) even with enableNullHandling=true.
      QueryContext emptyQuery = QueryContextConverterUtils.getQueryContext(
          "SET enableNullHandling=true; SELECT SUM0(metric) FROM testTable WHERE dim = 'zzz'");
      AggregationResultsBlock emptyBlock = new AggregationPlanNode(segmentContext, emptyQuery).run().nextBlock();
      assertEquals(emptyBlock.getResults().get(0), 0.0);
    } finally {
      if (segment != null) {
        segment.destroy();
      }
      FileUtils.deleteDirectory(tempDir);
    }
  }
}
