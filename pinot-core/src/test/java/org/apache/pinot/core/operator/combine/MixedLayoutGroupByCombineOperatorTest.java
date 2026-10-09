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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests that [GroupByCombineOperator] correctly merges a MIX of BASE-layout blocks (grouping-set base
/// aggregation: union columns only, no $groupingId) and FULL-layout blocks (per-row expansion, with $groupingId)
/// in the same query. Mixed layouts are legitimate because the base-vs-expansion choice is per segment: the
/// MV-column carve-out and the base-group cardinality gate are evaluated against each segment's own metadata.
public class MixedLayoutGroupByCombineOperatorTest {
  private final ExecutorService _executorService = Executors.newFixedThreadPool(4);

  /// `GROUP BY GROUPING SETS ((d1), ())` over `SUM(m1)`: union columns = [d1], sets = {d1} (ordinal 0) and the
  /// grand total (ordinal 1). Full layout: [d1, $groupingId, sum(m1)]; base layout: [d1, sum(m1)].
  private static QueryContext queryContext() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT d1, SUM(m1) FROM t GROUP BY GROUPING SETS ((d1), ())");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 60_000);
    return queryContext;
  }

  private static GroupByResultsBlock baseLayoutBlock(QueryContext queryContext) {
    DataSchema baseSchema = new DataSchema(new String[]{"d1", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    List<IntermediateRecord> records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a"}), new Record(new Object[]{"a", 1.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"b"}), new Record(new Object[]{"b", 2.0})));
    return new GroupByResultsBlock(baseSchema, records, queryContext);
  }

  private static GroupByResultsBlock fullLayoutBlock(QueryContext queryContext) {
    DataSchema fullSchema = new DataSchema(new String[]{"d1", "$groupingId", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<IntermediateRecord> records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a", 0}),
            new Record(new Object[]{"a", 0, 5.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{null, 1}),
            new Record(new Object[]{null, 1, 7.0})));
    return new GroupByResultsBlock(fullSchema, records, queryContext);
  }

  @SuppressWarnings("rawtypes")
  private static Operator mockOperator(GroupByResultsBlock block) {
    Operator operator = mock(Operator.class);
    when(operator.nextBlock()).thenReturn(block);
    when(operator.getExecutionStatistics()).thenReturn(new ExecutionStatistics(0, 0, 0, 0));
    return operator;
  }

  @Test
  public void testMixedBaseAndExpansionBlocksMergeCorrectly()
      throws Exception {
    QueryContext queryContext = queryContext();
    // Segment 1 chose base aggregation (base-layout block); segment 2 chose per-row expansion (full layout).
    List<Operator> operators =
        List.of(mockOperator(baseLayoutBlock(queryContext)), mockOperator(fullLayoutBlock(queryContext)));
    GroupByCombineOperator combineOperator =
        new GroupByCombineOperator(operators, queryContext, _executorService);

    Object block = combineOperator.nextBlock();
    if (!(block instanceof GroupByResultsBlock)) {
      throw new AssertionError("Combine returned " + block.getClass().getSimpleName() + ": "
          + ((org.apache.pinot.core.operator.blocks.results.BaseResultsBlock) block).getErrorMessages());
    }
    GroupByResultsBlock mergedBlock = (GroupByResultsBlock) block;
    IndexedTable table = (IndexedTable) mergedBlock.getTable();

    // Base block derives to: (a,0)=1, (b,0)=2, (null,1)=3. Full block merges in: (a,0)+=5, (null,1)+=7.
    Map<String, Double> result = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      result.put(values[0] + "|" + values[1], ((Number) values[2]).doubleValue());
    }
    assertEquals(result.size(), 3);
    assertEquals(result.get("a|0"), 6.0);
    assertEquals(result.get("b|0"), 2.0);
    assertEquals(result.get("null|1"), 10.0);
  }

  @Test
  public void testUnexpectedBaseGroupOverflowFailsClosed() throws Exception {
    // The instance planner should reject this path before execution. If it does not, the combine must fail
    // rather than return an incomplete grand total or buffer unbounded aggregation intermediates.
    QueryContext queryContext = queryContext();
    queryContext.setNumGroupsLimit(3);
    DataSchema baseSchema = new DataSchema(new String[]{"d1", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    List<IntermediateRecord> block1Records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a"}), new Record(new Object[]{"a", 1.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"b"}), new Record(new Object[]{"b", 2.0})));
    List<IntermediateRecord> block2Records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"c"}), new Record(new Object[]{"c", 4.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"d"}), new Record(new Object[]{"d", 8.0})));
    List<Operator> operators = List.of(
        mockOperator(new GroupByResultsBlock(baseSchema, block1Records, queryContext)),
        mockOperator(new GroupByResultsBlock(baseSchema, block2Records, queryContext)));
    GroupByCombineOperator combineOperator =
        new GroupByCombineOperator(operators, queryContext, _executorService);

    assertTrue(combineOperator.nextBlock() instanceof ExceptionResultsBlock,
        "unexpected BASE overflow must fail instead of returning an incorrect total");
  }

  @Test
  public void testMixedLayoutsCannotGrowPastDerivedGroupLimit()
      throws Exception {
    QueryContext queryContext = queryContext();
    queryContext.setNumGroupsLimit(2);
    queryContext.setNumGroupsWarningLimit(2);
    DataSchema baseSchema = new DataSchema(new String[]{"d1", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    List<IntermediateRecord> baseRecords = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a"}), new Record(new Object[]{"a", 1.0})));
    DataSchema fullSchema = new DataSchema(new String[]{"d1", "$groupingId", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<IntermediateRecord> fullRecords = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"c", 0}),
            new Record(new Object[]{"c", 0, 5.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{null, 1}),
            new Record(new Object[]{null, 1, 7.0})));
    GroupByCombineOperator combineOperator = new GroupByCombineOperator(List.of(
        mockOperator(new GroupByResultsBlock(baseSchema, baseRecords, queryContext)),
        mockOperator(new GroupByResultsBlock(fullSchema, fullRecords, queryContext))), queryContext,
        _executorService);

    GroupByResultsBlock mergedBlock = (GroupByResultsBlock) combineOperator.nextBlock();
    IndexedTable table = (IndexedTable) mergedBlock.getTable();
    assertEquals(table.size(), 2, "full-layout groups must not bypass the derive limit");
    assertTrue(mergedBlock.isGroupsTrimmed());
    assertTrue(mergedBlock.isNumGroupsLimitReached());
    assertTrue(mergedBlock.isNumGroupsWarningLimitReached());
    Iterator<Record> iterator = table.iterator();
    boolean foundGrandTotal = false;
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      if (((Number) values[1]).intValue() == 1) {
        foundGrandTotal = true;
        assertEquals(((Number) values[2]).doubleValue(), 8.0,
            "the retained grand total must include both layouts");
      }
    }
    assertTrue(foundGrandTotal);
  }

  @Test
  public void testPerSetTrimRunsAfterFullLayoutMerge()
      throws Exception {
    // The per-set server trim must run AFTER expansion-path (full-layout) records are merged into the derived
    // table. If the derive trimmed first, the merge below would re-add the trimmed group "x" with only its
    // expansion-path share (100.0) instead of its complete total (101.0) -- a wrong row that can win the
    // broker's ORDER BY.
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT d1, SUM(m1) FROM t GROUP BY GROUPING SETS ((d1), ()) ORDER BY SUM(m1) DESC LIMIT 1");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 60_000);
    // LIMIT 1 with trim size 1 resolves to an effective per-set trim of max(5 * LIMIT, 1) = 5 groups; with 2
    // grouping sets the trim triggers once the derived table exceeds 10 groups.
    queryContext.getQueryOptions().put(QueryOptionKey.GROUPING_SETS_MIN_SERVER_TRIM_SIZE, "1");

    // Base layout: group "x" with a LOW base-path share (1.0, trimmed away under a derive-first trim) plus 11
    // groups worth 11..21, so the derived table holds 12 set-0 groups + the grand total = 13 > 10.
    DataSchema baseSchema = new DataSchema(new String[]{"d1", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    List<IntermediateRecord> baseRecords = new ArrayList<>();
    baseRecords.add(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"x"}), new Record(new Object[]{"x", 1.0})));
    for (int i = 1; i <= 11; i++) {
      String d1 = String.format("g%02d", i);
      double value = 10.0 + i;
      baseRecords.add(IntermediateRecord.withoutOrderByValues(new Key(new Object[]{d1}),
          new Record(new Object[]{d1, value})));
    }
    // Full layout: an expansion-path segment carrying the HIGH share of "x" and its own grand total.
    DataSchema fullSchema = new DataSchema(new String[]{"d1", "$groupingId", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<IntermediateRecord> fullRecords = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"x", 0}),
            new Record(new Object[]{"x", 0, 100.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{null, 1}),
            new Record(new Object[]{null, 1, 100.0})));
    GroupByCombineOperator combineOperator = new GroupByCombineOperator(List.of(
        mockOperator(new GroupByResultsBlock(baseSchema, baseRecords, queryContext)),
        mockOperator(new GroupByResultsBlock(fullSchema, fullRecords, queryContext))), queryContext,
        _executorService);

    GroupByResultsBlock mergedBlock = (GroupByResultsBlock) combineOperator.nextBlock();
    assertTrue(mergedBlock.isGroupsTrimmed(), "the per-set trim must flag the response as trimmed");
    IndexedTable table = (IndexedTable) mergedBlock.getTable();
    Map<String, Double> result = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      result.put(values[0] + "|" + values[1], ((Number) values[2]).doubleValue());
    }
    // Top-5 of set 0 by SUM DESC (x=101, g11=21, g10=20, g09=19, g08=18) plus the grand total.
    assertEquals(result.size(), 6);
    assertEquals(result.get("x|0"), 101.0, "a retained group must carry its COMPLETE merged value");
    assertEquals(result.get("null|1"), 277.0, "the grand total must include both layouts");
  }

  @Test
  public void testFullLayoutWithoutGroupingIdIsRejected() {
    QueryContext queryContext = queryContext();
    DataSchema badSchema = new DataSchema(new String[]{"d1", "notGroupingId", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<IntermediateRecord> records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a", 0}),
            new Record(new Object[]{"a", 0, 1.0})));
    GroupByCombineOperator combineOperator = new GroupByCombineOperator(
        List.of(mockOperator(new GroupByResultsBlock(badSchema, records, queryContext))), queryContext,
        _executorService);

    assertTrue(combineOperator.nextBlock() instanceof ExceptionResultsBlock,
        "a full-layout block without $groupingId must fail closed");
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdownNow();
  }
}
