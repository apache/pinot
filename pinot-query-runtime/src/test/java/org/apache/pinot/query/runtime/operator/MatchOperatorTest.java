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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.match.MatchLimits;
import org.apache.pinot.query.runtime.operator.match.MatchTestFixtures;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.INPUT_SCHEMA;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.VALUE_INDEX;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.anySymbol;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.concat;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.labelSymbols;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.quantify;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.rows;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.symbol;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * End to end tests of {@link MatchOperator}: measures, the four AFTER MATCH SKIP modes, empty matches, partition
 * handling and both resource guardrails.
 */
public class MatchOperatorTest {
  /** Two pattern variables that match every row, so only the pattern shape decides the classification. */
  private static final List<PatternSymbol> ANY_A_B = List.of(anySymbol("A"), anySymbol("B"));

  @Test
  public void testEmitsOneRowPerMatchWithMeasures() {
    // PATTERN (A B+) DEFINE A AS label = 'A', B AS label = 'B' over A B B A B.
    RowPattern pattern = concat(symbol(0), quantify(symbol(1), 1, RowPattern.Quantify.UNBOUNDED, true));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "mno", "cls", "firstA", "lastB", "sumB", "cnt"},
        new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT,
            ColumnDataType.LONG, ColumnDataType.LONG
        });
    List<MatchNode.Measure> measures = List.of(
        measure("mno", matchNumber()),
        measure("cls", classifier()),
        measure("firstA", navigation("FIRST", ColumnDataType.INT, patternRef(0, "A"), 0)),
        measure("lastB", navigation("LAST", ColumnDataType.INT, patternRef(1, "B"), 0)),
        measure("sumB", aggregate("SUM", ColumnDataType.LONG, patternRef(1, "B"))),
        measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    List<Object[]> output = run(node(resultSchema, labelSymbols("A", "B"), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("ABBAB"));

    assertEquals(output.size(), 2);
    // First match covers rows 0..2: A, B, B.
    assertEquals(output.get(0), new Object[]{0, 1L, "B", 0, 2, 3L, 3L});
    // Second match covers rows 3..4: A, B. MATCH_NUMBER restarts nothing, it keeps counting within the partition.
    assertEquals(output.get(1), new Object[]{0, 2L, "B", 3, 4, 4L, 2L});
  }

  /**
   * Every single-variable aggregate other than SUM, which is the only one the rest of this class exercises.
   * COUNT over an explicit argument used to throw {@code Unexpected MATCH_RECOGNIZE aggregate: COUNT} because the
   * accumulation loop ran for COUNT as well, and {@code accumulate} has no COUNT branch.
   */
  @Test
  public void testSingleVariableAggregates() {
    // PATTERN (A B+) over A B B A B, where the value column equals the row index.
    RowPattern pattern = concat(symbol(0), quantify(symbol(1), 1, RowPattern.Quantify.UNBOUNDED, true));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "countB", "minB", "maxB", "avgB"},
        new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.DOUBLE
        });
    List<MatchNode.Measure> measures = List.of(
        measure("countB", aggregate("COUNT", ColumnDataType.LONG, patternRef(1, "B"))),
        measure("minB", aggregate("MIN", ColumnDataType.INT, patternRef(1, "B"))),
        measure("maxB", aggregate("MAX", ColumnDataType.INT, patternRef(1, "B"))),
        measure("avgB", aggregate("AVG", ColumnDataType.DOUBLE, patternRef(1, "B"))));

    List<Object[]> output = run(node(resultSchema, labelSymbols("A", "B"), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("ABBAB"));

    assertEquals(output.size(), 2);
    // First match binds rows 1 and 2 to B, so the aggregated values are 1 and 2.
    assertEquals(output.get(0), new Object[]{0, 2L, 1, 2, 1.5});
    // Second match binds only row 4 to B.
    assertEquals(output.get(1), new Object[]{0, 1L, 4, 4, 4.0});
  }

  @Test(dataProvider = "skipModes")
  public void testAfterMatchSkipModes(MatchNode.AfterMatchSkipMode skipMode, int skipToSymbolOrdinal,
      List<Integer> expectedLastValues) {
    // PATTERN (A A B B) with both variables matching every row, over seven rows valued 0..6. Every skip mode resumes
    // at a different place, so the set of matches differs.
    RowPattern pattern = concat(symbol(0), symbol(0), symbol(1), symbol(1));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "lastB"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures =
        List.of(measure("lastB", navigation("LAST", ColumnDataType.INT, patternRef(1, "B"), 0)));

    List<Object[]> output =
        run(node(resultSchema, ANY_A_B, pattern, measures, skipMode, skipToSymbolOrdinal), rows("0123456"));

    assertEquals(lastValues(output), expectedLastValues);
  }

  /**
   * The second column of every output row, which the skip mode tests use to identify the matches that were reported.
   */
  private static List<Integer> lastValues(List<Object[]> output) {
    List<Integer> values = new ArrayList<>(output.size());
    for (Object[] row : output) {
      values.add((Integer) row[1]);
    }
    return values;
  }

  @DataProvider(name = "skipModes")
  public Object[][] skipModes() {
    //@formatter:off
    return new Object[][]{
        // Resume past row 3, so only rows 4..6 are left and they are too few for another match.
        {MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL, List.of(3)},
        // Resume one row after the start of the match every time, so the matches overlap maximally.
        {MatchNode.AfterMatchSkipMode.TO_NEXT_ROW, MatchNode.NO_SKIP_TO_SYMBOL, List.of(3, 4, 5, 6)},
        // The first row mapped to B is two rows after the start of the match.
        {MatchNode.AfterMatchSkipMode.TO_FIRST, 1, List.of(3, 5)},
        // The last row mapped to B is the last row of the match.
        {MatchNode.AfterMatchSkipMode.TO_LAST, 1, List.of(3, 6)}
    };
    //@formatter:on
  }

  @Test
  public void testSkipToLastResumesAtThatVariableRatherThanAtTheEndOfTheMatch() {
    // PATTERN (A A B C), all variables matching every row, over seven rows valued 0..6. The last row mapped to B sits
    // in the middle of the match, so SKIP TO LAST B must resume there and not at the end of the match. Resuming at the
    // end would report the same rows as SKIP PAST LAST ROW, which is the bug this pins down.
    RowPattern pattern = concat(symbol(0), symbol(0), symbol(1), symbol(2));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "lastC"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures =
        List.of(measure("lastC", navigation("LAST", ColumnDataType.INT, patternRef(2, "C"), 0)));
    List<PatternSymbol> symbols = List.of(anySymbol("A"), anySymbol("B"), anySymbol("C"));

    List<Object[]> toLast = run(node(resultSchema, symbols, pattern, measures,
        MatchNode.AfterMatchSkipMode.TO_LAST, 1), rows("0123456"));
    // Match at row 0 covers rows 0..3 and resumes at row 2, the row mapped to B; the next match covers rows 2..5.
    assertEquals(lastValues(toLast), List.of(3, 5));

    List<Object[]> pastLastRow = run(node(resultSchema, symbols, pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("0123456"));
    // Resuming past row 3 leaves only three rows, which is one too few for another match.
    assertEquals(lastValues(pastLastRow), List.of(3));
  }

  @Test
  public void testPartitionSplitAcrossInputBlocksIsMatchedAsOnePartition() {
    // A match must be found across an input block boundary: blocks are a transport detail, partitions are not.
    RowPattern pattern = concat(symbol(0), symbol(0), symbol(0));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));
    MatchNode node = node(resultSchema, List.of(anySymbol("A")), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL);

    OpChainExecutionContext context = OperatorTestUtil.getContext(Map.of());
    List<Object[]> partitionRows = rows(0, "AAA");
    // One row in the first block, the remaining two in the second: no single block holds a whole match.
    MultiStageOperator input = new BlockListMultiStageOperator.Builder(context, INPUT_SCHEMA)
        .addRow(partitionRows.get(0)).finishBlock()
        .addRow(partitionRows.get(1)).addRow(partitionRows.get(2)).finishBlock()
        .buildWithEos();

    MatchOperator operator = new MatchOperator(context, input, INPUT_SCHEMA, node);
    List<Object[]> output = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (!block.isEos()) {
      output.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertTrue(!((MseBlock.Eos) block).isError(), "Expecting a successful end of stream, got: " + block);
    assertEquals(output.size(), 1);
    assertEquals(output.get(0), new Object[]{0, 3L});
  }

  @Test
  public void testEmptyMatchIsEmittedAndAlwaysAdvancesOneRow() {
    // PATTERN (A*) over B A B: rows that are not an A still produce an empty match, and the scan must move on
    // instead of matching the same empty match forever.
    RowPattern pattern = quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "mno", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.LONG});
    List<MatchNode.Measure> measures = List.of(measure("mno", matchNumber()),
        measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    List<Object[]> output = run(node(resultSchema, labelSymbols("A"), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("BAB"));

    assertEquals(output.size(), 3);
    assertEquals(output.get(0), new Object[]{0, 1L, 0L});
    assertEquals(output.get(1), new Object[]{0, 2L, 1L});
    assertEquals(output.get(2), new Object[]{0, 3L, 0L});
  }

  @Test
  public void testSkipToFirstThatCannotProgressIsAnError() {
    // AFTER MATCH SKIP TO FIRST A on PATTERN (A+) would resume at the first row of the match it just reported, which
    // never terminates. SQL:2016 makes that an error rather than a silently adjusted position.
    RowPattern pattern = quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    MseBlock block = execute(node(resultSchema, labelSymbols("A"), pattern, measures,
        MatchNode.AfterMatchSkipMode.TO_FIRST, 0), rows("AAA"), Map.of());

    assertErrorContains(block, QueryErrorCode.QUERY_EXECUTION, "would resume at the first row of the match");
  }

  @Test
  public void testMatchesAreScopedToTheirPartition() {
    // PATTERN (A A) over two partitions of two rows each: a match must never span the boundary.
    RowPattern pattern = concat(symbol(0), symbol(0));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "lastA"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures =
        List.of(measure("lastA", navigation("LAST", ColumnDataType.INT, patternRef(0, "A"), 0)));

    List<Object[]> partitioned = new ArrayList<>(rows(0, "AA"));
    partitioned.addAll(rows(1, "AA"));
    List<Object[]> output = run(node(resultSchema, List.of(anySymbol("A")), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), partitioned);

    assertEquals(output.size(), 2);
    assertEquals(output.get(0), new Object[]{0, 1});
    assertEquals(output.get(1), new Object[]{1, 1});
  }

  @Test
  public void testUngroupedPartitionsAreRejected() {
    // The sort exchange below the operator guarantees that a partition arrives in one contiguous run. If it does not,
    // matching each fragment separately would report matches that do not exist, so fail instead.
    RowPattern pattern = concat(symbol(0), symbol(0));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "lastA"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures =
        List.of(measure("lastA", navigation("LAST", ColumnDataType.INT, patternRef(0, "A"), 0)));

    List<Object[]> interleaved = new ArrayList<>(rows(0, "AA"));
    interleaved.addAll(rows(1, "AA"));
    interleaved.addAll(rows(0, "AA"));
    MseBlock block = execute(node(resultSchema, List.of(anySymbol("A")), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), interleaved, Map.of());

    assertErrorContains(block, QueryErrorCode.QUERY_EXECUTION, "is not grouped by partition key");
  }

  @Test
  public void testMaxRowsInMatchThrowsRatherThanTruncating() {
    RowPattern pattern = quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    MseBlock block = execute(node(resultSchema, List.of(anySymbol("A")), pattern, measures,
            MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAA"),
        Map.of(MatchLimits.MAX_ROWS_IN_MATCH, "2"));

    assertErrorContains(block, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED,
        "exceeds the maximum of 2 rows");
  }

  @Test
  public void testMaxStepsPerMatchAttemptThrowsRatherThanTruncating() {
    RowPattern pattern = quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    MseBlock block = execute(node(resultSchema, List.of(anySymbol("A")), pattern, measures,
            MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAAAAAAAA"),
        Map.of(MatchLimits.MAX_STEPS_PER_MATCH_ATTEMPT, "3"));

    assertErrorContains(block, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED,
        "maximum of 3 pattern matching steps");
  }

  @Test
  public void testHintOutranksTheQueryOption() {
    RowPattern pattern = quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));
    PlanNode.NodeHint hint = new PlanNode.NodeHint(
        Map.of(MatchLimits.MATCH_HINT_OPTIONS, Map.of(MatchLimits.MAX_ROWS_IN_MATCH_HINT, "2")));

    MatchNode node = new MatchNode(-1, resultSchema, hint, List.of(), List.of(anySymbol("A")), pattern, measures,
        List.of(MatchTestFixtures.PID_INDEX), List.of(), MatchNode.AfterMatchSkipMode.PAST_LAST_ROW,
        MatchNode.NO_SKIP_TO_SYMBOL, MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
    // The query option is far more permissive, but the hint wins.
    MseBlock block = execute(node, rows("AAAA"), Map.of(MatchLimits.MAX_ROWS_IN_MATCH, "1000"));

    assertErrorContains(block, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "exceeds the maximum of 2 rows");
  }

  @Test
  public void testAllRowsPerMatchIsRejected() {
    RowPattern pattern = symbol(0);
    DataSchema resultSchema = new DataSchema(new String[]{"pid"}, new ColumnDataType[]{ColumnDataType.INT});
    MatchNode node = new MatchNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of(anySymbol("A")),
        pattern, List.of(), List.of(MatchTestFixtures.PID_INDEX), List.of(),
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL,
        MatchNode.RowsPerMatchMode.ALL_ROWS_PER_MATCH);
    MultiStageOperator input = new BlockListMultiStageOperator.Builder(INPUT_SCHEMA).addBlock().buildWithEos();

    QueryException exception = expectThrows(QueryException.class,
        () -> new MatchOperator(OperatorTestUtil.getTracingContext(), input, INPUT_SCHEMA, node));
    assertTrue(exception.getMessage().contains("ALL ROWS PER MATCH"), exception.getMessage());
  }

  @Test
  public void testDefinePredicateCanNavigateToEarlierRows() {
    // DEFINE B AS value > PREV(value, 1): an increasing run. Values are the row indexes, so every row qualifies once
    // the first one anchored the run.
    RexExpression current = new RexExpression.FunctionCall(ColumnDataType.INT, "PREV",
        List.of(patternRef(0, "A"), literal(0)));
    RexExpression previous = new RexExpression.FunctionCall(ColumnDataType.INT, "PREV",
        List.of(patternRef(0, "A"), literal(1)));
    PatternSymbol increasing = new PatternSymbol("A",
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "GREATER_THAN", List.of(current, previous)));

    RowPattern pattern = quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    List<Object[]> output = run(node(resultSchema, List.of(increasing), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAA"));

    // Row 0 has no previous row, so PREV is null and the predicate is false: the first match starts at row 1 and runs
    // to the end of the partition.
    assertEquals(output.size(), 1);
    assertEquals(output.get(0), new Object[]{0, 3L});
  }

  @Test
  public void testDefinePredicateCanReferenceAnotherPatternVariable() {
    // DEFINE A AS label = 'A', B AS B.value > A.value. The cross symbol reference must resolve to the last row mapped
    // to A so far, not to the row being tested. Row 2 breaks the run precisely because A.value is still 5, so an
    // implementation that resolved A.value to the current row would compare a value with itself and match nothing.
    PatternSymbol a = MatchTestFixtures.labelSymbol("A", 0);
    RexExpression bValue = new RexExpression.FunctionCall(ColumnDataType.INT, "PREV",
        List.of(patternRef(1, "B"), literal(0)));
    RexExpression aValue = new RexExpression.FunctionCall(ColumnDataType.INT, "PREV",
        List.of(patternRef(0, "A"), literal(0)));
    PatternSymbol b = new PatternSymbol("B",
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "GREATER_THAN", List.of(bValue, aValue)));

    RowPattern pattern = concat(symbol(0), quantify(symbol(1), 1, RowPattern.Quantify.UNBOUNDED, true));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "lastB", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures = List.of(
        measure("lastB", navigation("LAST", ColumnDataType.INT, patternRef(1, "B"), 0)),
        measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));

    List<Object[]> inputRows = List.of(new Object[]{0, "A", 5}, new Object[]{0, "B", 6}, new Object[]{0, "B", 3},
        new Object[]{0, "B", 7});
    List<Object[]> output = run(node(resultSchema, List.of(a, b), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), inputRows);

    // The run stops at value 3, which is not greater than 5, so the match is rows 0..1 only. Rows 2 and 3 cannot start
    // a match of their own because neither is labelled 'A'.
    assertEquals(output.size(), 1);
    assertEquals(output.get(0), new Object[]{0, 6, 2L});
  }

  @Test
  public void testNavigationMayLeaveTheMatchButNotThePartition() {
    // NEXT(A.value, 1) reads the row after the last row of the match. That row is outside the match but inside the
    // partition, which SQL:2016 allows: a physical step is bounded by the partition only. Stepping past the end of the
    // partition yields null rather than the first row of the next partition.
    RowPattern pattern = concat(symbol(0), symbol(0));
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "next"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures =
        List.of(measure("next", navigation("NEXT", ColumnDataType.INT, patternRef(0, "A"), 1)));

    List<Object[]> partitioned = new ArrayList<>(rows(0, "AAAA"));
    partitioned.addAll(rows(1, "AA"));
    List<Object[]> output = run(node(resultSchema, List.of(anySymbol("A")), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), partitioned);

    assertEquals(output.size(), 3);
    // Partition 0: the match on rows 0..1 sees row 2, the one on rows 2..3 falls off the end of the partition.
    assertEquals(output.get(0), new Object[]{0, 2});
    assertEquals(output.get(1), new Object[]{0, null});
    // Partition 1: the only match covers the whole partition, so there is no next row here either.
    assertEquals(output.get(2), new Object[]{1, null});
  }

  @Test
  public void testMatchNumberRestartsInEachPartition() {
    // SQL:2016 numbers matches within a partition, so the counter must restart at the partition boundary rather than
    // run on across the whole input.
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "mno"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures = List.of(measure("mno", matchNumber()));

    List<Object[]> partitioned = new ArrayList<>(rows(0, "AA"));
    partitioned.addAll(rows(1, "AA"));
    List<Object[]> output = run(node(resultSchema, List.of(anySymbol("A")), symbol(0), measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), partitioned);

    assertEquals(output.size(), 4);
    assertEquals(output.get(0), new Object[]{0, 1L});
    assertEquals(output.get(1), new Object[]{0, 2L});
    assertEquals(output.get(2), new Object[]{1, 1L});
    assertEquals(output.get(3), new Object[]{1, 2L});
  }

  @Test
  public void testLogicalOffsetSelectsTheNthRowMappedToTheVariable() {
    // FIRST(A.value, 1) is the second row mapped to A and LAST(A.value, 1) is the second to last, so over a match of
    // four rows valued 0..3 they must be different rows. An offset that was ignored would make them equal.
    RowPattern pattern = quantify(symbol(0), 4, 4, true);
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "first1", "last1"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures = List.of(
        measure("first1", navigation("FIRST", ColumnDataType.INT, patternRef(0, "A"), 1)),
        measure("last1", navigation("LAST", ColumnDataType.INT, patternRef(0, "A"), 1)));

    List<Object[]> output = run(node(resultSchema, List.of(anySymbol("A")), pattern, measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAA"));

    assertEquals(output.size(), 1);
    assertEquals(output.get(0), new Object[]{0, 1, 2});
  }

  @Test
  public void testAnchorsRestrictWhereAMatchMayStartAndEnd() {
    // PATTERN (^ A A) over four rows: without the anchor the scan would report a second match on rows 2..3.
    DataSchema resultSchema = new DataSchema(new String[]{"pid", "lastA"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    List<MatchNode.Measure> measures =
        List.of(measure("lastA", navigation("LAST", ColumnDataType.INT, patternRef(0, "A"), 0)));
    List<PatternSymbol> symbols = List.of(anySymbol("A"));

    List<Object[]> anchoredStart = run(node(resultSchema, symbols,
        concat(RowPattern.AnchorStart.INSTANCE, symbol(0), symbol(0)), measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAA"));
    assertEquals(lastValues(anchoredStart), List.of(1));

    // PATTERN (A A $) only matches the pair that ends at the last row of the partition.
    List<Object[]> anchoredEnd = run(node(resultSchema, symbols,
        concat(symbol(0), symbol(0), RowPattern.AnchorEnd.INSTANCE), measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAA"));
    assertEquals(lastValues(anchoredEnd), List.of(3));

    // Unanchored, the same pattern reports both pairs, which is what the anchors are being compared against.
    List<Object[]> unanchored = run(node(resultSchema, symbols, concat(symbol(0), symbol(0)), measures,
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL), rows("AAAA"));
    assertEquals(lastValues(unanchored), List.of(1, 3));
  }

  @Test
  public void testPartitionKeysAreEmittedInTheOrderTheSchemaDeclaresThem() {
    // PARTITION BY label, pid: the partition key list is not in ascending input-column order, so the operator must
    // fill output slot i from _partitionKeys[i] rather than from the i-th smallest input index. Reading the keys off
    // Calcite's ImmutableBitSet instead would put pid's Integer in the STRING slot and blow up in TypeUtils.
    DataSchema resultSchema = new DataSchema(new String[]{"label", "pid", "cnt"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures =
        List.of(measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));
    MatchNode node = new MatchNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of(anySymbol("A")),
        concat(symbol(0), symbol(0)),
        measures, List.of(MatchTestFixtures.LABEL_INDEX, MatchTestFixtures.PID_INDEX), List.of(),
        MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL,
        MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);

    List<Object[]> output = run(node, rows("AA"));

    assertEquals(output.size(), 1);
    assertEquals(output.get(0), new Object[]{"A", 0, 2L});
  }

  private static MatchNode node(DataSchema resultSchema, List<PatternSymbol> symbols, RowPattern pattern,
      List<MatchNode.Measure> measures, MatchNode.AfterMatchSkipMode skipMode, int skipToSymbolOrdinal) {
    return new MatchNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), symbols, pattern, measures,
        List.of(MatchTestFixtures.PID_INDEX), List.of(), skipMode, skipToSymbolOrdinal,
        MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
  }

  /**
   * Drains the operator and returns every output row. The operator emits a block per partition, so a test that spans
   * partitions must not stop at the first block.
   */
  private static List<Object[]> run(MatchNode node, List<Object[]> inputRows) {
    List<Object[]> outputRows = new ArrayList<>();
    MseBlock.Eos terminal = drain(node, inputRows, Map.of(), outputRows);
    assertTrue(!terminal.isError(), "Expecting a successful end of stream, got: " + terminal);
    return outputRows;
  }

  private static MseBlock execute(MatchNode node, List<Object[]> inputRows, Map<String, String> opChainMetadata) {
    return drain(node, inputRows, opChainMetadata, new ArrayList<>());
  }

  private static MseBlock.Eos drain(MatchNode node, List<Object[]> inputRows, Map<String, String> opChainMetadata,
      List<Object[]> outputRows) {
    OpChainExecutionContext context = OperatorTestUtil.getContext(opChainMetadata);
    BlockListMultiStageOperator.Builder builder = new BlockListMultiStageOperator.Builder(context, INPUT_SCHEMA);
    for (Object[] row : inputRows) {
      builder.addRow(row);
    }
    MultiStageOperator input = (inputRows.isEmpty() ? builder : builder.finishBlock()).buildWithEos();
    MatchOperator operator = new MatchOperator(context, input, INPUT_SCHEMA, node);
    while (true) {
      MseBlock block = operator.nextBlock();
      if (block.isEos()) {
        return (MseBlock.Eos) block;
      }
      outputRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
    }
  }

  private static void assertErrorContains(MseBlock block, QueryErrorCode errorCode, String fragment) {
    assertTrue(block.isError(), "Expecting an error block, got: " + block);
    String message = ((ErrorMseBlock) block).getErrorMessages().get(errorCode);
    assertTrue(message != null && message.contains(fragment),
        "Expecting " + errorCode + " containing '" + fragment + "', got: "
            + ((ErrorMseBlock) block).getErrorMessages());
  }

  private static MatchNode.Measure measure(String name, RexExpression expression) {
    return new MatchNode.Measure(name, expression);
  }

  private static RexExpression patternRef(int symbolOrdinal, String alpha) {
    return new RexExpression.PatternFieldRef(VALUE_INDEX, symbolOrdinal, alpha);
  }

  private static RexExpression literal(int value) {
    return new RexExpression.Literal(ColumnDataType.INT, value);
  }

  private static RexExpression navigation(String functionName, ColumnDataType dataType, RexExpression operand,
      int offset) {
    return new RexExpression.FunctionCall(dataType, functionName, List.of(operand, literal(offset)));
  }

  private static RexExpression aggregate(String functionName, ColumnDataType dataType, RexExpression operand) {
    return new RexExpression.FunctionCall(dataType, functionName, List.of(operand));
  }

  private static RexExpression matchNumber() {
    return new RexExpression.FunctionCall(ColumnDataType.LONG, "MATCH_NUMBER", List.of());
  }

  private static RexExpression classifier() {
    return new RexExpression.FunctionCall(ColumnDataType.STRING, "CLASSIFIER", List.of());
  }
}
