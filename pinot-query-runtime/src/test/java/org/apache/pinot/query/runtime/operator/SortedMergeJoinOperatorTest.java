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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link SortedMergeJoinOperator}. Inputs are always provided pre-sorted (ascending) on the join keys,
 * which is the operator's precondition. Several tests assert that the streaming sorted merge join produces the same
 * result multiset as {@link HashJoinOperator} for the same inputs.
 */
public class SortedMergeJoinOperatorTest {
  private static final DataSchema CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
  private static final DataSchema RESULT_SCHEMA = new DataSchema(
      new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

  @Test
  public void shouldHandleInnerJoinOnInt() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(rows.get(1), new Object[]{2, "BB", 2, "BB"});
  }

  @Test
  public void shouldHandleKeyCollisionOneToMany() {
    // Inputs sorted ascending on the string join key (column 1).
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(rows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(rows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleManyToMany() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    // 2 x 2 cross product for the single shared key.
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, "k", 10, "k"});
    assertEquals(rows.get(1), new Object[]{1, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 10, "k"});
    assertEquals(rows.get(3), new Object[]{2, "k", 20, "k"});
  }

  @Test
  public void shouldHandleLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "CC")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(rows.get(1), new Object[]{2, "CC", null, null});
  }

  @Test
  public void shouldHandleEmptyLeftInput() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    assertEquals(drain(operator).size(), 0);
  }

  @Test
  public void shouldHandleEmptyRightInputInnerJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    assertEquals(drain(operator).size(), 0);
  }

  @Test
  public void shouldHandleLongKeysNearPrecisionBoundary() {
    DataSchema schema = new DataSchema(new String[]{"long_col", "string_col"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING});
    DataSchema resultSchema = new DataSchema(
        new String[]{"l_long", "l_str", "r_long", "r_str"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.STRING});
    long base = 1L << 53;
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(schema)
        .addRow(base, "a")
        .addRow(base + 1, "b")
        .addRow(base + 2, "c")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(schema)
        .addRow(base + 1, "x")
        .addRow(base + 2, "y")
        .buildWithEos();
    SortedMergeJoinOperator operator = new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left, schema,
        right, new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.INNER, List.of(0),
            List.of(0), List.of(), JoinNode.JoinStrategy.SORTED));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{base + 1, "b", base + 1, "x"});
    assertEquals(rows.get(1), new Object[]{base + 2, "c", base + 2, "y"});
  }

  @Test
  public void shouldRespectMaxRowsInJoinThrowMode() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .addRow(3, "k")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1), nodeHint);
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError(), "THROW overflow mode should produce an error block");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void shouldRejectRightJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    getOperator(left, right, RESULT_SCHEMA, JoinRelType.RIGHT, List.of(0), List.of(0));
  }

  @Test
  public void shouldHandleEmptyRightInputLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(0), List.of(0));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "Aa", null, null});
    assertEquals(rows.get(1), new Object[]{2, "BB", null, null});
  }

  @Test
  public void shouldHandleMultiBlockStreamingInputs() {
    // Rows are split across multiple blocks on both sides; the merge must span block boundaries, and a single key
    // run ("k") must be assembled from rows arriving in different blocks.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "k")
        .finishBlock()
        .addRow(3, "k")
        .addRow(4, "z")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "a")
        .finishBlock()
        .addRow(20, "k")
        .finishBlock()
        .addRow(30, "k")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 5);
    assertEquals(rows.get(0), new Object[]{1, "a", 10, "a"});
    assertEquals(rows.get(1), new Object[]{2, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 30, "k"});
    assertEquals(rows.get(3), new Object[]{3, "k", 20, "k"});
    assertEquals(rows.get(4), new Object[]{3, "k", 30, "k"});
  }

  @Test
  public void shouldHandleMultiColumnKey() {
    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    DataSchema resultSchema = new DataSchema(
        new String[]{"l_int", "l_str", "r_int", "r_str"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    // Sorted ascending on (int_col, string_col).
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "a")
        .addRow(1, "b")
        .addRow(2, "a")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "a")
        .addRow(1, "c")
        .addRow(2, "a")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, resultSchema, JoinRelType.INNER, List.of(0, 1), List.of(0, 1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "a", 1, "a"});
    assertEquals(rows.get(1), new Object[]{2, "a", 2, "a"});
  }

  @Test
  public void shouldExcludeNullKeysFromInnerJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, null)
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(3, null)
        .addRow(4, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{2, "BB", 4, "BB"});
  }

  @Test
  public void shouldPreserveNullKeyLeftRowsInLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, null)
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(3, null)
        .addRow(4, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertTrue(containsRow(rows, new Object[]{1, null, null, null}));
    assertTrue(containsRow(rows, new Object[]{2, "BB", 4, "BB"}));
  }

  /// Regression: the planner asks for NULLS LAST collation on both join inputs
  /// (`PinotJoinExchangeNodeInsertRule`), so a null-key row arrives immediately *after* the last non-null run — which
  /// is exactly where the run-scanning loops call `compareKeys` on it. A key comparator that unboxes or calls
  /// `compareTo` without a null check throws NPE there and fails the query for any nullable join key. The two tests
  /// above place nulls first, the opposite of the real ordering, so they cannot catch this.
  @Test
  public void shouldExcludeNullKeysSortedLastFromInnerJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "BB")
        .addRow(1, null)
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(4, "BB")
        .addRow(3, null)
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{2, "BB", 4, "BB"});
  }

  @Test
  public void shouldPreserveNullKeysSortedLastInLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "BB")
        .addRow(1, null)
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(4, "BB")
        .addRow(3, null)
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertTrue(containsRow(rows, new Object[]{2, "BB", 4, "BB"}));
    assertTrue(containsRow(rows, new Object[]{1, null, null, null}));
  }

  /// A null key on the right side alone, sorted last, must terminate the right run without being dereferenced.
  @Test
  public void shouldTerminateRightRunAtNullKeySortedLast() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "AA")
        .addRow(11, "AA")
        .addRow(12, "BB")
        .addRow(13, null)
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 3);
    assertTrue(containsRow(rows, new Object[]{1, "AA", 10, "AA"}));
    assertTrue(containsRow(rows, new Object[]{1, "AA", 11, "AA"}));
    assertTrue(containsRow(rows, new Object[]{2, "BB", 12, "BB"}));
  }

  @Test
  public void shouldRespectMaxRowsInJoinBreakMode() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .addRow(3, "k")
        .addRow(4, "k")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "5")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1), nodeHint);
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 5, "Should emit exactly the limit then break (potential 4 x 2 = 8)");
    StatMap<SortedMergeJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(SortedMergeJoinOperator.StatKey.class, operator.calculateStats());
    assertTrue(statMap.getBoolean(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
  }

  /**
   * Guards against the sorted merge join ignoring downstream early termination. The inputs are large enough to span
   * several output blocks (many distinct keys, 1:1 matches), so after the first data block there are still more blocks
   * to produce. Before the fix, calling {@link MultiStageOperator#earlyTerminate()} had no effect and the next
   * {@code nextBlock()} would return another data block from the remaining input; now it must return a clean success
   * EOS promptly.
   */
  @Test
  public void shouldStopProducingAfterEarlyTerminate() {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    // Distinct, ascending, zero-padded string keys so lexicographic order matches insertion order (the operator
    // requires inputs pre-sorted on the join key). 4000 keys with 1:1 matches produce ~4 output blocks.
    for (int i = 0; i < 4000; i++) {
      String key = String.format("k%04d", i);
      leftBuilder.addRow(i, key);
      rightBuilder.addRow(i, key);
    }
    MultiStageOperator left = leftBuilder.buildWithEos();
    MultiStageOperator right = rightBuilder.buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));

    MseBlock first = operator.nextBlock();
    assertTrue(first.isData());

    operator.earlyTerminate();

    MseBlock next = operator.nextBlock();
    assertTrue(next.isEos());
    assertFalse(next.isError());
    assertSame(next, SuccessMseBlock.INSTANCE);
  }

  @Test
  public void shouldMatchHashJoinResultsForRandomizedInnerJoin() {
    assertParityWithHashJoin(JoinRelType.INNER, 12345L);
  }

  @Test
  public void shouldMatchHashJoinResultsForRandomizedLeftJoin() {
    assertParityWithHashJoin(JoinRelType.LEFT, 67890L);
  }

  private void assertParityWithHashJoin(JoinRelType joinType, long seed) {
    Random random = new Random(seed);
    List<Object[]> leftRows = randomRows(random, 200, 40);
    List<Object[]> rightRows = randomRows(random, 200, 40);
    Object[][] sortedLeft = sortByKey(leftRows).toArray(new Object[0][]);
    Object[][] sortedRight = sortByKey(rightRows).toArray(new Object[0][]);

    // Sorted merge join (requires sorted inputs).
    SortedMergeJoinOperator sortedOperator = getOperator(
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedLeft).buildWithEos(),
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedRight).buildWithEos(),
        RESULT_SCHEMA, joinType, List.of(0), List.of(0));
    List<Object[]> sortedResult = drain(sortedOperator);

    // Hash join over the same data (order-independent).
    HashJoinOperator hashOperator = new HashJoinOperator(OperatorTestUtil.getTracingContext(),
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedLeft).buildWithEos(), CHILD_SCHEMA,
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedRight).buildWithEos(),
        new JoinNode(-1, RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), joinType, List.of(0), List.of(0),
            List.of(), JoinNode.JoinStrategy.HASH));
    List<Object[]> hashResult = drain(hashOperator);

    assertEquals(toMultiset(sortedResult), toMultiset(hashResult),
        "Sorted merge join must produce the same result multiset as hash join");
    // Sorted merge join output must be ordered by the (int) join key.
    for (int i = 1; i < sortedResult.size(); i++) {
      Integer prev = (Integer) sortedResult.get(i - 1)[0];
      Integer curr = (Integer) sortedResult.get(i)[0];
      assertTrue(prev <= curr, "Sorted merge join output must be ascending on the join key");
    }
  }

  private static List<Object[]> randomRows(Random random, int numRows, int keyRange) {
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(new Object[]{random.nextInt(keyRange), "v" + i});
    }
    return rows;
  }

  private static List<Object[]> sortByKey(List<Object[]> rows) {
    List<Object[]> copy = new ArrayList<>(rows);
    copy.sort(Comparator.comparingInt(r -> (Integer) r[0]));
    return copy;
  }

  private static List<String> toMultiset(List<Object[]> rows) {
    List<String> repr = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      repr.add(Arrays.toString(row));
    }
    repr.sort(Comparator.naturalOrder());
    return repr;
  }

  private static boolean containsRow(List<Object[]> rows, Object[] expected) {
    for (Object[] row : rows) {
      if (Arrays.equals(row, expected)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void shouldPropagateLeftInputError() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testLeftError")));
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "b")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages()
        .get(QueryErrorCode.UNKNOWN).contains("testLeftError"));
  }

  @Test
  public void shouldPropagateRightInputError() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "b")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testRightError")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages()
        .get(QueryErrorCode.UNKNOWN).contains("testRightError"));
  }

  @Test
  public void shouldHandleLeftJoinManyToMany() {
    // LEFT JOIN with multiple left rows and multiple right rows sharing the same key, plus an additional key
    // to verify correct cursor advancement after the many-to-many group.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .addRow(3, "m")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .addRow(30, "m")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    // 2 left x 2 right for "k" = 4 rows, plus 1 left x 1 right for "m" = 1 row => 5 total
    assertEquals(rows.size(), 5);
    assertEquals(rows.get(0), new Object[]{1, "k", 10, "k"});
    assertEquals(rows.get(1), new Object[]{1, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 10, "k"});
    assertEquals(rows.get(3), new Object[]{2, "k", 20, "k"});
    assertEquals(rows.get(4), new Object[]{3, "m", 30, "m"});
  }

  @Test
  public void shouldHandleLeftJoinManyToManyAcrossBlocks() {
    // LEFT JOIN where left rows sharing a key span across block boundaries, and the right run also spans blocks.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .finishBlock()
        .addRow(2, "k")
        .addRow(3, "m")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .finishBlock()
        .addRow(20, "k")
        .addRow(30, "m")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    // 2 left x 2 right for "k" = 4 rows, plus 1 left x 1 right for "m" = 1 row => 5 total
    assertEquals(rows.size(), 5);
    assertEquals(rows.get(0), new Object[]{1, "k", 10, "k"});
    assertEquals(rows.get(1), new Object[]{1, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 10, "k"});
    assertEquals(rows.get(3), new Object[]{2, "k", 20, "k"});
    assertEquals(rows.get(4), new Object[]{3, "m", 30, "m"});
  }

  @Test
  public void shouldApplyNonEquiCondition() {
    // Both sides share key=1 with different string values; the non-equi condition filters some matches.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(1, "b")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(1, "c")
        .buildWithEos();
    // Non-equi condition: left.string_col (result col 1) != right.string_col (result col 3)
    RexExpression nonEqui = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(),
        List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(3)));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0), List.of(nonEqui));
    List<Object[]> rows = drain(operator);
    // (1,"a") x (1,"c") matches (a != c), (1,"b") x (1,"a") matches (b != a), (1,"b") x (1,"c") matches (b != c)
    // (1,"a") x (1,"a") does NOT match (a == a)
    assertEquals(rows.size(), 3);
  }

  private static List<Object[]> drain(MultiStageOperator operator) {
    List<Object[]> rows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertFalse(block.isError(), "Did not expect an error block");
    return rows;
  }

  private SortedMergeJoinOperator getOperator(MultiStageOperator left, MultiStageOperator right,
      DataSchema resultSchema, JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys) {
    return getOperator(left, right, resultSchema, joinType, leftKeys, rightKeys, PlanNode.NodeHint.EMPTY);
  }

  private SortedMergeJoinOperator getOperator(MultiStageOperator left, MultiStageOperator right,
      DataSchema resultSchema, JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys,
      PlanNode.NodeHint nodeHint) {
    return new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left, CHILD_SCHEMA, right,
        new JoinNode(-1, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, List.of(),
            JoinNode.JoinStrategy.SORTED));
  }

  private SortedMergeJoinOperator getOperator(MultiStageOperator left, MultiStageOperator right,
      DataSchema resultSchema, JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys,
      List<RexExpression> nonEquiConditions) {
    return new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left, CHILD_SCHEMA, right,
        new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), joinType, leftKeys, rightKeys,
            nonEquiConditions, JoinNode.JoinStrategy.SORTED));
  }
}
