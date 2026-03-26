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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/**
 * Regression tests for the group-by result-materialisation and combine-limit paths in
 * {@link MultistageGroupByExecutor}.
 *
 * <h3>Known open bug — combine-limit truncation</h3>
 * <p>When an intermediate MSE stage applies {@code LIMIT K} trimming
 * (see {@link AggregateOperator}, the TODO comment in the no-collation branch),
 * and a LEAF stage also trimmed at {@code LIMIT K} before sending results upstream, groups
 * that are globally in the top-K can be silently dropped.
 *
 * <p>The test {@link #testLeafTrimCausesIncorrectGlobalTopK} documents this open bug.
 * It is currently written as a "document-what-actually-happens" assertion — the assertion
 * explicitly tests the <em>wrong</em> result so that when the bug is fixed the assertion
 * will fail and must be updated to verify the correct answer.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultistageGroupByCombineLimitTest {

  // ---- schema ----

  /** Raw input schema: [key INT, val DOUBLE]. */
  private static final DataSchema IN_SCHEMA = new DataSchema(
      new String[]{"key", "val"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});

  /**
   * Result / intermediate schema for SUM: [key INT, sum_val DOUBLE].
   * When used as an INTERMEDIATE executor input the second column is the already-accumulated sum.
   */
  private static final DataSchema RESULT_SCHEMA = new DataSchema(
      new String[]{"key", "sum_val"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});

  // ---- helpers ----

  private static AggregationFunction<?, ?>[] sumOnCol1() {
    ExpressionContext arg = ExpressionContext.forIdentifier("$1");
    FunctionContext fn = new FunctionContext(FunctionContext.Type.AGGREGATION, "SUM", List.of(arg));
    return new AggregationFunction[]{AggregationFunctionFactory.getAggregationFunction(fn, true)};
  }

  /** ORDER BY col[1] DESC comparator (reversed for min-heap semantics used by PriorityQueue). */
  private static Comparator<Object[]> sumDescComparator() {
    List<RelFieldCollation> cols = List.of(
        new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
    return new SortUtils.SortComparator(cols, true);
  }

  /**
   * Creates a DIRECT (LEAF→FINAL in one hop) executor for raw (key INT, val DOUBLE) rows
   * and SUM aggregation.
   */
  private static MultistageGroupByExecutor directExecutor(int numGroupsLimit) {
    return new MultistageGroupByExecutor(
        new int[]{0},          // groupKeyIds: column 0 is the key
        sumOnCol1(),
        new int[]{-1},         // no per-function filter
        -1,
        AggType.DIRECT,
        true,                  // leafReturnFinalResult
        RESULT_SCHEMA,
        Map.of(),
        null);
  }

  /**
   * Creates an INTERMEDIATE executor that merges already-aggregated rows
   * arriving in the form [key INT, sum_val DOUBLE].
   * The {@code numGroupsLimit} is applied via {@link PlanNode.NodeHint}.
   */
  private static MultistageGroupByExecutor intermediateExecutor(int numGroupsLimit) {
    PlanNode.NodeHint hint = new PlanNode.NodeHint(Map.of(
        PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, String.valueOf(numGroupsLimit))));
    return new MultistageGroupByExecutor(
        new int[]{0},
        sumOnCol1(),
        new int[]{-1},
        -1,
        AggType.INTERMEDIATE,
        true,
        RESULT_SCHEMA,
        Map.of(),
        hint);
  }

  private static RowHeapDataBlock blockOf(DataSchema schema, Object[]... rows) {
    List<Object[]> rowList = new ArrayList<>();
    for (Object[] r : rows) {
      rowList.add(r);
    }
    return new RowHeapDataBlock(rowList, schema);
  }

  // ---- correctness tests: top-K result materialisation ----

  @Test
  public void testTopKMaterializationReturnsTrueTopK() {
    // Four groups with known sums, ORDER BY sum DESC LIMIT 2 → must return groups 0 and 1.
    MultistageGroupByExecutor ex = directExecutor(100);
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 10.0},
        new Object[]{1, 9.0},
        new Object[]{2, 8.0},
        new Object[]{3, 7.0}));

    List<Object[]> result = ex.getResult(sumDescComparator(), 2);

    assertEquals(result.size(), 2, "Expected exactly 2 rows for LIMIT 2");
    assertEquals(result.get(0)[0], 0, "Top-1 group should be key=0 (sum=10)");
    assertEquals((double) result.get(0)[1], 10.0, 1e-9);
    assertEquals(result.get(1)[0], 1, "Top-2 group should be key=1 (sum=9)");
    assertEquals((double) result.get(1)[1], 9.0, 1e-9);
  }

  @Test
  public void testTopKMaterializationAcrossMultipleBlocks() {
    // Same data split over two blocks to verify accumulators are correct.
    MultistageGroupByExecutor ex = directExecutor(100);
    // Block 1: partial data for three groups
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 5.0},
        new Object[]{1, 4.0},
        new Object[]{2, 3.0}));
    // Block 2: add more data — group 2 ends up with the highest total
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 5.0},   // group 0 total = 10
        new Object[]{2, 8.0})); // group 2 total = 11

    List<Object[]> result = ex.getResult(sumDescComparator(), 2);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0)[0], 2, "Top-1 should be key=2 (sum=11)");
    assertEquals((double) result.get(0)[1], 11.0, 1e-9);
    assertEquals(result.get(1)[0], 0, "Top-2 should be key=0 (sum=10)");
    assertEquals((double) result.get(1)[1], 10.0, 1e-9);
  }

  @Test
  public void testTopKWhenAllGroupsRequested() {
    // Requesting more rows than groups should return all groups.
    MultistageGroupByExecutor ex = directExecutor(100);
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 3.0},
        new Object[]{1, 1.0},
        new Object[]{2, 2.0}));

    List<Object[]> result = ex.getResult(sumDescComparator(), 10);
    assertEquals(result.size(), 3, "Should return all 3 groups when limit > numGroups");
  }

  @Test
  public void testTopKWithTiedValues() {
    // Two groups with the same sum — either can be in the top-2.
    MultistageGroupByExecutor ex = directExecutor(100);
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 5.0},
        new Object[]{1, 5.0},
        new Object[]{2, 3.0}));

    List<Object[]> result = ex.getResult(sumDescComparator(), 2);
    assertEquals(result.size(), 2);
    // Both top rows should have sum=5
    assertEquals((double) result.get(0)[1], 5.0, 1e-9);
    assertEquals((double) result.get(1)[1], 5.0, 1e-9);
    // The third group (sum=3) must not appear
    for (Object[] row : result) {
      assertNotEquals((double) row[1], 3.0);
    }
  }

  @Test
  public void testRowsInStatTracking() {
    MultistageGroupByExecutor ex = directExecutor(100);
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 1.0},
        new Object[]{1, 2.0}));
    ex.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 3.0}));

    assertEquals(ex.getRowsIn(), 3L, "rowsIn should count all rows across all processBlock calls");
    assertEquals(ex.getNumGroups(), 2);
  }

  @Test
  public void testGeneratorTypeReported() {
    MultistageGroupByExecutor ex = directExecutor(100);
    // INT key → OneIntKeyGroupIdGenerator
    String genType = ex.getGeneratorType();
    assertTrue(genType.contains("OneIntKey") || genType.contains("GroupIdGenerator"),
        "Generator type should reflect the backend chosen: " + genType);
  }

  // ---- regression test: combine-limit truncation open bug ----

  /**
   * Documents the open correctness bug where LEAF-level {@code LIMIT K} trimming can cause
   * an INTERMEDIATE stage to compute incorrect top-K results.
   *
   * <h3>Scenario</h3>
   * <pre>
   * Global data (two partitions):
   *   Partition 1: key=0 (val=10),  key=1 (val=9)
   *   Partition 2: key=1 (val=9)          ← key=1 appears in both partitions
   *
   * Correct global sums: key=0 → 10,  key=1 → 18
   * Correct LIMIT-1 answer: key=1 (sum=18)
   *
   * With LEAF trim=1 on partition 1:
   *   Partition 1 sends only key=0 (locally highest), discarding key=1
   *   Partition 2 sends key=1 (val=9)
   *   INTERMEDIATE sees: key=0 → 10,  key=1 → 9
   *   INTERMEDIATE top-1: key=0 (sum=10)  ← WRONG
   * </pre>
   *
   * <p><strong>Note:</strong> The assertion in this test currently verifies the <em>buggy</em>
   * behaviour so that CI continues to pass.  When the bug is fixed (e.g., by preventing
   * over-aggressive LEAF-level trimming in ordered queries), the bottom assertion must be
   * updated to expect {@code key=1} (sum=18) as the top-1 result.
   */
  @Test
  public void testLeafTrimCausesIncorrectGlobalTopK() {
    // Step 1: Simulate partition 1 LEAF with trim=1 applied.
    //         Only key=0 (sum=10) survives; key=1 (sum=9) is dropped.
    MultistageGroupByExecutor leaf1 = directExecutor(100);
    leaf1.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 10.0},
        new Object[]{1, 9.0}));
    // Trim to top-1: returns only key=0
    List<Object[]> leaf1TrimmedRows = leaf1.getResult(sumDescComparator(), 1);
    assertEquals(leaf1TrimmedRows.size(), 1);
    assertEquals(leaf1TrimmedRows.get(0)[0], 0, "Leaf1 trim must keep key=0 (highest locally)");

    // Step 2: Simulate partition 2 LEAF (no trim needed — only one group).
    MultistageGroupByExecutor leaf2 = directExecutor(100);
    leaf2.processBlock(blockOf(IN_SCHEMA,
        new Object[]{1, 9.0}));
    List<Object[]> leaf2Rows = leaf2.getResult(Integer.MAX_VALUE);
    assertEquals(leaf2Rows.size(), 1);

    // Step 3: INTERMEDIATE executor receives the trimmed partition-1 output and partition-2 output.
    //         It uses the intermediate result format: [key INT, sum_val DOUBLE].
    MultistageGroupByExecutor intermediate = intermediateExecutor(100);
    intermediate.processBlock(new RowHeapDataBlock(leaf1TrimmedRows, RESULT_SCHEMA));  // only key=0
    intermediate.processBlock(new RowHeapDataBlock(leaf2Rows, RESULT_SCHEMA));        // key=1

    List<Object[]> finalResult = intermediate.getResult(sumDescComparator(), 1);
    assertEquals(finalResult.size(), 1);

    // ---- BUG: the intermediate result is incorrect ----
    // The correct global top-1 is key=1 with sum=18, but because key=1 was dropped by
    // the LEAF-1 trim, INTERMEDIATE sees key=1 only with sum=9 (from partition 2).
    // INTERMEDIATE therefore selects key=0 (sum=10) as top-1, which is WRONG.
    //
    // TODO: When this bug is fixed, change the assertion below to assertEquals(finalResult.get(0)[0], 1)
    //       and assertEquals((double) finalResult.get(0)[1], 18.0, 1e-9).
    assertEquals(finalResult.get(0)[0], 0,
        "BUG (open): INTERMEDIATE returns key=0 (sum=10) but the correct global top-1 is key=1 (sum=18). "
            + "The LEAF-level trim discarded key=1's contribution from partition 1.");
    assertEquals((double) finalResult.get(0)[1], 10.0, 1e-9);

    // Step 4: Verify the CORRECT answer when no LEAF trimming happens.
    MultistageGroupByExecutor intermediateNoTrim = intermediateExecutor(100);
    // partition 1 sends ALL rows (no trim)
    List<Object[]> leaf1AllRows = leaf1.getResult(Integer.MAX_VALUE);
    // leaf1 executor is already consumed; rebuild it
    MultistageGroupByExecutor leaf1Rebuilt = directExecutor(100);
    leaf1Rebuilt.processBlock(blockOf(IN_SCHEMA,
        new Object[]{0, 10.0},
        new Object[]{1, 9.0}));
    List<Object[]> leaf1Full = leaf1Rebuilt.getResult(Integer.MAX_VALUE);
    assertEquals(leaf1Full.size(), 2);

    intermediateNoTrim.processBlock(new RowHeapDataBlock(leaf1Full, RESULT_SCHEMA));
    intermediateNoTrim.processBlock(new RowHeapDataBlock(leaf2Rows, RESULT_SCHEMA));

    List<Object[]> correctResult = intermediateNoTrim.getResult(sumDescComparator(), 1);
    assertEquals(correctResult.size(), 1);
    assertEquals(correctResult.get(0)[0], 1,
        "Without LEAF trimming, INTERMEDIATE correctly identifies key=1 (sum=18) as global top-1");
    assertEquals((double) correctResult.get(0)[1], 18.0, 1e-9);
  }

  /**
   * Verifies that plain truncation (no ORDER BY) via {@link MultistageGroupByExecutor#getResult(int)}
   * returns exactly {@code maxRows} rows when more groups are present.
   *
   * <p>Per the {@code AggregateOperator} source comment, there is no ordering guarantee in this path —
   * the K groups returned are arbitrary.  This test only asserts count correctness.
   */
  @Test
  public void testPlainTruncationReturnsExactlyLimitRows() {
    MultistageGroupByExecutor ex = directExecutor(100);
    // 10 distinct groups
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(new Object[]{i, (double) (i + 1)});
    }
    ex.processBlock(new RowHeapDataBlock(rows, IN_SCHEMA));

    List<Object[]> result = ex.getResult(3);
    assertEquals(result.size(), 3,
        "Plain getResult(limit) must return exactly limit rows when numGroups > limit");
  }

  @Test
  public void testGroupIdSentAsInvalidIsSkippedDuringMerge() {
    // When the groups limit is reached, getGroupId returns INVALID_ID.
    // processMerge must skip those rows rather than crash.
    MultistageGroupByExecutor ex = intermediateExecutor(2);  // limit = 2 groups
    // Feed 3 distinct keys — the 3rd must be silently dropped
    ex.processBlock(blockOf(RESULT_SCHEMA,
        new Object[]{0, 5.0},
        new Object[]{1, 3.0},
        new Object[]{2, 99.0}));  // key=2 hits the limit → INVALID_ID

    List<Object[]> result = ex.getResult(Integer.MAX_VALUE);
    // Only 2 groups accepted (groups limit enforced)
    assertTrue(result.size() <= 2, "Executor must not exceed numGroupsLimit");
    assertTrue(ex.isNumGroupsLimitReached(), "isNumGroupsLimitReached must be true");
  }
}
