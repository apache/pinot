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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxObject;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Null handling for `EXPR_MIN` and `EXPR_MAX`.
///
/// These cannot be reached through [AggregationFunctionNullContractTest]: the factory rejects `EXPRMIN` and `EXPRMAX`
/// outright, since they are only legal in a selection without an alias and are rewritten into a parent/child pair
/// before execution. So the parent is driven directly here.
public class ExprMinMaxNullHandlingTest {
  private static final ExpressionContext MEASURING = ExpressionContext.forIdentifier("measuring");
  private static final ExpressionContext PROJECTION = ExpressionContext.forIdentifier("projection");

  @Test
  public void nullMeasuringValueDoesNotWinWhenNullHandlingEnabled() {
    // Row 1 holds the smallest value but is null, so row 0 is the minimum
    ExprMinMaxObject result = aggregate(false, true, nulls(1), new int[]{10, 3, 20}, new int[]{100, 101, 102});

    assertEquals(result.getExtremumKey()[0], 10);
    assertEquals(result.getNumberOfRows(), 1);
    assertEquals(result.getField(0, 0), 100);
  }

  @Test
  public void nullMeasuringValueWinsWhenNullHandlingDisabled() {
    ExprMinMaxObject result = aggregate(false, false, nulls(1), new int[]{10, 3, 20}, new int[]{100, 101, 102});

    assertEquals(result.getExtremumKey()[0], 3);
    assertEquals(result.getField(0, 0), 101);
  }

  @Test
  public void nullMeasuringValueDoesNotWinForMax() {
    ExprMinMaxObject result = aggregate(true, true, nulls(2), new int[]{10, 3, 20}, new int[]{100, 101, 102});

    assertEquals(result.getExtremumKey()[0], 10);
    assertEquals(result.getField(0, 0), 100);
  }

  @Test
  public void everyRowNullMeasuresNothing() {
    ExprMinMaxObject result = aggregate(false, true, nulls(0, 1, 2), new int[]{10, 3, 20}, new int[]{100, 101, 102});

    assertEquals(result.getNumberOfRows(), 0);
  }

  @Test
  public void tiedRowsAllProjectWhenTheirMeasuringValueIsNotNull() {
    ExprMinMaxObject result = aggregate(false, true, nulls(), new int[]{10, 10, 20}, new int[]{100, 101, 102});

    assertEquals(result.getExtremumKey()[0], 10);
    assertEquals(result.getNumberOfRows(), 2);
    assertEquals(result.getField(0, 0), 100);
    assertEquals(result.getField(1, 0), 101);
  }

  /// A second block must be read with its own values.
  ///
  /// The wrappers hold the block they were last bound to, so a function that only binds them while creating its
  /// accumulator reads the first block's values at the second block's row offsets.
  @Test
  public void secondBlockIsReadWithItsOwnValues() {
    ParentExprMinMaxAggregationFunction function = function(false, true);
    AggregationResultHolder holder = function.createAggregationResultHolder();

    function.aggregate(2, holder, block(nulls(), new int[]{10, 20}, new int[]{100, 101}));
    function.aggregate(2, holder, block(nulls(), new int[]{3, 30}, new int[]{200, 201}));

    ExprMinMaxObject result = function.extractAggregationResult(holder);
    assertEquals(result.getExtremumKey()[0], 3);
    assertEquals(result.getNumberOfRows(), 1);
    assertEquals(result.getField(0, 0), 200);
  }

  /// A block that replaces the key must discard what the previous key projected, and still keep the rows that tie the
  /// new key. `rowIds.clear()` cannot do the first part: it only drops candidates found within the current block.
  @Test
  public void aReplacedKeyDiscardsTheEarlierBlocksProjectionButKeepsItsOwnTies() {
    ParentExprMinMaxAggregationFunction function = function(false, true);
    AggregationResultHolder holder = function.createAggregationResultHolder();

    function.aggregate(2, holder, block(nulls(), new int[]{10, 20}, new int[]{100, 101}));
    function.aggregate(3, holder, block(nulls(), new int[]{3, 3, 30}, new int[]{200, 201, 202}));

    ExprMinMaxObject result = function.extractAggregationResult(holder);
    assertEquals(result.getExtremumKey()[0], 3);
    assertEquals(result.getNumberOfRows(), 2);
    assertEquals(result.getField(0, 0), 200);
    assertEquals(result.getField(1, 0), 201);
  }

  @Test
  public void nullMeasuringValueDoesNotWinInItsGroup() {
    ParentExprMinMaxAggregationFunction function = function(false, true);
    GroupByResultHolder holder = function.createGroupByResultHolder(2, 2);

    // Rows 0 and 1 are group 0; row 1 holds the smaller value but is null
    function.aggregateGroupBySV(4, new int[]{0, 0, 1, 1}, holder,
        block(nulls(1), new int[]{10, 3, 20, 30}, new int[]{100, 101, 102, 103}));

    ExprMinMaxObject group0 = function.extractGroupByResult(holder, 0);
    assertEquals(group0.getExtremumKey()[0], 10);
    assertEquals(group0.getNumberOfRows(), 1);
    assertEquals(group0.getField(0, 0), 100);

    ExprMinMaxObject group1 = function.extractGroupByResult(holder, 1);
    assertEquals(group1.getExtremumKey()[0], 20);
    assertEquals(group1.getField(0, 0), 102);
  }

  @Test
  public void nullMeasuringValueDoesNotWinInAnyOfItsGroups() {
    ParentExprMinMaxAggregationFunction function = function(false, true);
    GroupByResultHolder holder = function.createGroupByResultHolder(2, 2);

    // Row 1 belongs to both groups and holds the smaller value, but is null
    function.aggregateGroupByMV(2, new int[][]{{0, 1}, {0, 1}}, holder,
        block(nulls(1), new int[]{10, 3}, new int[]{100, 101}));

    assertEquals(function.extractGroupByResult(holder, 0).getExtremumKey()[0], 10);
    assertEquals(function.extractGroupByResult(holder, 0).getField(0, 0), 100);
    assertEquals(function.extractGroupByResult(holder, 1).getExtremumKey()[0], 10);
    assertEquals(function.extractGroupByResult(holder, 1).getField(0, 0), 100);
  }

  /// The group-by path publishes each winning row as it is found rather than batching them, so a replaced key clears
  /// the earlier block's projection through `setToNewVal` on its own. Pinned because the non-group-by path needed a
  /// fix to reach the same behaviour.
  @Test
  public void aReplacedKeyInAGroupDiscardsTheEarlierBlocksProjection() {
    ParentExprMinMaxAggregationFunction function = function(false, true);
    GroupByResultHolder holder = function.createGroupByResultHolder(2, 2);

    function.aggregateGroupBySV(2, new int[]{0, 0}, holder, block(nulls(), new int[]{10, 20}, new int[]{100, 101}));
    function.aggregateGroupBySV(2, new int[]{0, 0}, holder, block(nulls(), new int[]{3, 30}, new int[]{200, 201}));

    ExprMinMaxObject group0 = function.extractGroupByResult(holder, 0);
    assertEquals(group0.getExtremumKey()[0], 3);
    assertEquals(group0.getNumberOfRows(), 1);
    assertEquals(group0.getField(0, 0), 200);
  }

  private static ExprMinMaxObject aggregate(boolean isMax, boolean nullHandlingEnabled, RoaringBitmap measuringNulls,
      int[] measuring, int[] projection) {
    ParentExprMinMaxAggregationFunction function = function(isMax, nullHandlingEnabled);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(measuring.length, holder, block(measuringNulls, measuring, projection));
    return function.extractAggregationResult(holder);
  }

  private static ParentExprMinMaxAggregationFunction function(boolean isMax, boolean nullHandlingEnabled) {
    return new ParentExprMinMaxAggregationFunction(List.of(
        ExpressionContext.forLiteral(Literal.intValue(0)),
        ExpressionContext.forLiteral(Literal.intValue(1)),
        MEASURING,
        PROJECTION), isMax, nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> block(RoaringBitmap measuringNulls, int[] measuring,
      int[] projection) {
    return Map.of(
        MEASURING, SyntheticBlockValSets.Int.create(measuringNulls, measuring),
        PROJECTION, SyntheticBlockValSets.Int.create(null, projection)
    );
  }

  private static RoaringBitmap nulls(int... indexes) {
    return indexes.length == 0 ? null : RoaringBitmap.bitmapOf(indexes);
  }
}
