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
package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Null handling for the window funnel functions.
///
/// A funnel reads more than one column per row, so it never received the option through
/// [org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction] and is invisible to
/// [org.apache.pinot.core.query.aggregation.function.AggregationFunctionNullContractTest], whose synthetic block
/// supplies a single column.
///
/// Only the timestamp is consulted. A step expression is a predicate, and a predicate over a null operand is
/// UNKNOWN, which SQL treats as not satisfied wherever a boolean is consumed, so a null step already means that
/// step did not match. A null timestamp is different: the event has no position in the window, and today it is read
/// as the column default, which places a real step event at a fabricated time.
public class FunnelNullHandlingTest {
  private static final ExpressionContext TIMESTAMP = ExpressionContext.forIdentifier("ts");
  private static final ExpressionContext STEP_0 = ExpressionContext.forIdentifier("step0");
  private static final ExpressionContext STEP_1 = ExpressionContext.forIdentifier("step1");

  /// `funnelMaxStep(ts, '1000', 2, step0, step1)`, optionally with extra arguments such as `MODE=KEEP_ALL`.
  private static FunnelMaxStepAggregationFunction maxStep(boolean nullHandlingEnabled, String... extraArguments) {
    List<ExpressionContext> arguments = new ArrayList<>(List.of(TIMESTAMP,
        ExpressionContext.forLiteral(Literal.longValue(1000)),
        ExpressionContext.forLiteral(Literal.intValue(2)), STEP_0, STEP_1));
    for (String extraArgument : extraArguments) {
      arguments.add(ExpressionContext.forLiteral(Literal.stringValue(extraArgument)));
    }
    return new FunnelMaxStepAggregationFunction(arguments, nullHandlingEnabled);
  }

  /// Timestamps carry the null bitmap; the two step columns are the already-evaluated predicate results.
  private static Map<ExpressionContext, BlockValSet> block(RoaringBitmap timestampNulls, long[] timestamps,
      int[] step0, int[] step1) {
    return Map.of(
        TIMESTAMP, SyntheticBlockValSets.Long.create(timestampNulls, timestamps),
        STEP_0, SyntheticBlockValSets.Int.create(null, step0),
        STEP_1, SyntheticBlockValSets.Int.create(null, step1));
  }

  /// A row whose timestamp is null contributes no step event, so the funnel that row would have completed does not
  /// complete.
  @Test
  public void testNullTimestampRowContributesNoEvent() {
    long[] timestamps = {100L, 200L};
    int[] step0 = {1, 0};
    int[] step1 = {0, 1};

    FunnelMaxStepAggregationFunction enabled = maxStep(true);
    AggregationResultHolder holder = enabled.createAggregationResultHolder();
    enabled.aggregate(2, holder, block(RoaringBitmap.bitmapOf(1), timestamps, step0, step1));
    assertEquals(enabled.extractFinalResult(enabled.extractAggregationResult(holder)), Integer.valueOf(1));

    // With the option disabled the null bitmap is ignored and row 1 still completes the funnel, which is the answer
    // this mode has always given
    FunnelMaxStepAggregationFunction disabled = maxStep(false);
    AggregationResultHolder disabledHolder = disabled.createAggregationResultHolder();
    disabled.aggregate(2, disabledHolder, block(RoaringBitmap.bitmapOf(1), timestamps, step0, step1));
    assertEquals(disabled.extractFinalResult(disabled.extractAggregationResult(disabledHolder)), Integer.valueOf(2));
  }

  /// Every row null leaves the queue empty rather than filling it with events at fabricated timestamps.
  @Test
  public void testEveryRowNullYieldsNoEvents() {
    long[] timestamps = {100L, 200L};
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, 2L);

    FunnelMaxStepAggregationFunction function = maxStep(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, block(allNull, timestamps, new int[]{1, 0}, new int[]{0, 1}));

    PriorityQueue<FunnelStepEvent> events = function.extractAggregationResult(holder);
    assertNotNull(events, "the holder is created before the range, so it stays non-null");
    assertEquals(events.size(), 0);
    assertEquals(function.extractFinalResult(events), Integer.valueOf(0));
  }

  /// In KEEP_ALL mode a row that matches no step still produces a dummy event - but only if it has a timestamp to
  /// place it at.
  @Test
  public void testKeepAllDoesNotFabricateAnEventForANullTimestamp() {
    long[] timestamps = {100L, 200L};
    int[] noMatch = {0, 0};

    FunnelMaxStepAggregationFunction enabled = maxStep(true, "MODE=KEEP_ALL");
    AggregationResultHolder holder = enabled.createAggregationResultHolder();
    enabled.aggregate(2, holder, block(RoaringBitmap.bitmapOf(1), timestamps, new int[]{1, 0}, noMatch));
    assertEquals(enabled.extractAggregationResult(holder).size(), 1);

    FunnelMaxStepAggregationFunction disabled = maxStep(false, "MODE=KEEP_ALL");
    AggregationResultHolder disabledHolder = disabled.createAggregationResultHolder();
    disabled.aggregate(2, disabledHolder, block(RoaringBitmap.bitmapOf(1), timestamps, new int[]{1, 0}, noMatch));
    assertEquals(disabled.extractAggregationResult(disabledHolder).size(), 2);
  }

  /// The group-by path skips the null row for its own group only.
  @Test
  public void testGroupBySVSkipsTheNullRow() {
    long[] timestamps = {100L, 200L};

    FunnelMaxStepAggregationFunction function = maxStep(true);
    GroupByResultHolder holder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(2, new int[]{0, 1}, holder,
        block(RoaringBitmap.bitmapOf(1), timestamps, new int[]{1, 1}, new int[]{0, 0}));

    PriorityQueue<FunnelStepEvent> group0 = function.extractGroupByResult(holder, 0);
    assertNotNull(group0);
    assertEquals(group0.size(), 1);
    assertNull(function.extractGroupByResult(holder, 1));
  }

  /// A null row is skipped for every group key it would have fed.
  @Test
  public void testGroupByMVSkipsTheNullRowForAllKeys() {
    long[] timestamps = {100L, 200L};

    FunnelMaxStepAggregationFunction function = maxStep(true);
    GroupByResultHolder holder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(2, new int[][]{{0}, {0, 1}}, holder,
        block(RoaringBitmap.bitmapOf(1), timestamps, new int[]{1, 1}, new int[]{0, 0}));

    PriorityQueue<FunnelStepEvent> group0 = function.extractGroupByResult(holder, 0);
    assertNotNull(group0);
    assertEquals(group0.size(), 1);
    assertNull(function.extractGroupByResult(holder, 1));
  }

  // ---------- FUNNELCOUNT ----------

  private static final ExpressionContext USER = ExpressionContext.forIdentifier("userId");

  /// `funnelCount(steps(step0, step1), correlateby(userId))`, built through the same factory
  /// AggregationFunctionFactory uses.
  private static AggregationFunction<?, LongArrayList> funnelCount(boolean nullHandlingEnabled) {
    List<ExpressionContext> arguments = List.of(
        ExpressionContext.forFunction(
            new FunctionContext(FunctionContext.Type.TRANSFORM, "steps", List.of(STEP_0, STEP_1))),
        ExpressionContext.forFunction(
            new FunctionContext(FunctionContext.Type.TRANSFORM, "correlateby", List.of(USER))));
    return new FunnelCountAggregationFunctionFactory(arguments, nullHandlingEnabled).get();
  }

  /// Two users, ids 0 and 1, so a dictionary id doubles as the user it stands for.
  private static Dictionary userDictionary() {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.getIntValue(anyInt())).thenAnswer(invocation -> invocation.getArgument(0));
    return dictionary;
  }

  private static Map<ExpressionContext, BlockValSet> funnelCountBlock(RoaringBitmap userNulls, int[] userDictIds,
      int[] step0, int[] step1) {
    return Map.of(
        USER, SyntheticBlockValSets.DictIds.create(userNulls, userDictIds, userDictionary(), DataType.INT),
        STEP_0, SyntheticBlockValSets.Int.create(null, step0),
        STEP_1, SyntheticBlockValSets.Int.create(null, step1));
  }

  /// A row whose correlation key is null belongs to no user, so it must not be counted as one.
  ///
  /// Its dictionary id is the default's, which is a real user's id, so without the null check the funnel credits
  /// that user with a step they never took.
  @Test
  public void testFunnelCountSkipsRowsWithANullCorrelationKey() {
    // Row 0 is user 0 reaching step 0. Row 1 is null, and its dictionary id happens to be user 1's.
    int[] userDictIds = {0, 1};
    int[] step0 = {1, 1};
    int[] step1 = {0, 0};

    AggregationFunction<?, LongArrayList> enabled = funnelCount(true);
    AggregationResultHolder holder = enabled.createAggregationResultHolder();
    enabled.aggregate(2, holder, funnelCountBlock(RoaringBitmap.bitmapOf(1), userDictIds, step0, step1));
    assertEquals(extractCounts(enabled, holder), List.of(1L, 0L));

    AggregationFunction<?, LongArrayList> disabled = funnelCount(false);
    AggregationResultHolder disabledHolder = disabled.createAggregationResultHolder();
    disabled.aggregate(2, disabledHolder, funnelCountBlock(RoaringBitmap.bitmapOf(1), userDictIds, step0, step1));
    assertEquals(extractCounts(disabled, disabledHolder), List.of(2L, 0L));
  }

  @SuppressWarnings("unchecked")
  private static List<Long> extractCounts(AggregationFunction<?, LongArrayList> function,
      AggregationResultHolder holder) {
    AggregationFunction<Object, LongArrayList> typed = (AggregationFunction<Object, LongArrayList>) function;
    return typed.extractFinalResult(typed.extractAggregationResult(holder));
  }
}
