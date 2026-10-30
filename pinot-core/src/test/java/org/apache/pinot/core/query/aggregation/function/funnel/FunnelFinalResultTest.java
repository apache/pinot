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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelCompleteCountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelEventsFunctionEvalAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMatchStepAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelStepDurationStatsAggregationFunction;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Final extraction must preserve partial funnel state for subsequent merging.
public class FunnelFinalResultTest {
  @DataProvider
  public Object[][] funnels() {
    ExpressionContext step0 = ExpressionContext.forIdentifier("step0");
    ExpressionContext step1 = ExpressionContext.forIdentifier("step1");
    List<ExpressionContext> arguments = List.of(ExpressionContext.forIdentifier("ts"),
        ExpressionContext.forLiteral(Literal.longValue(1000)), ExpressionContext.forLiteral(Literal.intValue(2)),
        step0, step1);
    List<ExpressionContext> durationArguments = new ArrayList<>(arguments);
    durationArguments.add(ExpressionContext.forLiteral(Literal.stringValue("DURATIONFUNCTIONS=COUNT")));
    List<ExpressionContext> eventsArguments = new ArrayList<>(arguments);
    eventsArguments.add(ExpressionContext.forLiteral(Literal.intValue(0)));
    List<ExpressionContext> countArguments = List.of(
        ExpressionContext.forFunction(
            new FunctionContext(FunctionContext.Type.TRANSFORM, "steps", List.of(step0, step1))),
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "correlateby",
            List.of(ExpressionContext.forIdentifier("userId")))));
    List<ExpressionContext> setArguments = new ArrayList<>(countArguments);
    setArguments.add(ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "settings",
        List.of(ExpressionContext.forLiteral(Literal.stringValue("set"))))));
    return new Object[][]{
        {new FunnelMaxStepAggregationFunction(arguments, true), events(0), events(1), 1, 2},
        {new FunnelCompleteCountAggregationFunction(arguments, true), events(0), events(1), 0, 1},
        {new FunnelMatchStepAggregationFunction(arguments, true), events(0), events(1),
            new int[]{1, 0}, new int[]{1, 1}},
        {new FunnelStepDurationStatsAggregationFunction(durationArguments, true), events(0), events(1),
            new double[]{1, 0}, new double[]{1, 1}},
        {new FunnelEventsFunctionEvalAggregationFunction(eventsArguments, true), eventsWithExtraFields(0),
            eventsWithExtraFields(1), new String[]{"0"}, new String[]{"1, 0"}},
        {new FunnelCountAggregationFunctionFactory(countArguments, true).get(),
            List.of(RoaringBitmap.bitmapOf(1), RoaringBitmap.bitmapOf(2)),
            List.of(RoaringBitmap.bitmapOf(2), RoaringBitmap.bitmapOf(1)),
            new long[]{1, 0}, new long[]{2, 2}},
        {new FunnelCountAggregationFunctionFactory(setArguments, true).get(),
            List.of(new HashSet<>(List.of(1)), new HashSet<>(List.of(2))),
            List.of(new HashSet<>(List.of(2)), new HashSet<>(List.of(1))),
            new long[]{1, 0}, new long[]{2, 2}}
    };
  }

  @Test(dataProvider = "funnels")
  public <I, F> void testFinalExtractionPreservesIntermediateResult(
      AggregationFunction<I, F> function,
      I intermediateResult, I otherIntermediateResult, F expectedPartialResult, F expectedMergedResult) {
    assertEquals(function.extractFinalResult(intermediateResult), expectedPartialResult);
    assertEquals(function.extractFinalResult(intermediateResult), expectedPartialResult);
    assertEquals(function.extractFinalResult(function.merge(intermediateResult, otherIntermediateResult)),
        expectedMergedResult);
  }

  @Test
  public void testFunnelEventsFunctionEvalFinalResultMerge() {
    FunnelEventsFunctionEvalAggregationFunction function = new FunnelEventsFunctionEvalAggregationFunction(List.of(
        ExpressionContext.forIdentifier("ts"), ExpressionContext.forLiteral(Literal.longValue(1000)),
        ExpressionContext.forLiteral(Literal.intValue(2)), ExpressionContext.forIdentifier("step0"),
        ExpressionContext.forIdentifier("step1"), ExpressionContext.forLiteral(Literal.intValue(0))), true);
    String[] twoFunnels = {"2, 1, 2", "a", "b", "c"};
    String[] oneFunnel = {"1, 2", "d", "e"};
    assertEquals(function.mergeFinalResult(twoFunnels, oneFunnel),
        new String[]{"3, 1, 2, 2", "a", "b", "c", "d", "e"});
    assertEquals(function.mergeFinalResult(oneFunnel, new String[]{"0"}), oneFunnel);
    assertEquals(function.mergeFinalResult(new String[0], oneFunnel), oneFunnel);
  }

  @Test
  public void testBitmapFinalExtractionAvoidsUnnecessaryCopies() {
    RoaringBitmap firstStep = mock(RoaringBitmap.class);
    when(firstStep.getCardinality()).thenReturn(1_000_000);
    when(firstStep.clone()).thenThrow(new AssertionError("First-step population must not be copied"));
    assertEquals(new BitmapMergeStrategy(1).extractFinalResult(List.of(firstStep)),
        new long[]{1_000_000});
    assertEquals(new BitmapMergeStrategy(3).extractFinalResult(
        List.of(firstStep, new RoaringBitmap(), RoaringBitmap.bitmapOf(2))),
        new long[]{1_000_000, 0, 0});
    assertEquals(new BitmapMergeStrategy(2).extractFinalResult(List.of(new RoaringBitmap(), firstStep)),
        new long[]{0, 0});
    verify(firstStep, never()).clone();
  }

  @Test
  public void testBitmapFinalExtractionPreservesStepPopulations() {
    RoaringBitmap laterStep = mock(RoaringBitmap.class);
    when(laterStep.isEmpty()).thenThrow(new AssertionError("Steps after an empty intersection must not be inspected"));
    List<RoaringBitmap> steps = List.of(RoaringBitmap.bitmapOf(1, 2, 3), RoaringBitmap.bitmapOf(2, 3),
        RoaringBitmap.bitmapOf(3, 4), RoaringBitmap.bitmapOf(5, 6), laterStep);
    int[][] originalSteps = steps.subList(0, 4).stream().map(RoaringBitmap::toArray).toArray(int[][]::new);
    BitmapMergeStrategy strategy = new BitmapMergeStrategy(5);
    long[] expectedCounts = {3, 2, 1, 0, 0};
    assertEquals(strategy.extractFinalResult(steps), expectedCounts);
    assertEquals(strategy.extractFinalResult(steps), expectedCounts);
    for (int i = 0; i < originalSteps.length; i++) {
      assertEquals(steps.get(i).toArray(), originalSteps[i]);
    }
    verify(laterStep, never()).isEmpty();
  }

  @Test
  public void testSetFinalExtractionAvoidsTraversingLargeFirstStep() {
    Set<?> firstStep = mock(Set.class);
    when(firstStep.size()).thenReturn(1_000_000);
    when(firstStep.contains(2)).thenReturn(true);
    when(firstStep.iterator()).thenThrow(new AssertionError("Large first-step population must not be traversed"));
    assertEquals(new SetMergeStrategy(1).extractFinalResult(List.of(firstStep)),
        new long[]{1_000_000});
    assertEquals(new SetMergeStrategy(3).extractFinalResult(List.of(firstStep, Set.of(), Set.of(2))),
        new long[]{1_000_000, 0, 0});
    assertEquals(new SetMergeStrategy(3).extractFinalResult(List.of(firstStep, Set.of(2, 3), Set.of(2, 4, 5))),
        new long[]{1_000_000, 1, 1});
    verify(firstStep, never()).iterator();
    verify(firstStep, never()).toArray();
  }

  @DataProvider
  public Object[][] stepSets() {
    float floatNaN1 = Float.intBitsToFloat(0x7fc00001);
    float floatNaN2 = Float.intBitsToFloat(0x7fc00002);
    double doubleNaN1 = Double.longBitsToDouble(0x7ff8000000000001L);
    double doubleNaN2 = Double.longBitsToDouble(0x7ff8000000000002L);
    return new Object[][]{
        {List.of(new IntOpenHashSet(new int[]{1, 2, 3}), new IntOpenHashSet(new int[]{2, 3}),
            new IntOpenHashSet(new int[]{3, 4, 5, 6})), new long[]{3, 2, 1}},
        {List.of(new LongOpenHashSet(new long[]{1, 2, 3}), new LongOpenHashSet(new long[]{2, 3}),
            new LongOpenHashSet(new long[]{3, 4, 5, 6})), new long[]{3, 2, 1}},
        {List.of(new FloatOpenHashSet(new float[]{floatNaN1, floatNaN2, -0.0f, 0.0f}),
            new FloatOpenHashSet(new float[]{floatNaN1, floatNaN2, 0.0f}),
            new FloatOpenHashSet(new float[]{floatNaN2, 0.0f, 1, 2})), new long[]{4, 3, 2}},
        {List.of(new DoubleOpenHashSet(new double[]{doubleNaN1, doubleNaN2, -0.0, 0.0}),
            new DoubleOpenHashSet(new double[]{doubleNaN1, doubleNaN2, 0.0}),
            new DoubleOpenHashSet(new double[]{doubleNaN2, 0.0, 1, 2})), new long[]{4, 3, 2}},
        {List.of(Set.of("1", "2", "3"), Set.of("2", "3"), Set.of("3", "4", "5", "6")), new long[]{3, 2, 1}}
    };
  }

  @Test(dataProvider = "stepSets")
  public void testSetFinalExtractionPreservesStepPopulations(List<Set> steps, long[] expectedCounts) {
    Object[][] originalSteps = steps.stream().map(Set::toArray).toArray(Object[][]::new);
    SetMergeStrategy strategy = new SetMergeStrategy(3);
    assertEquals(strategy.extractFinalResult(steps), expectedCounts);
    assertEquals(strategy.extractFinalResult(steps), expectedCounts);
    for (int i = 0; i < steps.size(); i++) {
      assertEquals(steps.get(i).toArray(), originalSteps[i]);
    }
  }

  private static PriorityQueue<FunnelStepEvent> events(int step) {
    return new PriorityQueue<>(List.of(new FunnelStepEvent(100L * (step + 1), step)));
  }

  private static PriorityQueue<FunnelStepEventWithExtraFields> eventsWithExtraFields(int step) {
    return new PriorityQueue<>(
        List.of(new FunnelStepEventWithExtraFields(new FunnelStepEvent(100L * (step + 1), step), List.of())));
  }
}
