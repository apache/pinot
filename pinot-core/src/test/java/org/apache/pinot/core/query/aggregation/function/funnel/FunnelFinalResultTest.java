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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
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
            IntArrayList.wrap(new int[]{1, 0}), IntArrayList.wrap(new int[]{1, 1})},
        {new FunnelStepDurationStatsAggregationFunction(durationArguments, true), events(0), events(1),
            DoubleArrayList.wrap(new double[]{1, 0}), DoubleArrayList.wrap(new double[]{1, 1})},
        {new FunnelEventsFunctionEvalAggregationFunction(eventsArguments, true), eventsWithExtraFields(0),
            eventsWithExtraFields(1), new ObjectArrayList<>(List.of("0")), new ObjectArrayList<>(List.of("1, 0"))},
        {new FunnelCountAggregationFunctionFactory(countArguments, true).get(),
            List.of(RoaringBitmap.bitmapOf(1), RoaringBitmap.bitmapOf(2)),
            List.of(RoaringBitmap.bitmapOf(2), RoaringBitmap.bitmapOf(1)),
            LongArrayList.wrap(new long[]{1, 0}), LongArrayList.wrap(new long[]{2, 2})},
        {new FunnelCountAggregationFunctionFactory(setArguments, true).get(),
            List.of(new HashSet<>(List.of(1)), new HashSet<>(List.of(2))),
            List.of(new HashSet<>(List.of(2)), new HashSet<>(List.of(1))),
            LongArrayList.wrap(new long[]{1, 0}), LongArrayList.wrap(new long[]{2, 2})}
    };
  }

  @Test(dataProvider = "funnels")
  public <I, F extends Comparable<?>> void testFinalExtractionPreservesIntermediateResult(
      AggregationFunction<I, F> function,
      I intermediateResult, I otherIntermediateResult, F expectedPartialResult, F expectedMergedResult) {
    assertEquals(function.extractFinalResult(intermediateResult), expectedPartialResult);
    assertEquals(function.extractFinalResult(intermediateResult), expectedPartialResult);
    assertEquals(function.extractFinalResult(function.merge(intermediateResult, otherIntermediateResult)),
        expectedMergedResult);
  }

  @Test
  public void testSetFinalExtractionAvoidsTraversingLargeFirstStep() {
    Set<?> firstStep = mock(Set.class);
    when(firstStep.size()).thenReturn(1_000_000);
    when(firstStep.contains(2)).thenReturn(true);
    when(firstStep.iterator()).thenThrow(new AssertionError("Large first-step population must not be traversed"));
    assertEquals(new SetMergeStrategy(1).extractFinalResult(List.of(firstStep)),
        LongArrayList.wrap(new long[]{1_000_000}));
    assertEquals(new SetMergeStrategy(3).extractFinalResult(List.of(firstStep, Set.of(), Set.of(2))),
        LongArrayList.wrap(new long[]{1_000_000, 0, 0}));
    assertEquals(new SetMergeStrategy(3).extractFinalResult(List.of(firstStep, Set.of(2, 3), Set.of(2, 4, 5))),
        LongArrayList.wrap(new long[]{1_000_000, 1, 1}));
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
    assertEquals(strategy.extractFinalResult(steps), LongArrayList.wrap(expectedCounts));
    assertEquals(strategy.extractFinalResult(steps), LongArrayList.wrap(expectedCounts));
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
