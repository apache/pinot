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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
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

  private static PriorityQueue<FunnelStepEvent> events(int step) {
    return new PriorityQueue<>(List.of(new FunnelStepEvent(100L * (step + 1), step)));
  }

  private static PriorityQueue<FunnelStepEventWithExtraFields> eventsWithExtraFields(int step) {
    return new PriorityQueue<>(
        List.of(new FunnelStepEventWithExtraFields(new FunnelStepEvent(100L * (step + 1), step), List.of())));
  }
}
