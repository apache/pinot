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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.List;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Covers [AggregationFunction#hasApproximatedResult()] for the SMART functions: extracting an exact accumulator leaves
/// it unset, and extracting one that a merge switched to a sketch sets it.
@SuppressWarnings({"rawtypes", "unchecked"})
public class SmartFunctionApproximatedResultTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("col");
  private static final ExpressionContext THRESHOLD_ONE =
      ExpressionContext.forLiteral(Literal.stringValue("threshold=1"));

  @DataProvider
  public static Object[][] distinctCountFunctions() {
    List<ExpressionContext> arguments = List.of(COLUMN, THRESHOLD_ONE);
    return new Object[][]{
        {new DistinctCountSmartHLLAggregationFunction(arguments, false)},
        {new DistinctCountSmartHLLPlusAggregationFunction(arguments, false)},
        {new DistinctCountSmartULLAggregationFunction(arguments, false)}
    };
  }

  @Test(dataProvider = "distinctCountFunctions")
  public void testDistinctCount(AggregationFunction function) {
    assertEquals(function.extractFinalResult(null), 0);
    assertEquals(function.extractFinalResult(new IntOpenHashSet(new int[]{1})), 1);
    assertFalse(function.hasApproximatedResult());

    // Two values cross the threshold of one, so the merge switches the accumulator to a sketch.
    Object merged = function.merge(new IntOpenHashSet(new int[]{1}), new IntOpenHashSet(new int[]{2}));
    function.extractFinalResult(merged);
    assertTrue(function.hasApproximatedResult());
  }

  @Test
  public void testPercentile() {
    PercentileSmartTDigestAggregationFunction function = new PercentileSmartTDigestAggregationFunction(
        List.of(COLUMN, ExpressionContext.forLiteral(Literal.doubleValue(50.0)), THRESHOLD_ONE), false);
    function.extractFinalResult(null);
    assertEquals(function.extractFinalResult(new DoubleArrayList(new double[]{1.0})), 1.0);
    assertFalse(function.hasApproximatedResult());

    Object merged = function.merge(new DoubleArrayList(new double[]{1.0}), new DoubleArrayList(new double[]{2.0}));
    function.extractFinalResult(merged);
    assertTrue(function.hasApproximatedResult());
  }

  @Test
  public void testExactFunctionKeepsTheDefault() {
    DistinctCountAggregationFunction function = new DistinctCountAggregationFunction(List.of(COLUMN), false);
    function.extractFinalResult(function.merge(new IntOpenHashSet(new int[]{1}), new IntOpenHashSet(new int[]{2})));
    assertFalse(function.hasApproximatedResult());
  }
}
