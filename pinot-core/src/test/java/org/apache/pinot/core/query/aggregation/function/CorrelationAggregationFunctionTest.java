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
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.segment.local.customobject.CorrelationTuple;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Behaviour of `CORR`, which pairs a value from each of two columns per row and returns their Pearson correlation.
public class CorrelationAggregationFunctionTest {
  private static final ExpressionContext X = ExpressionContext.forIdentifier("x");
  private static final ExpressionContext Y = ExpressionContext.forIdentifier("y");
  // Y = 10 * X, so the columns are perfectly positively correlated
  private static final double[] X_VALUES = {1.0, 2.0, 3.0, 4.0};
  private static final double[] Y_VALUES = {10.0, 20.0, 30.0, 40.0};

  private static CorrelationAggregationFunction create(boolean nullHandlingEnabled) {
    return new CorrelationAggregationFunction(List.of(X, Y), nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> blocks(double[] xValues, double[] yValues, RoaringBitmap nullsInX,
      RoaringBitmap nullsInY) {
    return Map.of(X, SyntheticBlockValSets.Double.create(nullsInX, xValues),
        Y, SyntheticBlockValSets.Double.create(nullsInY, yValues));
  }

  private static Map<ExpressionContext, BlockValSet> blocks(RoaringBitmap nullsInX, RoaringBitmap nullsInY) {
    return blocks(X_VALUES, Y_VALUES, nullsInX, nullsInY);
  }

  private static CorrelationTuple aggregate(CorrelationAggregationFunction function, int length,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(length, resultHolder, blockValSetMap);
    return function.extractAggregationResult(resultHolder);
  }

  /// A row contributes only when both of its values are present, so the rows skipped are the union of the two
  /// columns' nulls rather than either one alone. The squared sums are kept alongside the covariance sums.
  @Test
  public void testRowIsSkippedWhenEitherColumnIsNull() {
    CorrelationTuple result =
        aggregate(create(true), X_VALUES.length, blocks(RoaringBitmap.bitmapOf(1), RoaringBitmap.bitmapOf(2)));

    // Rows 1 and 2 drop out, leaving rows 0 and 3
    assertEquals(result.getCount(), 2L);
    assertEquals(result.getSumX(), 5.0);
    assertEquals(result.getSumY(), 50.0);
    assertEquals(result.getSumXY(), 170.0);
    assertEquals(result.getSumX2(), 17.0);
    assertEquals(result.getSumY2(), 1700.0);
  }

  @Test
  public void testNothingAggregatedWhenEveryRowIsNull() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, X_VALUES.length);
    CorrelationAggregationFunction function = create(true);

    assertNull(aggregate(function, X_VALUES.length, blocks(allNull, null)));
    assertNull(function.extractFinalResult(null));
  }

  /// With the option disabled the column default is folded in, which keeps the mode's long-standing behaviour.
  @Test
  public void testNullRowsFoldedInWhenOptionDisabled() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, X_VALUES.length);

    CorrelationTuple result = aggregate(create(false), X_VALUES.length, blocks(allNull, allNull));

    assertEquals(result.getCount(), 4L);
    assertEquals(result.getSumX(), 10.0);
  }

  @Test
  public void testPerfectPositiveCorrelation() {
    CorrelationTuple result = aggregate(create(true), X_VALUES.length, blocks(null, null));
    assertEquals(create(true).extractFinalResult(result), 1.0, 1e-9);
  }

  /// Y = 50 - 10 * X is perfectly inversely correlated with X.
  @Test
  public void testPerfectNegativeCorrelation() {
    double[] yValues = {40.0, 30.0, 20.0, 10.0};
    CorrelationTuple result =
        aggregate(create(true), X_VALUES.length, blocks(X_VALUES, yValues, null, null));
    assertEquals(create(true).extractFinalResult(result), -1.0, 1e-9);
  }

  /// A column that never varies leaves the correlation undefined: NULL with the option enabled, the sentinel with it
  /// disabled.
  @Test
  public void testConstantColumnIsUndefined() {
    double[] constantY = {7.0, 7.0, 7.0, 7.0};
    CorrelationTuple enabled =
        aggregate(create(true), X_VALUES.length, blocks(X_VALUES, constantY, null, null));
    assertNull(create(true).extractFinalResult(enabled));

    CorrelationTuple disabled =
        aggregate(create(false), X_VALUES.length, blocks(X_VALUES, constantY, null, null));
    assertEquals(create(false).extractFinalResult(disabled), Double.NEGATIVE_INFINITY);
  }

  /// The computed coefficient matches Apache Commons Math on data that is neither perfectly correlated nor constant.
  @Test
  public void testMatchesCommonsMath() {
    double[] xValues = {1.0, 2.0, 4.0, 7.0, 11.0, 16.0};
    double[] yValues = {2.0, 1.0, 5.0, 3.0, 9.0, 8.0};
    double expected = new PearsonsCorrelation().correlation(xValues, yValues);

    CorrelationTuple result =
        aggregate(create(true), xValues.length, blocks(xValues, yValues, null, null));
    assertEquals(create(true).extractFinalResult(result), expected, 1e-9);
  }

  /// An untouched accumulator renders the sentinel with the option disabled, and `NULL` with it enabled.
  @Test
  public void testEmptyInputRendersSentinelOnlyWhenOptionDisabled() {
    assertEquals(create(false).extractFinalResult(null), Double.NEGATIVE_INFINITY);
    assertNull(create(true).extractFinalResult(null));
  }

  /// The intermediate tuple survives a serialize/deserialize round trip, which is how it crosses from server to
  /// broker.
  @Test
  public void testIntermediateTupleSerDeRoundTrip() {
    CorrelationTuple tuple = new CorrelationTuple(5.0, 50.0, 170.0, 17.0, 1700.0, 2L);
    CorrelationTuple restored = CorrelationTuple.fromBytes(tuple.toBytes());

    assertEquals(restored.getSumX(), tuple.getSumX());
    assertEquals(restored.getSumY(), tuple.getSumY());
    assertEquals(restored.getSumXY(), tuple.getSumXY());
    assertEquals(restored.getSumX2(), tuple.getSumX2());
    assertEquals(restored.getSumY2(), tuple.getSumY2());
    assertEquals(restored.getCount(), tuple.getCount());
  }
}
