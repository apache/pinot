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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
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
  // With a 1e9 offset and a spread of ~10 the inputs only carry ~8 significant digits of spread, so this is close to
  // what double arithmetic allows; the raw-sum formula was off in the first digit (or NULL) on the same data
  private static final double PRECISION = 1e-7;

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
  /// columns' nulls rather than either one alone.
  @Test
  public void testRowIsSkippedWhenEitherColumnIsNull() {
    CorrelationTuple result =
        aggregate(create(true), X_VALUES.length, blocks(RoaringBitmap.bitmapOf(1), RoaringBitmap.bitmapOf(2)));

    // Rows 1 and 2 drop out, leaving rows 0 and 3
    assertEquals(result.getCount(), 2L);
    assertEquals(result.getMeanX(), 2.5);
    assertEquals(result.getMeanY(), 25.0);
    assertEquals(result.getM2X(), 4.5);
    assertEquals(result.getM2Y(), 450.0);
    assertEquals(result.getCXY(), 45.0);
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
    assertEquals(result.getMeanX(), 2.5);
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

  /// Values that are large relative to their spread (timestamps, IDs) must not lose the correlation to cancellation.
  /// The result has to match Commons Math through plain aggregation, group-by, and a serialize/merge across partitions.
  @Test
  public void testLargeCommonOffsetKeepsPrecision() {
    Random random = new Random(42);
    int numRows = 100_000;
    double[] xValues = new double[numRows];
    double[] yValues = new double[numRows];
    for (int i = 0; i < numRows; i++) {
      double noise = random.nextGaussian();
      xValues[i] = 1e9 + 10 * noise;
      yValues[i] = 1e9 + 10 * (0.5 * noise + random.nextGaussian());
    }
    double expected = new PearsonsCorrelation().correlation(xValues, yValues);
    CorrelationAggregationFunction function = create(true);
    Map<ExpressionContext, BlockValSet> blockValSetMap = blocks(xValues, yValues, null, null);

    assertEquals(function.extractFinalResult(aggregate(function, numRows, blockValSetMap)), expected, PRECISION);

    GroupByResultHolder groupByResultHolder = function.createGroupByResultHolder(1, 1);
    function.aggregateGroupBySV(numRows, new int[numRows], groupByResultHolder, blockValSetMap);
    CorrelationTuple groupResult = function.extractGroupByResult(groupByResultHolder, 0);
    assertEquals(function.extractFinalResult(groupResult), expected, PRECISION);

    CorrelationTuple merged = null;
    int partitionSize = numRows / 4;
    for (int from = 0; from < numRows; from += partitionSize) {
      double[] xPartition = Arrays.copyOfRange(xValues, from, from + partitionSize);
      double[] yPartition = Arrays.copyOfRange(yValues, from, from + partitionSize);
      CorrelationTuple partition = CorrelationTuple.fromBytes(
          aggregate(function, partitionSize, blocks(xPartition, yPartition, null, null)).toBytes());
      merged = merged == null ? partition : function.merge(merged, partition);
    }
    assertEquals(function.extractFinalResult(merged), expected, PRECISION);
  }

  /// Correlation is shift-invariant, so offsetting both columns by 1e9 must not change it.
  @Test
  public void testShiftInvariance() {
    double[] xValues = {1.0, 2.0, 4.0, 7.0, 11.0, 16.0};
    double[] yValues = {2.0, 1.0, 5.0, 3.0, 9.0, 8.0};
    double[] shiftedX = new double[xValues.length];
    double[] shiftedY = new double[yValues.length];
    for (int i = 0; i < xValues.length; i++) {
      shiftedX[i] = xValues[i] + 1e9;
      shiftedY[i] = yValues[i] + 1e9;
    }
    double expected = new PearsonsCorrelation().correlation(xValues, yValues);

    CorrelationTuple result = aggregate(create(true), xValues.length, blocks(shiftedX, shiftedY, null, null));
    assertEquals(create(true).extractFinalResult(result), expected, PRECISION);
  }

  /// A single row has zero variance in both columns, so the correlation is undefined.
  @Test
  public void testSingleRowIsUndefined() {
    CorrelationTuple result = aggregate(create(true), 1, blocks(new double[]{1.0}, new double[]{2.0}, null, null));
    assertNull(create(true).extractFinalResult(result));
  }

  /// The intermediate tuple survives a serialize/deserialize round trip, which is how it crosses from server to
  /// broker.
  @Test
  public void testIntermediateTupleSerDeRoundTrip() {
    CorrelationTuple tuple = new CorrelationTuple(2L, 2.5, 25.0, 4.5, 450.0, 45.0);
    CorrelationTuple restored = CorrelationTuple.fromBytes(tuple.toBytes());

    assertEquals(restored.getCount(), tuple.getCount());
    assertEquals(restored.getMeanX(), tuple.getMeanX());
    assertEquals(restored.getMeanY(), tuple.getMeanY());
    assertEquals(restored.getM2X(), tuple.getM2X());
    assertEquals(restored.getM2Y(), tuple.getM2Y());
    assertEquals(restored.getCXY(), tuple.getCXY());
  }
}
