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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Null handling for `COVAR_POP` and `COVAR_SAMP`, which pair a value from each of two columns per row.
public class CovarianceAggregationFunctionTest {
  private static final ExpressionContext X = ExpressionContext.forIdentifier("x");
  private static final ExpressionContext Y = ExpressionContext.forIdentifier("y");
  private static final double[] X_VALUES = {1.0, 2.0, 3.0, 4.0};
  private static final double[] Y_VALUES = {10.0, 20.0, 30.0, 40.0};

  private static CovarianceAggregationFunction create(boolean nullHandlingEnabled) {
    return new CovarianceAggregationFunction(List.of(X, Y), false, nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> blocks(RoaringBitmap nullsInX, RoaringBitmap nullsInY) {
    return Map.of(X, SyntheticBlockValSets.Double.create(nullsInX, X_VALUES),
        Y, SyntheticBlockValSets.Double.create(nullsInY, Y_VALUES));
  }

  private static CovarianceTuple aggregate(CovarianceAggregationFunction function,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(X_VALUES.length, resultHolder, blockValSetMap);
    return function.extractAggregationResult(resultHolder);
  }

  /// A row contributes only when both of its values are present, so the rows skipped are the union of the two
  /// columns' nulls rather than either one alone.
  @Test
  public void testRowIsSkippedWhenEitherColumnIsNull() {
    CovarianceTuple result =
        aggregate(create(true), blocks(RoaringBitmap.bitmapOf(1), RoaringBitmap.bitmapOf(2)));

    // Rows 1 and 2 drop out, leaving rows 0 and 3
    assertEquals(result.getCount(), 2L);
    assertEquals(result.getSumX(), 5.0);
    assertEquals(result.getSumY(), 50.0);
    assertEquals(result.getSumXY(), 170.0);
  }

  /// Both columns null in the same row, including row 0: the merged null stream reports the row once, and the row
  /// is dropped once.
  @Test
  public void testRowNullInBothColumnsIsDroppedOnce() {
    CovarianceTuple result =
        aggregate(create(true), blocks(RoaringBitmap.bitmapOf(0, 2), RoaringBitmap.bitmapOf(0, 3)));

    // Rows 0, 2 and 3 drop out, leaving row 1 alone
    assertEquals(result.getCount(), 1L);
    assertEquals(result.getSumX(), 2.0);
    assertEquals(result.getSumY(), 20.0);
    assertEquals(result.getSumXY(), 40.0);
  }

  @Test
  public void testNothingAggregatedWhenEveryRowIsNull() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, X_VALUES.length);
    CovarianceAggregationFunction function = create(true);

    assertNull(aggregate(function, blocks(allNull, null)));
    assertNull(function.extractFinalResult(null));
  }

  /// With the option disabled the column default is folded in, which is the answer this mode has always given.
  @Test
  public void testNullRowsFoldedInWhenOptionDisabled() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, X_VALUES.length);

    CovarianceTuple result = aggregate(create(false), blocks(allNull, allNull));

    assertEquals(result.getCount(), 4L);
    assertEquals(result.getSumX(), 10.0);
  }

  /// `COVAR_SAMP` divides by `count - 1`, so one contributing row leaves it undefined rather than zero.
  @Test
  public void testSampleCovarianceOverASingleRowIsNull() {
    RoaringBitmap allButFirst = RoaringBitmap.bitmapOf(1, 2, 3);
    CovarianceAggregationFunction sample = new CovarianceAggregationFunction(List.of(X, Y), true, true);
    AggregationResultHolder resultHolder = sample.createAggregationResultHolder();
    sample.aggregate(X_VALUES.length, resultHolder, blocks(allButFirst, null));
    CovarianceTuple tuple = sample.extractAggregationResult(resultHolder);

    assertEquals(tuple.getCount(), 1L);
    assertNull(sample.extractFinalResult(tuple));
  }

  /// An untouched accumulator renders the identity with the option disabled, and `NULL` with it enabled.
  @Test
  public void testEmptyInputRendersIdentityOnlyWhenOptionDisabled() {
    assertEquals(create(false).extractFinalResult(null), Double.NEGATIVE_INFINITY);
    assertNull(create(true).extractFinalResult(null));
  }
}
