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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.array.SumArrayDoubleAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.array.SumArrayLongAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Null handling, multi-value columns and the empty-input answer for the value-accumulating aggregations:
/// `SKEWNESS`, `KURTOSIS`, `STUNION`, `SUMARRAYLONG`, `SUMARRAYDOUBLE`, `HISTOGRAM` and `IDSET`.
///
/// [AggregationFunctionNullContractTest] drives one synthetic single-value block through `aggregate` only, and it
/// cannot construct most of these from its shared argument shapes at all, so the group-by paths, the multi-value
/// paths and the per-mode empty answers are checked nowhere else.
///
/// Each of these functions renders "nothing aggregated" differently with the option disabled — `NaN`, the empty
/// point, an all-zero histogram, an empty id set, `NULL` — and that sentinel is a backward-compatibility
/// constraint, so both modes are asserted rather than only the SQL one.
public class ValueAggregationNullHandlingTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");
  private static final RoaringBitmap ROW1_NULL = RoaringBitmap.bitmapOf(1);

  private static RoaringBitmap allNull(int length) {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, length);
    return bitmap;
  }

  private static Map<ExpressionContext, BlockValSet> block(BlockValSet blockValSet) {
    return Map.of(COLUMN, blockValSet);
  }

  // ---------- SKEWNESS / KURTOSIS ----------

  private static FourthMomentAggregationFunction skewness(boolean nullHandlingEnabled) {
    return new FourthMomentAggregationFunction(List.of(COLUMN), FourthMomentAggregationFunction.Type.SKEWNESS,
        nullHandlingEnabled);
  }

  private static PinotFourthMoment aggregateDoubles(FourthMomentAggregationFunction function, RoaringBitmap nullBitmap,
      double[] values) {
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(values.length, holder, block(SyntheticBlockValSets.Double.create(nullBitmap, values)));
    return function.extractAggregationResult(holder);
  }

  /// A null row contributes nothing, so the skewness equals that of the same block with the null row removed.
  @Test
  public void testSkewnessSkipsNullRows() {
    double[] withNull = {1.0d, 999.0d, 2.0d, 4.0d, 8.0d};
    double[] without = {1.0d, 2.0d, 4.0d, 8.0d};

    Double skipped = skewness(true).extractFinalResult(
        aggregateDoubles(skewness(true), RoaringBitmap.bitmapOf(1), withNull));
    Double expected = skewness(true).extractFinalResult(aggregateDoubles(skewness(true), null, without));

    assertNotNull(skipped);
    assertEquals(skipped, expected);
  }

  /// With the option enabled the skewness of nothing is NULL; with it disabled it is the NaN an untouched moment
  /// renders to, which is the answer that mode has always given.
  ///
  /// The two modes need different inputs to reach "nothing aggregated". With the option enabled an all-null block
  /// does it, because every row is skipped. With it disabled the null bitmap is ignored and those same rows are
  /// aggregated as values, so only a holder that was never touched gets there.
  @Test
  public void testSkewnessEmptyAnswerDiffersByMode() {
    double[] values = {1.0d, 2.0d, 4.0d};

    assertNull(skewness(true).extractFinalResult(aggregateDoubles(skewness(true), allNull(3), values)));

    FourthMomentAggregationFunction disabled = skewness(false);
    Double result = disabled.extractFinalResult(
        disabled.extractAggregationResult(disabled.createAggregationResultHolder()));
    assertNotNull(result);
    assertTrue(result.isNaN(), "expected NaN, got " + result);
  }

  /// With the option disabled a null row is read as the column default and still aggregated, which is the answer
  /// that mode has always given.
  @Test
  public void testSkewnessCountsNullRowsWhenOptionDisabled() {
    double[] values = {1.0d, 2.0d, 4.0d};

    Double withBitmap = skewness(false).extractFinalResult(aggregateDoubles(skewness(false), allNull(3), values));
    Double withoutBitmap = skewness(false).extractFinalResult(aggregateDoubles(skewness(false), null, values));

    assertNotNull(withBitmap);
    assertEquals(withBitmap, withoutBitmap);
  }

  // ---------- STUNION ----------

  private static StUnionAggregationFunction stUnion(boolean nullHandlingEnabled) {
    return new StUnionAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
  }

  private static Geometry point(double x, double y) {
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(new org.locationtech.jts.geom.Coordinate(x, y));
  }

  private static byte[] serialized(double x, double y) {
    return GeometrySerializer.serialize(point(x, y));
  }

  /// Only the geometries of the rows that carry one are unioned.
  @Test
  public void testStUnionSkipsNullRows() {
    byte[][] values = {serialized(0, 0), serialized(5, 5)};

    StUnionAggregationFunction function = stUnion(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, block(SyntheticBlockValSets.Bytes.create(ROW1_NULL, values)));

    assertEquals(function.extractAggregationResult(holder), point(0, 0));
  }

  /// With the option enabled the union of nothing is NULL; with it disabled it is the empty point.
  @Test
  public void testStUnionEmptyAnswerDiffersByMode() {
    byte[][] values = {serialized(0, 0), serialized(5, 5)};

    StUnionAggregationFunction enabled = stUnion(true);
    AggregationResultHolder enabledHolder = enabled.createAggregationResultHolder();
    enabled.aggregate(2, enabledHolder, block(SyntheticBlockValSets.Bytes.create(allNull(2), values)));
    assertNull(enabled.extractFinalResult(enabled.extractAggregationResult(enabledHolder)));

    StUnionAggregationFunction disabled = stUnion(false);
    assertEquals(disabled.extractFinalResult(
            disabled.extractAggregationResult(disabled.createAggregationResultHolder())),
        new ByteArray(GeometrySerializer.serialize(GeometryUtils.EMPTY_POINT)));
  }

  /// Every geometry of a multi-value row is folded into the union.
  @Test
  public void testStUnionMVColumnUnionsEveryGeometry() {
    byte[][][] rows = {
        {serialized(0, 0), serialized(1, 1)},
        {serialized(5, 5)}
    };

    StUnionAggregationFunction function = stUnion(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, block(SyntheticBlockValSets.BytesMV.create(ROW1_NULL, rows)));

    // Row 1 is null, so only the two geometries of row 0 are unioned
    Geometry result = function.extractAggregationResult(holder);
    assertNotNull(result);
    assertEquals(result.getNumGeometries(), 2);
    assertTrue(result.covers(point(0, 0)));
    assertTrue(result.covers(point(1, 1)));
  }

  /// A row's geometries land in that row's group, and a group whose only row is null is never created.
  @Test
  public void testStUnionMVColumnGroupBySV() {
    byte[][][] rows = {
        {serialized(0, 0), serialized(1, 1)},
        {serialized(5, 5)}
    };

    StUnionAggregationFunction function = stUnion(true);
    GroupByResultHolder holder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(2, new int[]{0, 1}, holder,
        block(SyntheticBlockValSets.BytesMV.create(ROW1_NULL, rows)));

    Geometry group0 = function.extractGroupByResult(holder, 0);
    assertNotNull(group0);
    assertEquals(group0.getNumGeometries(), 2);
    assertNull(function.extractGroupByResult(holder, 1));
  }

  // ---------- SUMARRAYLONG / SUMARRAYDOUBLE ----------

  /// A null row contributes none of its array elements.
  @Test
  public void testSumArrayLongSkipsNullRows() {
    long[][] rows = {{1L, 2L}, {100L, 100L}, {10L, 20L}};

    SumArrayLongAggregationFunction function = new SumArrayLongAggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(3, holder, block(SyntheticBlockValSets.LongMV.create(ROW1_NULL, rows)));

    assertEquals(function.extractFinalResult(function.extractAggregationResult(holder)),
        new LongArrayList(new long[]{11L, 22L}));
  }

  @Test
  public void testSumArrayDoubleSkipsNullRows() {
    double[][] rows = {{1.5d, 2.5d}, {100d, 100d}, {10d, 20d}};

    SumArrayDoubleAggregationFunction function = new SumArrayDoubleAggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(3, holder, block(SyntheticBlockValSets.DoubleMV.create(ROW1_NULL, rows)));

    assertEquals(function.extractFinalResult(function.extractAggregationResult(holder)),
        new DoubleArrayList(new double[]{11.5d, 22.5d}));
  }

  /// Unlike the others here, the sum of no arrays is NULL in both modes, which is why neither needs a mode-aware
  /// branch in extractFinalResult.
  @Test
  public void testSumArrayEmptyAnswerIsNullInBothModes() {
    long[][] rows = {{1L}, {2L}};
    for (boolean nullHandlingEnabled : new boolean[]{true, false}) {
      SumArrayLongAggregationFunction function =
          new SumArrayLongAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
      assertNull(function.extractFinalResult(
              function.extractAggregationResult(function.createAggregationResultHolder())),
          "SUM_ARRAY over nothing must be NULL with nullHandlingEnabled=" + nullHandlingEnabled);
      if (nullHandlingEnabled) {
        AggregationResultHolder holder = function.createAggregationResultHolder();
        function.aggregate(2, holder, block(SyntheticBlockValSets.LongMV.create(allNull(2), rows)));
        assertNull(function.extractFinalResult(function.extractAggregationResult(holder)));
      }
    }
  }

  /// A zero-length block still reaches the range callback, and must not create the accumulator: doing so would
  /// turn the empty answer into an empty array instead of NULL.
  @Test
  public void testSumArrayZeroLengthBlockLeavesTheHolderUntouched() {
    long[][] rows = {{1L}, {2L}};

    for (boolean nullHandlingEnabled : new boolean[]{true, false}) {
      SumArrayLongAggregationFunction function =
          new SumArrayLongAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
      AggregationResultHolder holder = function.createAggregationResultHolder();
      function.aggregate(0, holder, block(SyntheticBlockValSets.LongMV.create(null, rows)));
      assertNull(function.extractAggregationResult(holder),
          "a zero-length block must leave the holder untouched with nullHandlingEnabled=" + nullHandlingEnabled);
    }
  }

  // ---------- HISTOGRAM ----------

  /// Two equal-length bins over [0, 10): [0, 5) and [5, 10].
  private static HistogramAggregationFunction histogram(boolean nullHandlingEnabled) {
    return new HistogramAggregationFunction(List.of(COLUMN,
        ExpressionContext.forLiteral(Literal.doubleValue(0)),
        ExpressionContext.forLiteral(Literal.doubleValue(10)),
        ExpressionContext.forLiteral(Literal.intValue(2))), nullHandlingEnabled);
  }

  private static DoubleArrayList histogramOf(HistogramAggregationFunction function, BlockValSet blockValSet,
      int length) {
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(length, holder, block(blockValSet));
    return function.extractFinalResult(function.extractAggregationResult(holder));
  }

  /// A null row is counted into no bin.
  @Test
  public void testHistogramSkipsNullRows() {
    double[] values = {1.0d, 7.0d, 6.0d};

    assertEquals(histogramOf(histogram(true), SyntheticBlockValSets.Double.create(ROW1_NULL, values), 3),
        new DoubleArrayList(new double[]{1.0d, 1.0d}));
  }

  /// With the option enabled the histogram of nothing is NULL; with it disabled it is the all-zero histogram.
  @Test
  public void testHistogramEmptyAnswerDiffersByMode() {
    double[] values = {1.0d, 7.0d, 6.0d};

    assertNull(histogramOf(histogram(true), SyntheticBlockValSets.Double.create(allNull(3), values), 3));
    assertEquals(histogramOf(histogram(false), SyntheticBlockValSets.Double.create(allNull(3), values), 0),
        new DoubleArrayList(new double[]{0.0d, 0.0d}));
  }

  /// Every value of a multi-value row is counted, which is a path this function did not support before.
  @Test
  public void testHistogramMVColumnCountsEveryValue() {
    double[][] rows = {{1.0d, 2.0d}, {7.0d}, {6.0d, 9.0d}};

    assertEquals(histogramOf(histogram(true), SyntheticBlockValSets.DoubleMV.create(ROW1_NULL, rows), 3),
        new DoubleArrayList(new double[]{2.0d, 2.0d}));
  }

  /// BIG_DECIMAL is numeric and now lands in the same bins as the other numeric types, single- and multi-value.
  @Test
  public void testHistogramBigDecimalColumn() {
    BigDecimal[] sv = {new BigDecimal("1.5"), new BigDecimal("99"), new BigDecimal("6.5")};
    assertEquals(histogramOf(histogram(true), SyntheticBlockValSets.BigDec.create(ROW1_NULL, sv), 3),
        new DoubleArrayList(new double[]{1.0d, 1.0d}));

    BigDecimal[][] mv = {{new BigDecimal("1.5"), new BigDecimal("2.5")}, {new BigDecimal("99")}};
    assertEquals(histogramOf(histogram(true), SyntheticBlockValSets.BigDecMV.create(ROW1_NULL, mv), 2),
        new DoubleArrayList(new double[]{2.0d, 0.0d}));
  }

  /// A row's values land in that row's group, and a group whose only row is null is never created.
  @Test
  public void testHistogramMVColumnGroupBySV() {
    double[][] rows = {{1.0d, 2.0d}, {7.0d}, {6.0d}};

    HistogramAggregationFunction function = histogram(true);
    GroupByResultHolder holder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(3, new int[]{0, 1, 0}, holder,
        block(SyntheticBlockValSets.DoubleMV.create(ROW1_NULL, rows)));

    assertEquals(function.extractGroupByResult(holder, 0), new DoubleArrayList(new double[]{2.0d, 1.0d}));
    assertNull(function.extractGroupByResult(holder, 1));
  }

  /// A null row is skipped for every group key it would have fed.
  @Test
  public void testHistogramMVColumnGroupByMV() {
    double[][] rows = {{1.0d, 2.0d}, {7.0d}};

    HistogramAggregationFunction function = histogram(true);
    GroupByResultHolder holder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(2, new int[][]{{0, 1}, {0, 1}}, holder,
        block(SyntheticBlockValSets.DoubleMV.create(ROW1_NULL, rows)));

    assertEquals(function.extractGroupByResult(holder, 0), new DoubleArrayList(new double[]{2.0d, 0.0d}));
    assertEquals(function.extractGroupByResult(holder, 1), new DoubleArrayList(new double[]{2.0d, 0.0d}));
  }

  // ---------- IDSET ----------

  private static IdSetAggregationFunction idSet(boolean nullHandlingEnabled) {
    return new IdSetAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
  }

  /// Only the ids of the rows that carry one are collected.
  @Test
  public void testIdSetSkipsNullRows() {
    int[] values = {10, 20, 30};

    IdSetAggregationFunction function = idSet(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(3, holder, block(SyntheticBlockValSets.Int.create(ROW1_NULL, values)));

    IdSet result = function.extractAggregationResult(holder);
    assertNotNull(result);
    assertTrue(result.contains(10));
    assertTrue(result.contains(30));
    assertFalse(result.contains(20));
  }

  /// With the option enabled the id set of nothing is NULL; with it disabled it is the empty id set.
  @Test
  public void testIdSetEmptyAnswerDiffersByMode() {
    int[] values = {10, 20};

    IdSetAggregationFunction enabled = idSet(true);
    AggregationResultHolder holder = enabled.createAggregationResultHolder();
    enabled.aggregate(2, holder, block(SyntheticBlockValSets.Int.create(allNull(2), values)));
    assertNull(enabled.extractAggregationResult(holder));
    assertNull(enabled.extractFinalResult(null));

    IdSetAggregationFunction disabled = idSet(false);
    IdSet empty = disabled.extractAggregationResult(disabled.createAggregationResultHolder());
    assertNotNull(empty);
    assertEquals(empty.getType(), IdSets.emptyIdSet().getType());
  }

  /// Every id of a multi-value row is collected.
  @Test
  public void testIdSetMVColumnCollectsEveryValue() {
    int[][] rows = {{10, 11}, {99}, {30}};

    IdSetAggregationFunction function = idSet(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(3, holder, block(SyntheticBlockValSets.IntMV.create(ROW1_NULL, rows)));

    IdSet result = function.extractAggregationResult(holder);
    assertNotNull(result);
    assertTrue(result.contains(10));
    assertTrue(result.contains(11));
    assertTrue(result.contains(30));
    assertFalse(result.contains(99));
  }

  /// A multi-value BYTES column was rejected before, although IdSets supports BYTES and the single-value case
  /// already worked.
  @Test
  public void testIdSetMVBytesColumn() {
    byte[][][] rows = {{{1, 2}, {3, 4}}, {{9, 9}}};

    IdSetAggregationFunction function = idSet(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, block(SyntheticBlockValSets.BytesMV.create(ROW1_NULL, rows)));

    IdSet result = function.extractAggregationResult(holder);
    assertNotNull(result);
    assertTrue(result.contains(new byte[]{1, 2}));
    assertTrue(result.contains(new byte[]{3, 4}));
    assertFalse(result.contains(new byte[]{9, 9}));
  }

  /// A row's ids land in that row's group, and a group whose only row is null is never created.
  @Test
  public void testIdSetMVColumnGroupBySV() {
    int[][] rows = {{10, 11}, {99}, {30}};

    IdSetAggregationFunction function = idSet(true);
    GroupByResultHolder holder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(3, new int[]{0, 1, 0}, holder,
        block(SyntheticBlockValSets.IntMV.create(ROW1_NULL, rows)));

    IdSet group0 = function.extractGroupByResult(holder, 0);
    assertNotNull(group0);
    assertTrue(group0.contains(10));
    assertTrue(group0.contains(30));
    assertNull(function.extractGroupByResult(holder, 1));
  }
}
