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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Null handling for the sketch-backed distinct counts, across the paths [AggregationFunctionNullContractTest]
/// cannot reach.
///
/// That harness drives one synthetic single-value block through `aggregate` only, so the group-by paths, the
/// multi-value column paths, and the serialized-sketch input are checked nowhere else. The `BYTES` value cases are
/// newer still — they arrived with UUID support and treat `BYTES` as a value to hash rather than as a serialized
/// sketch, which is a different branch from the logical-`BYTES` one.
public class DistinctCountSketchNullHandlingTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");
  private static final int NUM_DOCS = 4;
  private static final long[] VALUES = {10L, 20L, 30L, 40L};

  private static DistinctCountHLLAggregationFunction hll(boolean nullHandlingEnabled) {
    return new DistinctCountHLLAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> longBlock(@Nullable RoaringBitmap nullBitmap) {
    return Map.of(COLUMN, SyntheticBlockValSets.Long.create(nullBitmap, VALUES));
  }

  private static RoaringBitmap allNull() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, NUM_DOCS);
    return bitmap;
  }

  private static long aggregate(DistinctCountHLLAggregationFunction function, int length,
      Map<ExpressionContext, BlockValSet> block) {
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(length, resultHolder, block);
    return function.extractFinalResult(function.extractAggregationResult(resultHolder));
  }

  /// Only the rows that carry a value are counted.
  @Test
  public void testNullRowsAreSkipped() {
    assertEquals(aggregate(hll(true), NUM_DOCS, longBlock(RoaringBitmap.bitmapOf(1, 3))), 2L);
  }

  /// With the option disabled every row counts, including the column default the null rows read as. That is the
  /// answer this mode has always given.
  @Test
  public void testNullRowsCountedWhenOptionDisabled() {
    assertEquals(aggregate(hll(false), NUM_DOCS, longBlock(RoaringBitmap.bitmapOf(1, 3))), 4L);
  }

  /// A distinct count over nothing is zero in both modes, so the empty answer is unchanged by this work.
  @Test
  public void testEveryRowNullCountsZero() {
    assertEquals(aggregate(hll(true), NUM_DOCS, longBlock(allNull())), 0L);
    assertEquals(hll(true).extractFinalResult(null).longValue(), 0L);
    assertEquals(hll(false).extractFinalResult(null).longValue(), 0L);
  }

  /// A zero-length block still reaches the range callback, and must not aggregate anything.
  @Test
  public void testZeroLengthBlockCountsZero() {
    assertEquals(aggregate(hll(true), 0, longBlock(null)), 0L);
  }

  /// The group-by path skips null rows per group, and a group whose every row is null stays empty.
  @Test
  public void testGroupBySVSkipsNullRows() {
    DistinctCountHLLAggregationFunction function = hll(true);
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(4, 4);
    // Rows 0 and 2 go to group 0, rows 1 and 3 to group 1; rows 1 and 3 are null, so group 1 gets nothing
    function.aggregateGroupBySV(NUM_DOCS, new int[]{0, 1, 0, 1}, resultHolder,
        longBlock(RoaringBitmap.bitmapOf(1, 3)));

    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 0)).longValue(), 2L);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 1)).longValue(), 0L);
  }

  /// A null row is skipped for every group key it would have fed, not just the first.
  @Test
  public void testGroupByMVSkipsNullRowsForAllKeys() {
    DistinctCountHLLAggregationFunction function = hll(true);
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(4, 4);
    int[][] groupKeys = {{0, 1}, {0, 1}, {0, 1}, {0, 1}};
    function.aggregateGroupByMV(NUM_DOCS, groupKeys, resultHolder, longBlock(RoaringBitmap.bitmapOf(1, 3)));

    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 0)).longValue(), 2L);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 1)).longValue(), 2L);
  }

  /// A serialized sketch column is deserialized and merged only for the rows that carry one. This is the logical
  /// `BYTES` branch, which is distinct from the `BYTES` value case below.
  @Test
  public void testSerializedSketchRowsMergedOnlyWhenNotNull() throws Exception {
    HyperLogLog first = new HyperLogLog(8);
    first.offer("a");
    HyperLogLog second = new HyperLogLog(8);
    second.offer("b");
    byte[][] serialized = {
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(first),
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(second)
    };

    DistinctCountHLLAggregationFunction function = hll(true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(2, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(RoaringBitmap.bitmapOf(1), serialized)));

    // Only the first sketch was merged, so its single distinct value is the whole answer
    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 1L);
  }

  /// A multi-value column contributes every value of a non-null row and nothing at all from a null row.
  @Test
  public void testMVColumnSkipsNullRows() {
    long[][] values = {{1L, 2L}, {3L, 4L}, {5L, 6L}, {7L, 8L}};
    DistinctCountHLLAggregationFunction function = hll(true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(NUM_DOCS, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.LongMV.create(RoaringBitmap.bitmapOf(1, 3), values)));

    // Rows 0 and 2 survive, contributing 1, 2, 5 and 6
    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 4L);
  }

  /// A serialized CPC sketch column merges only the rows that carry one.
  ///
  /// The `bytes.length > 0` test inside `deserializeSketches` makes an empty default look skipped, but that is
  /// incidental: a column with a non-empty `defaultNullValue` deserializes into a real sketch, so the null row has to
  /// be excluded by the null bitmap rather than by the payload happening to be empty.
  @Test
  public void testCpcSerializedRowsMergedOnlyWhenNotNull() {
    CpcSketch first = new CpcSketch(12);
    first.update("a");
    CpcSketch second = new CpcSketch(12);
    second.update("b");
    byte[][] serialized = {first.toByteArray(), second.toByteArray()};

    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(2, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(RoaringBitmap.bitmapOf(1), serialized)));

    assertEquals(((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue(),
        1L);
  }

  /// The default sketch of `DISTINCTCOUNTTHETASKETCH` folds in only the rows that carry a value.
  ///
  /// Without a filter predicate this is the whole aggregation, and it is a separate loop from the filtered one, so a
  /// plain `DISTINCTCOUNTTHETASKETCH(sketchColumn)` is the path that this covers.
  @Test
  public void testThetaDefaultSketchMergesOnlyNotNullRows() {
    UpdatableThetaSketch first = UpdatableThetaSketch.builder().build();
    first.update("a");
    UpdatableThetaSketch second = UpdatableThetaSketch.builder().build();
    second.update("b");
    byte[][] serialized = {first.compact().toByteArray(), second.compact().toByteArray()};

    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(2, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(RoaringBitmap.bitmapOf(1), serialized)));

    assertEquals(((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue(),
        1L);
  }

  /// A null row is never deserialized, so its payload does not have to be a valid sketch.
  ///
  /// The default for a `BYTES` column is an empty array, which `ThetaSketch.wrap` cannot read. Deserializing the whole
  /// block up front meant that default was wrapped before any filtering could skip the row.
  @Test
  public void testThetaNullRowWithEmptyDefaultIsNotDeserialized() {
    UpdatableThetaSketch present = UpdatableThetaSketch.builder().build();
    present.update("a");
    // Row 1 is null and carries the BYTES default rather than a sketch
    byte[][] serialized = {present.compact().toByteArray(), new byte[0]};

    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(2, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(RoaringBitmap.bitmapOf(1), serialized)));

    assertEquals(((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue(),
        1L);
  }

  /// A multi-value `BYTES` column carries values to hash, not serialized sketches.
  ///
  /// The serialized-sketch branch reads the single-value representation, so it has to be reached only when the column
  /// is single-value; a multi-value `BYTES` column belongs on the value path with the other multi-value types.
  @Test
  public void testThetaMultiValueBytesColumnIsTreatedAsValues() {
    byte[][][] rows = {
        {new byte[]{1}, new byte[]{2}},
        {new byte[]{3}, new byte[]{4}}
    };

    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    // Row 1 is null, so only the two values of row 0 are counted
    function.aggregate(2, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.BytesMV.create(RoaringBitmap.bitmapOf(1), rows)));

    assertEquals(((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue(),
        2L);
  }

  /// A filtered `DISTINCTCOUNTTHETASKETCH` grouped by a multi-value column merges the sketch of the row it is on.
  ///
  /// A null row leaves a hole in the deserialized array, so reading that array at anything other than the row index
  /// can land on the hole. `CustomObjectAccumulator.apply` opens with a null check, which turns the wrong answer into
  /// a crash on `DISTINCTCOUNTTHETASKETCH(sketchCol, params, predicate, '$1') ... GROUP BY mvCol`.
  @Test
  public void testThetaFilteredGroupByMVMergesTheSketchOfItsOwnRow() {
    UpdatableThetaSketch present = UpdatableThetaSketch.builder().build();
    present.update("a");
    // Row 0 is null and carries the BYTES default, so its slot in the deserialized array stays empty
    byte[][] serialized = {new byte[0], present.compact().toByteArray()};

    ExpressionContext filterColumn = ExpressionContext.forIdentifier("dim");
    DistinctCountThetaSketchAggregationFunction function = new DistinctCountThetaSketchAggregationFunction(
        List.of(COLUMN, ExpressionContext.forLiteral(Literal.stringValue("nominalEntries=4096")),
            ExpressionContext.forLiteral(Literal.stringValue("dim = 1")),
            ExpressionContext.forLiteral(Literal.stringValue("$1"))), true);

    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(2, 2);
    // The predicate matches both rows; row 0 belongs to group 0 and row 1 to group 1
    function.aggregateGroupByMV(2, new int[][]{{0}, {1}}, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(RoaringBitmap.bitmapOf(0), serialized),
            filterColumn, SyntheticBlockValSets.Int.create(null, new int[]{1, 1})));

    assertEquals(((Number) function.extractFinalResult(function.extractGroupByResult(resultHolder, 1))).longValue(),
        1L);
    // Group 0 saw only the null row, so nothing was ever accumulated for it
    assertEquals(((Number) function.extractFinalResult(function.extractGroupByResult(resultHolder, 0))).longValue(),
        0L);
  }
}
