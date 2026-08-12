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
import org.apache.datasketches.frequencies.FrequentLongsSketch;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Null handling and row bounding for `FREQUENTLONGSSKETCH`.
///
/// [AggregationFunctionNullContractTest] drives these functions through one synthetic block shape and only through
/// `aggregate`, so the group-by paths and the row-bounding below are checked nowhere else.
public class FrequentSketchNullHandlingTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");
  private static final long[] VALUES = {10L, 20L, 30L, 40L};

  private static FrequentLongsSketchAggregationFunction longs(boolean nullHandlingEnabled) {
    return new FrequentLongsSketchAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> block(RoaringBitmap nullBitmap, long[] values) {
    return Map.of(COLUMN, SyntheticBlockValSets.Long.create(nullBitmap, values));
  }

  private static FrequentLongsSketch aggregate(FrequentLongsSketchAggregationFunction function, int length,
      RoaringBitmap nullBitmap, long[] values) {
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(length, resultHolder, block(nullBitmap, values));
    return function.extractAggregationResult(resultHolder);
  }

  private static long estimate(FrequentLongsSketch sketch, long item) {
    return sketch.getEstimate(item);
  }

  /// Only the rows that carry a value are counted.
  @Test
  public void testNullRowsAreSkipped() {
    FrequentLongsSketch sketch = aggregate(longs(true), VALUES.length, RoaringBitmap.bitmapOf(1, 3), VALUES);

    assertNotNull(sketch);
    assertEquals(estimate(sketch, 10L), 1L);
    assertEquals(estimate(sketch, 20L), 0L);
    assertEquals(estimate(sketch, 30L), 1L);
    assertEquals(estimate(sketch, 40L), 0L);
  }

  /// Aggregation must stop at `length`, not run to the end of the values array.
  ///
  /// `ProjectionBlockValSet` hands back the `DataBlockCache` array, and `DataBlockCache.initNewBlock` only clears the
  /// cache when the new block is larger than the last one. A short block after a full one therefore sees a longer
  /// array whose tail still holds the previous block's values, and anything that iterates the array rather than the
  /// range counts that tail a second time.
  @Test
  public void testValuesBeyondLengthAreNotCounted() {
    long[] oversized = {10L, 20L, 30L, 40L, 99L, 99L, 99L};

    FrequentLongsSketch sketch = aggregate(longs(false), 4, null, oversized);

    assertNotNull(sketch);
    assertEquals(estimate(sketch, 10L), 1L);
    assertEquals(estimate(sketch, 40L), 1L);
    assertEquals(estimate(sketch, 99L), 0L);
  }

  @Test
  public void testEveryRowNullYieldsNoIntermediateResult() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, VALUES.length);

    assertNull(aggregate(longs(true), VALUES.length, allNull, VALUES));
  }

  /// A zero-length block still reaches the range callback, and must not mark the holder as aggregated.
  @Test
  public void testZeroLengthBlockLeavesTheHolderUntouched() {
    assertNull(aggregate(longs(true), 0, null, VALUES));
  }

  /// The group-by path skips null rows per group, and a group whose every row is null is never created.
  @Test
  public void testGroupBySkipsNullRows() {
    FrequentLongsSketchAggregationFunction function = longs(true);
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(4, 4);
    // Rows 0 and 2 go to group 0, rows 1 and 3 to group 1; rows 1 and 3 are null, so group 1 gets nothing
    function.aggregateGroupBySV(VALUES.length, new int[]{0, 1, 0, 1}, resultHolder,
        block(RoaringBitmap.bitmapOf(1, 3), VALUES));

    FrequentLongsSketch group0 = function.extractGroupByResult(resultHolder, 0);
    assertNotNull(group0);
    assertEquals(estimate(group0, 10L), 1L);
    assertEquals(estimate(group0, 30L), 1L);
    assertNull(function.extractGroupByResult(resultHolder, 1));
  }

  /// With the option disabled the column default is counted, which is the answer this mode has always given, and the
  /// final result stays `NULL` for an untouched accumulator.
  @Test
  public void testOptionDisabledCountsNullRowsAndStillRendersNull() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, VALUES.length);

    FrequentLongsSketch sketch = aggregate(longs(false), VALUES.length, allNull, VALUES);
    assertNotNull(sketch);
    assertEquals(estimate(sketch, 10L), 1L);

    assertNull(longs(false).extractFinalResult(null));
    assertNull(longs(true).extractFinalResult(null));
  }

  /// A serialized sketch column is deserialized only for the rows that carry one.
  @Test
  public void testSerializedRowsAreDeserializedOnlyWhenNotNull() {
    FrequentLongsSketch first = new FrequentLongsSketch(32);
    first.update(10L);
    FrequentLongsSketch second = new FrequentLongsSketch(32);
    second.update(20L);
    byte[][] serialized = {first.toByteArray(), second.toByteArray()};

    FrequentLongsSketchAggregationFunction function = longs(true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(2, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(RoaringBitmap.bitmapOf(1), serialized)));

    FrequentLongsSketch sketch = function.extractAggregationResult(resultHolder);
    assertNotNull(sketch);
    assertEquals(estimate(sketch, 10L), 1L);
    assertEquals(estimate(sketch, 20L), 0L);
  }
}
