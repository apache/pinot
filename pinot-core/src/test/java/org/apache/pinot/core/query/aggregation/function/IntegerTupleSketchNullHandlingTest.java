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
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Null handling for the integer tuple sketch family, whose input is always a column of serialized sketches.
///
/// [AggregationFunctionNullContractTest] cannot reach these functions: its synthetic `BYTES` block supplies empty
/// byte arrays, which are not deserializable sketches. They are pinned in its skip list, so this is the only place
/// their null behaviour is checked.
public class IntegerTupleSketchNullHandlingTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");
  private static final int NUM_DOCS = 4;

  private static byte[][] serializedSketches() {
    byte[][] values = new byte[NUM_DOCS][];
    for (int i = 0; i < NUM_DOCS; i++) {
      IntegerTupleSketch sketch = new IntegerTupleSketch(4, IntegerSummary.Mode.Sum);
      sketch.update(i, 1);
      values[i] = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(sketch.compact());
    }
    return values;
  }

  private static Map<ExpressionContext, BlockValSet> block(RoaringBitmap nullBitmap) {
    return Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(nullBitmap, serializedSketches()));
  }

  private static RoaringBitmap allNull() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, NUM_DOCS);
    return bitmap;
  }

  private static TupleIntSketchAccumulator aggregate(IntegerTupleSketchAggregationFunction function,
      RoaringBitmap nullBitmap) {
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(NUM_DOCS, resultHolder, block(nullBitmap));
    return function.extractAggregationResult(resultHolder);
  }

  private static IntegerTupleSketchAggregationFunction raw(boolean nullHandlingEnabled) {
    return new IntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, nullHandlingEnabled);
  }

  /// Only the rows that carry a sketch are deserialized and unioned.
  @Test
  public void testNullRowsAreSkipped() {
    TupleIntSketchAccumulator accumulator = aggregate(raw(true), RoaringBitmap.bitmapOf(1, 3));

    assertNotNull(accumulator);
    assertEquals(accumulator.getResult().getRetainedEntries(), 2);
  }

  /// Nothing aggregated leaves the holder untouched, which is how the state reaches `extractFinalResult`.
  @Test
  public void testEveryRowNullYieldsNoIntermediateResult() {
    assertNull(aggregate(raw(true), allNull()));
  }

  /// A zero-length block still reaches the range callback, and must not mark the holder as aggregated.
  @Test
  public void testZeroLengthBlockLeavesTheHolderUntouched() {
    IntegerTupleSketchAggregationFunction function = raw(true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(0, resultHolder, block(null));

    assertNull(function.extractAggregationResult(resultHolder));
  }

  /// With the option enabled each function gives its own answer for an empty input.
  @Test
  public void testEmptyInputAnswersPerFunctionWhenEnabled() {
    assertNull(raw(true).extractFinalResult(null));
    assertEquals(new DistinctCountIntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, true)
        .extractFinalResult(null), 0L);
    assertNull(new SumValuesIntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, true)
        .extractFinalResult(null));
    assertNull(new AvgValueIntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, true)
        .extractFinalResult(null));
  }

  /// With the option disabled each function renders what its empty accumulator has always rendered: the raw variant
  /// a serialized empty sketch, the counting and summing variants zero, the average `NULL` for want of entries.
  @Test
  public void testEmptyInputRendersTheIdentityWhenDisabled() {
    IntegerTupleSketchAggregationFunction rawFunction = raw(false);
    Comparable<?> rendered = rawFunction.extractFinalResult(null);
    assertNotNull(rendered);
    assertEquals(rendered, rawFunction.extractFinalResult(rawFunction.emptyAccumulator()));

    assertEquals(new DistinctCountIntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum,
        false).extractFinalResult(null), 0L);
    assertEquals(new SumValuesIntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, false)
        .extractFinalResult(null), 0L);
    assertNull(new AvgValueIntegerTupleSketchAggregationFunction(List.of(COLUMN), IntegerSummary.Mode.Sum, false)
        .extractFinalResult(null));
  }
}
