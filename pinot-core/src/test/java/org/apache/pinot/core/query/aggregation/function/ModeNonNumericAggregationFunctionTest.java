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

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies serialization, exact timestamp values and null handling for the typed MODE implementations.
public class ModeNonNumericAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("value");

  @Test
  public void testStringIntermediateResultsRoundTripAndMerge() {
    ModeStringAggregationFunction function = new ModeStringAggregationFunction(List.of(EXPRESSION), true);
    var first = aggregateAndRoundTrip(function,
        SyntheticBlockValSets.Str.create(null, new String[]{"é", "é", "é", "苹果", "苹果", ""}), 6);
    var second = aggregateAndRoundTrip(function,
        SyntheticBlockValSets.Str.create(null, new String[]{"zebra", "zebra", "zebra", "苹果", "苹果", "\0"}), 6);

    assertEquals(function.getFinalResultColumnType(), ColumnDataType.STRING);
    assertEquals(function.extractFinalResult(first), "é");
    assertEquals(function.extractFinalResult(second), "zebra");
    assertEquals(function.extractFinalResult(function.merge(first, second)), "苹果");
  }

  @Test
  public void testTimestampIntermediateResultsPreserveLongPrecision() {
    // Adjacent long values above 2^53 become identical if converted through double.
    long earlier = 9_007_199_254_740_992L;
    long later = earlier + 1;
    ModeTimestampAggregationFunction minFunction =
        new ModeTimestampAggregationFunction(List.of(EXPRESSION), true);
    ModeTimestampAggregationFunction maxFunction =
        new ModeTimestampAggregationFunction(argumentsWithReducer("MAX"), true);
    var first = aggregateAndRoundTrip(minFunction, timestampValues(earlier, later, later), 3);
    var second = aggregateAndRoundTrip(minFunction, timestampValues(earlier), 1);

    assertEquals(minFunction.getFinalResultColumnType(), ColumnDataType.TIMESTAMP);
    assertEquals(minFunction.extractFinalResult(first), Long.valueOf(later));
    var merged = minFunction.merge(first, second);
    assertEquals(minFunction.extractFinalResult(merged), Long.valueOf(earlier));
    assertEquals(maxFunction.extractFinalResult(merged), Long.valueOf(later));
  }

  @Test
  public void testTimestampMergesPrimitiveAndGenericStates() {
    ModeTimestampAggregationFunction minFunction =
        new ModeTimestampAggregationFunction(List.of(EXPRESSION), true);
    ModeTimestampAggregationFunction maxFunction =
        new ModeTimestampAggregationFunction(argumentsWithReducer("MAX"), true);
    for (boolean primitiveLeft : new boolean[]{false, true}) {
      for (boolean primitiveRight : new boolean[]{false, true}) {
        Map<Long, Long> left = primitiveLeft ? new Long2LongOpenHashMap() : new HashMap<>();
        Map<Long, Long> right = primitiveRight ? new Long2LongOpenHashMap() : new HashMap<>();
        left.put(Long.MIN_VALUE, 2L);
        left.put(0L, 1L);
        right.put(Long.MIN_VALUE, 1L);
        right.put(Long.MAX_VALUE, 3L);

        Map<Long, Long> merged = minFunction.merge(left, right);
        assertSame(merged, left);
        assertEquals(merged, Map.of(Long.MIN_VALUE, 3L, 0L, 1L, Long.MAX_VALUE, 3L));
        assertEquals(minFunction.extractFinalResult(merged), Long.valueOf(Long.MIN_VALUE));
        assertEquals(maxFunction.extractFinalResult(merged), Long.valueOf(Long.MAX_VALUE));
        assertEquals(right, Map.of(Long.MIN_VALUE, 1L, Long.MAX_VALUE, 3L));
      }
    }
  }

  @Test
  public void testStringModeSkipsNullRowsForMultiValueGroupKeys() {
    ModeStringAggregationFunction function = new ModeStringAggregationFunction(List.of(EXPRESSION), true);
    GroupByResultHolder holder = function.createGroupByResultHolder(3, 3);
    BlockValSet values = SyntheticBlockValSets.Str.create(RoaringBitmap.bitmapOf(0, 2),
        new String[]{"ignored", "alpha", "ignored", "beta", "alpha"});
    function.aggregateGroupByMV(5, new int[][]{{0, 1}, {0}, {1, 2}, {0, 1}, {0}}, holder,
        Map.of(EXPRESSION, values));

    assertEquals(function.extractFinalResult(function.extractGroupByResult(holder, 0)), "alpha");
    assertEquals(function.extractFinalResult(function.extractGroupByResult(holder, 1)), "beta");
    assertNull(function.extractFinalResult(function.extractGroupByResult(holder, 2)));
  }

  @Test
  public void testEmptyResultsAreNullWithEitherNullHandlingMode() {
    for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
      ModeStringAggregationFunction stringFunction =
          new ModeStringAggregationFunction(List.of(EXPRESSION), nullHandlingEnabled);
      ModeTimestampAggregationFunction timestampFunction =
          new ModeTimestampAggregationFunction(List.of(EXPRESSION), nullHandlingEnabled);
      assertNull(stringFunction.extractFinalResult(null));
      assertNull(timestampFunction.extractFinalResult(null));
      assertNull(timestampFunction.extractFinalResult(new Long2LongOpenHashMap()));
      assertNull(timestampFunction.extractFinalResult(Map.of()));
      assertNull(stringFunction.extractFinalResult(
          stringFunction.extractAggregationResult(stringFunction.createAggregationResultHolder())));
      assertNull(timestampFunction.extractFinalResult(
          timestampFunction.extractAggregationResult(timestampFunction.createAggregationResultHolder())));
    }
  }

  @Test
  public void testNonNumericModesRejectAverageReducer() {
    IllegalArgumentException stringError = expectThrows(IllegalArgumentException.class,
        () -> new ModeStringAggregationFunction(argumentsWithReducer("AVG"), true));
    assertTrue(stringError.getMessage().contains("AVG"));
    assertTrue(stringError.getMessage().contains("STRING"));
    IllegalArgumentException timestampError = expectThrows(IllegalArgumentException.class,
        () -> new ModeTimestampAggregationFunction(argumentsWithReducer("AVG"), true));
    assertTrue(timestampError.getMessage().contains("AVG"));
    assertTrue(timestampError.getMessage().contains("TIMESTAMP"));
  }

  private static List<ExpressionContext> argumentsWithReducer(String reducer) {
    return List.of(EXPRESSION, ExpressionContext.forLiteral(Literal.stringValue(reducer)));
  }

  private static BlockValSet timestampValues(long... values) {
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.isSingleValue()).thenReturn(true);
    when(blockValSet.getValueType()).thenReturn(DataType.TIMESTAMP);
    when(blockValSet.getLongValuesSV()).thenReturn(values);
    return blockValSet;
  }

  private static <I, R extends Comparable<R>> I aggregateAndRoundTrip(AggregationFunction<I, R> function,
      BlockValSet values, int length) {
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(length, holder, Map.of(EXPRESSION, values));
    I intermediateResult = function.extractAggregationResult(holder);
    AggregationFunction.SerializedIntermediateResult serialized =
        function.serializeIntermediateResult(intermediateResult);
    I deserialized = function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
    assertEquals(deserialized, intermediateResult);
    return deserialized;
  }
}
