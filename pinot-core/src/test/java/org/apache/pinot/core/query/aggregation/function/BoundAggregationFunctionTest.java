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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/// Verifies immutable aggregate bindings across accumulation, grouping and serialized intermediate merges.
public class BoundAggregationFunctionTest {
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");
  private static final ExpressionContext TIME = ExpressionContext.forIdentifier("time");

  @DataProvider
  public Object[][] modeTypes() {
    return new Object[][]{
        {ColumnDataType.STRING, "", "zebra"},
        {ColumnDataType.TIMESTAMP, 9_007_199_254_740_992L, 9_007_199_254_740_993L},
        {ColumnDataType.BOOLEAN, 0, 1}
    };
  }

  @Test(dataProvider = "modeTypes")
  public void testModeRawDictionaryMerge(ColumnDataType type, Object smaller, Object larger)
      throws Exception {
    ModeAggregationFunction min = mode(type, "MIN", true);
    assertEquals(min.getFinalResultColumnType(), type);
    assertEquals(min.getIntermediateResultColumnType(), ColumnDataType.OBJECT);
    assertEquals(min.getResultColumnName(), "mode(value)");
    Map<?, Long> rawCounts = aggregate(min, raw(type, null, smaller, larger, larger), 3);
    try (MutableDictionary dictionary = dictionary(type)) {
      int[] ids = dictionary.index(new Object[]{smaller, smaller, larger, larger});
      BlockValSet block = SyntheticBlockValSets.DictIds.create(RoaringBitmap.bitmapOf(3), ids, dictionary,
          type.toDataType());
      Map<?, Long> dictionaryCounts = aggregate(min, block, 4);
      Map<?, Long> merged = min.merge(roundTrip(min, rawCounts), roundTrip(min, dictionaryCounts));
      assertEquals(min.extractFinalResult(merged), smaller);
      assertEquals(mode(type, "MAX", true).extractFinalResult(merged), larger);
      assertEquals(mode(type, "min", true).extractFinalResult(merged), smaller);
      assertEquals(merged.get(smaller), Long.valueOf(3));
      assertEquals(merged.get(larger), Long.valueOf(3));
    }
  }

  @Test(dataProvider = "modeTypes")
  public void testModeGroupingAndNulls(ColumnDataType type, Object smaller, Object larger) {
    ModeAggregationFunction function = mode(type, "MIN", true);
    BlockValSet block = raw(type, RoaringBitmap.bitmapOf(3), smaller, larger, larger, smaller);
    Map<ExpressionContext, BlockValSet> blocks = Map.of(VALUE, block);
    GroupByResultHolder single = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(4, new int[]{0, 0, 1, 1}, single, blocks);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(single, 0)), smaller);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(single, 1)), larger);
    GroupByResultHolder multi = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(4, new int[][]{{0, 1}, {0}, {1}, {}}, multi, blocks);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(multi, 0)), smaller);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(multi, 1)), smaller);

    assertNull(function.extractFinalResult(aggregate(function,
        raw(type, RoaringBitmap.bitmapOf(0, 1), smaller, larger), 2)));
    for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
      ModeAggregationFunction empty = mode(type, "MIN", nullHandlingEnabled);
      assertNull(empty.extractFinalResult(empty.extractAggregationResult(empty.createAggregationResultHolder())));
      assertNull(empty.extractFinalResult(Map.of()));
      assertNull(empty.extractFinalResult(null));
    }
  }

  @Test(dataProvider = "modeTypes")
  public void testModeRejectsNonNumericAverage(ColumnDataType type, Object smaller, Object larger) {
    assertThrows(IllegalArgumentException.class,
        () -> new ModeAggregationFunction(modeArguments("AVG"), true, type));
  }

  @Test
  public void testLegacyNumericMode() {
    ModeAggregationFunction function = new ModeAggregationFunction(List.of(VALUE), false);
    assertEquals(function.getFinalResultColumnType(), ColumnDataType.DOUBLE);
    Map<?, Long> empty = function.extractAggregationResult(function.createAggregationResultHolder());
    assertEquals(function.extractFinalResult(empty), Double.NEGATIVE_INFINITY);
    BlockValSet block = SyntheticBlockValSets.Long.create(null, new long[]{5L, 5L, 8L});
    assertEquals(function.extractFinalResult(aggregate(function, block, 3)), Double.valueOf(5));
    assertThrows(IllegalArgumentException.class, () -> new ModeAggregationFunction(
        RequestContextUtils.getExpression("mode(value, 'MIN', 'STRING')").getFunction().getArguments(), true));
  }

  @DataProvider
  public Object[][] valueWithTimeTypes() {
    return new Object[][]{
        {ColumnDataType.INT}, {ColumnDataType.LONG}, {ColumnDataType.FLOAT}, {ColumnDataType.DOUBLE},
        {ColumnDataType.STRING}, {ColumnDataType.BOOLEAN}, {ColumnDataType.TIMESTAMP}
    };
  }

  @Test(dataProvider = "valueWithTimeTypes")
  public void testValueWithTimeBinding(ColumnDataType type) {
    for (String name : List.of("firstWithTime", "lastWithTime")) {
      AggregationFunction<?, ?> function = valueWithTime(name, type);
      assertEquals(function.getFinalResultColumnType(), type);
      assertEquals(function.getIntermediateResultColumnType(), ColumnDataType.OBJECT);
      assertEquals(function.getResultColumnName(), name.toLowerCase() + "(value,time)");
      assertNull(function.extractFinalResult(null));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimestampValueWithTimePreservesPrecision() {
    long earlierValue = 9_007_199_254_740_993L;
    long laterValue = earlierValue + 2;
    for (String name : List.of("firstWithTime", "lastWithTime")) {
      AggregationFunction<ValueLongPair<Long>, Long> function =
          (AggregationFunction<ValueLongPair<Long>, Long>) valueWithTime(name, ColumnDataType.TIMESTAMP);
      AggregationResultHolder holder = function.createAggregationResultHolder();
      function.aggregate(3, holder, Map.of(
          VALUE, SyntheticBlockValSets.Long.create(RoaringBitmap.bitmapOf(1), new long[]{earlierValue, 0, laterValue}),
          TIME, SyntheticBlockValSets.Long.create(null, new long[]{1, 0, 2})));
      AggregationFunction.SerializedIntermediateResult serialized =
          function.serializeIntermediateResult(function.extractAggregationResult(holder));
      ValueLongPair<Long> intermediate = function.deserializeIntermediateResult(
          new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
      assertEquals(function.extractFinalResult(intermediate),
          Long.valueOf(name.equals("firstWithTime") ? earlierValue : laterValue));
      assertNull(function.extractAggregationResult(function.createAggregationResultHolder()));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testValueWithTimeExtremesAreNotEmpty() {
    for (String name : List.of("firstWithTime", "lastWithTime")) {
      AggregationFunction<ValueLongPair<Long>, Long> function =
          (AggregationFunction<ValueLongPair<Long>, Long>) valueWithTime(name, ColumnDataType.LONG);
      long time = name.equals("firstWithTime") ? Long.MAX_VALUE : Long.MIN_VALUE;
      BlockValSet times = SyntheticBlockValSets.Long.create(null, new long[]{time});
      AggregationResultHolder holder = function.createAggregationResultHolder();
      function.aggregate(1, holder, Map.of(
          VALUE, SyntheticBlockValSets.Long.create(null, new long[]{Long.MIN_VALUE}), TIME, times));
      assertEquals(function.extractFinalResult(function.extractAggregationResult(holder)),
          Long.valueOf(Long.MIN_VALUE));

      // An all-null block must not replace the preceding real row, even when its time matches the old sentinel.
      Map<ExpressionContext, BlockValSet> nullBlock = Map.of(
          VALUE, SyntheticBlockValSets.Long.create(RoaringBitmap.bitmapOf(0), new long[]{123}), TIME, times);
      function.aggregate(1, holder, nullBlock);
      assertEquals(function.extractFinalResult(function.extractAggregationResult(holder)),
          Long.valueOf(Long.MIN_VALUE));
      AggregationResultHolder empty = function.createAggregationResultHolder();
      function.aggregate(1, empty, nullBlock);
      assertNull(function.extractAggregationResult(empty));
      GroupByResultHolder groups = function.createGroupByResultHolder(1, 1);
      function.aggregateGroupBySV(1, new int[]{0}, groups, nullBlock);
      assertNull(function.extractGroupByResult(groups, 0));
    }
  }

  @DataProvider
  public Object[][] conversions() {
    return new Object[][]{
        {ColumnDataType.INT, ColumnDataType.TIMESTAMP, 1, 2, 1L, 2L},
        {ColumnDataType.LONG, ColumnDataType.BOOLEAN, 0L, 1L, 0, 1},
        {ColumnDataType.INT, ColumnDataType.STRING, 1, 2, "1", "2"}
    };
  }

  @Test(dataProvider = "conversions")
  public void testPhysicalInputConversion(ColumnDataType physicalType, ColumnDataType resultType, Object smaller,
      Object larger, Object convertedSmaller, Object convertedLarger)
      throws Exception {
    ModeAggregationFunction function = mode(resultType, "MIN", true);
    BlockValSet raw = raw(physicalType, RoaringBitmap.bitmapOf(3), smaller, larger, larger, smaller);
    try (MutableDictionary dictionary = physicalType == ColumnDataType.INT
        ? new IntOnHeapMutableDictionary()
        : new LongOnHeapMutableDictionary()) {
      int[] ids = dictionary.index(new Object[]{smaller, larger, larger, smaller});
      BlockValSet encoded = SyntheticBlockValSets.DictIds.create(RoaringBitmap.bitmapOf(3), ids, dictionary,
          physicalType.toDataType());
      for (BlockValSet block : List.of(raw, encoded)) {
        Map<?, Long> counts = aggregate(function, block, 4);
        assertEquals(counts, Map.of(convertedSmaller, 1L, convertedLarger, 2L));
        assertEquals(roundTrip(function, counts), counts);
        assertEquals(function.extractFinalResult(counts), convertedLarger);

        Map<ExpressionContext, BlockValSet> blocks = Map.of(VALUE, block);
        GroupByResultHolder single = function.createGroupByResultHolder(2, 2);
        function.aggregateGroupBySV(4, new int[]{0, 0, 1, 1}, single, blocks);
        assertEquals(roundTrip(function, function.extractGroupByResult(single, 0)),
            Map.of(convertedSmaller, 1L, convertedLarger, 1L));
        assertEquals(roundTrip(function, function.extractGroupByResult(single, 1)), Map.of(convertedLarger, 1L));
        GroupByResultHolder multi = function.createGroupByResultHolder(2, 2);
        function.aggregateGroupByMV(4, new int[][]{{0, 1}, {0}, {1}, {0, 1}}, multi, blocks);
        for (int group = 0; group < 2; group++) {
          assertEquals(roundTrip(function, function.extractGroupByResult(multi, group)),
              Map.of(convertedSmaller, 1L, convertedLarger, 1L));
        }

        // Another segment already has the bound physical representation. Its count must merge into the same key.
        BlockValSet current = raw(resultType, null, convertedSmaller, convertedSmaller);
        Map<?, Long> merged = function.merge(roundTrip(function, counts), aggregate(function, current, 2));
        assertEquals(merged, Map.of(convertedSmaller, 3L, convertedLarger, 2L));
        assertEquals(function.extractFinalResult(merged), convertedSmaller);
      }
    }
  }

  @Test
  public void testDictionaryValuesCoalescedByConversionKeepAllCounts()
      throws Exception {
    ModeAggregationFunction function = mode(ColumnDataType.TIMESTAMP, "MIN", true);
    try (MutableDictionary dictionary = new StringOnHeapMutableDictionary()) {
      int[] ids = dictionary.index(new Object[]{"1", "01", "2"});
      BlockValSet block = SyntheticBlockValSets.DictIds.create(null, ids, dictionary, DataType.STRING);
      Map<?, Long> counts = roundTrip(function, aggregate(function, block, 3));
      assertEquals(counts, Map.of(1L, 2L, 2L, 1L));
      assertEquals(function.extractFinalResult(counts), Long.valueOf(1L));
    }
  }

  private static ModeAggregationFunction mode(ColumnDataType type, String reducer, boolean nullHandlingEnabled) {
    FunctionContext function = new FunctionContext(FunctionContext.Type.AGGREGATION, "mode", modeArguments(reducer),
        new AggregateCallBinding(List.of(type, ColumnDataType.STRING), type));
    return (ModeAggregationFunction) AggregationFunctionFactory.getAggregationFunction(function, nullHandlingEnabled);
  }

  private static List<ExpressionContext> modeArguments(String reducer) {
    return RequestContextUtils.getExpression("mode(value, '" + reducer + "')").getFunction().getArguments();
  }

  private static AggregationFunction<?, ?> valueWithTime(String name, ColumnDataType type) {
    FunctionContext function = new FunctionContext(FunctionContext.Type.AGGREGATION, name, List.of(VALUE, TIME),
        new AggregateCallBinding(List.of(type, ColumnDataType.LONG), type));
    return AggregationFunctionFactory.getAggregationFunction(function, true);
  }

  private static Map<?, Long> aggregate(ModeAggregationFunction function, BlockValSet block, int length) {
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(length, holder, Map.of(VALUE, block));
    return function.extractAggregationResult(holder);
  }

  private static Map<?, Long> roundTrip(ModeAggregationFunction function, Map<?, Long> counts) {
    AggregationFunction.SerializedIntermediateResult serialized = function.serializeIntermediateResult(counts);
    return function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
  }

  private static BlockValSet raw(ColumnDataType type, RoaringBitmap nulls, Object... values) {
    Object[][] rows = Arrays.stream(values).map(value -> new Object[]{value}).toArray(Object[][]::new);
    if (nulls != null) {
      nulls.forEach((int i) -> rows[i][0] = null);
    }
    return new RowBasedBlockValSet(type, Arrays.asList(rows), 0, true);
  }

  private static MutableDictionary dictionary(ColumnDataType type) {
    switch (type) {
      case STRING:
        return new StringOnHeapMutableDictionary();
      case TIMESTAMP:
        return new LongOnHeapMutableDictionary();
      case BOOLEAN:
        return new IntOnHeapMutableDictionary();
      default:
        throw new IllegalArgumentException(type.toString());
    }
  }
}
