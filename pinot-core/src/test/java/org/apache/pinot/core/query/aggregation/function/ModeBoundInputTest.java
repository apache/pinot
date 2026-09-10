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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies MODE normalizes physical values before counting, grouping and merging bound logical results.
public class ModeBoundInputTest {
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");

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
    ModeAggregationFunction function = mode(resultType);
    BlockValSet raw = new RowBasedBlockValSet(physicalType,
        List.of(new Object[]{smaller}, new Object[]{larger}, new Object[]{larger}, new Object[]{null}), 0, true);
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
        BlockValSet current = new RowBasedBlockValSet(resultType,
            List.of(new Object[]{convertedSmaller}, new Object[]{convertedSmaller}), 0, true);
        Map<?, Long> merged = function.merge(roundTrip(function, counts), aggregate(function, current, 2));
        assertEquals(merged, Map.of(convertedSmaller, 3L, convertedLarger, 2L));
        assertEquals(function.extractFinalResult(merged), convertedSmaller);
      }
    }
  }

  @Test
  public void testDictionaryValuesCoalescedByConversionKeepAllCounts()
      throws Exception {
    ModeAggregationFunction function = mode(ColumnDataType.TIMESTAMP);
    try (MutableDictionary dictionary = new StringOnHeapMutableDictionary()) {
      int[] ids = dictionary.index(new Object[]{"1", "01", "2"});
      BlockValSet block = SyntheticBlockValSets.DictIds.create(null, ids, dictionary, DataType.STRING);
      Map<?, Long> counts = roundTrip(function, aggregate(function, block, 3));
      assertEquals(counts, Map.of(1L, 2L, 2L, 1L));
      assertEquals(function.extractFinalResult(counts), Long.valueOf(1L));
    }
  }

  private static ModeAggregationFunction mode(ColumnDataType type) {
    FunctionContext function = new FunctionContext(FunctionContext.Type.AGGREGATION, "mode", List.of(VALUE),
        new AggregateCallBinding(List.of(type), type));
    return (ModeAggregationFunction) AggregationFunctionFactory.getAggregationFunction(function, true);
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
}
