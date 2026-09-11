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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.aggregation.AggregationFunctionBinder;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BigDecimalOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.DoubleOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.FloatOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/// Verifies ANY_VALUE logical types before execution and through scalar and object intermediate transports.
public class AnyValueBindingTest {
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");

  @DataProvider
  public Object[][] scalarTypes() {
    return new Object[][]{
        {ColumnDataType.INT, 42},
        {ColumnDataType.LONG, 9_007_199_254_740_993L},
        {ColumnDataType.FLOAT, 1.25f},
        {ColumnDataType.DOUBLE, 2.75d},
        {ColumnDataType.BIG_DECIMAL, new BigDecimal("12345678901234567890.123456789")},
        {ColumnDataType.STRING, "value"},
        {ColumnDataType.BOOLEAN, 1},
        {ColumnDataType.TIMESTAMP, 9_007_199_254_740_993L},
        {ColumnDataType.JSON, "{\"key\":42}"},
        {ColumnDataType.BYTES, new byte[]{0, 1, 2, -1}},
        {ColumnDataType.UUID, new byte[16]}
    };
  }

  @Test(dataProvider = "scalarTypes")
  public void testLogicalTypeAndSerializedMerges(ColumnDataType type, Object value)
      throws Exception {
    AnyValueAggregationFunction function = bound(type, true);
    assertTypes(function, type);
    Object expected = comparableValue(value);
    Object rawResult = aggregate(function, raw(type, RoaringBitmap.bitmapOf(0), value));
    assertEquals(rawResult, expected);
    try (MutableDictionary dictionary = dictionary(type)) {
      int[] ids = dictionary.index(new Object[]{value, value, value});
      BlockValSet block = SyntheticBlockValSets.DictIds.create(RoaringBitmap.bitmapOf(0, 2), ids, dictionary,
          type.toDataType());
      Object dictionaryResult = aggregate(function, block);
      assertEquals(dictionaryResult, expected);
      Object merged = function.merge(scalarRoundTrip(function, rawResult), objectRoundTrip(function, dictionaryResult));
      assertEquals(function.extractFinalResult(merged), expected);
      assertTypes(function, type);
    }
  }

  @Test(dataProvider = "scalarTypes")
  public void testGroupsAndEmptyInputs(ColumnDataType type, Object value)
      throws Exception {
    AnyValueAggregationFunction function = bound(type, true);
    try (MutableDictionary dictionary = dictionary(type)) {
      int[] ids = dictionary.index(new Object[]{value, value, value});
      for (BlockValSet block : List.of(raw(type, RoaringBitmap.bitmapOf(0), value),
          SyntheticBlockValSets.DictIds.create(RoaringBitmap.bitmapOf(0), ids, dictionary, type.toDataType()))) {
        Map<ExpressionContext, BlockValSet> blocks = Map.of(VALUE, block);
        GroupByResultHolder single = function.createGroupByResultHolder(3, 3);
        function.aggregateGroupBySV(3, new int[]{0, 1, 2}, single, blocks);
        assertNull(function.extractGroupByResult(single, 0));
        assertEquals(function.extractGroupByResult(single, 1), comparableValue(value));
        assertEquals(function.extractGroupByResult(single, 2), comparableValue(value));
        GroupByResultHolder multi = function.createGroupByResultHolder(3, 3);
        function.aggregateGroupByMV(3, new int[][]{{0}, {1, 2}, {2}}, multi, blocks);
        assertNull(function.extractGroupByResult(multi, 0));
        assertEquals(function.extractGroupByResult(multi, 1), comparableValue(value));
        assertEquals(function.extractGroupByResult(multi, 2), comparableValue(value));
      }
    }
    assertNull(aggregate(function, raw(type, RoaringBitmap.bitmapOf(0, 1, 2), value)));
    for (boolean nullHandlingEnabled : new boolean[]{true, false}) {
      AnyValueAggregationFunction empty = bound(type, nullHandlingEnabled);
      assertTypes(empty, type);
      assertNull(empty.extractAggregationResult(empty.createAggregationResultHolder()));
      assertNull(empty.extractFinalResult(null));
      assertNull(objectRoundTrip(empty, null));
    }
  }

  @Test
  public void testWidenedPhysicalInputs()
      throws Exception {
    assertWidenedInput(ColumnDataType.INT, 42, ColumnDataType.LONG, 42L);
    assertWidenedInput(ColumnDataType.FLOAT, 1.25f, ColumnDataType.DOUBLE, 1.25d);
    assertWidenedInput(ColumnDataType.LONG, 9_007_199_254_740_993L, ColumnDataType.BIG_DECIMAL,
        new BigDecimal("9007199254740993"));
  }

  private static void assertWidenedInput(ColumnDataType physicalType, Object physicalValue,
      ColumnDataType logicalType, Object expected)
      throws Exception {
    AnyValueAggregationFunction function = bound(logicalType, true);
    BlockValSet raw = spy(raw(physicalType, null, physicalValue));
    switch (logicalType) {
      case LONG:
        doReturn(new long[]{(long) expected, (long) expected, (long) expected}).when(raw).getLongValuesSV();
        break;
      case DOUBLE:
        doReturn(new double[]{(double) expected, (double) expected, (double) expected}).when(raw).getDoubleValuesSV();
        break;
      case BIG_DECIMAL:
        doReturn(new BigDecimal[]{(BigDecimal) expected, (BigDecimal) expected, (BigDecimal) expected})
            .when(raw).getBigDecimalValuesSV();
        break;
      default:
        throw new IllegalArgumentException(logicalType.toString());
    }
    Object rawResult = aggregate(function, raw);
    assertEquals(rawResult, expected);
    try (MutableDictionary dictionary = dictionary(physicalType)) {
      int[] ids = dictionary.index(new Object[]{physicalValue, physicalValue, physicalValue});
      Object dictionaryResult = aggregate(function,
          SyntheticBlockValSets.DictIds.create(null, ids, dictionary, physicalType.toDataType()));
      assertEquals(dictionaryResult, expected);
      Object merged = function.merge(scalarRoundTrip(function, rawResult), objectRoundTrip(function, dictionaryResult));
      assertEquals(function.extractFinalResult(merged), expected);
      assertTypes(function, logicalType);
    }
  }

  @Test
  public void testJsonSchemaBinding() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("value", DataType.JSON).build();
    FunctionContext call = AggregationFunctionBinder.bind(
        RequestContextUtils.getExpression("ANY_VALUE(value)").getFunction(), schema);
    AggregateCallBinding binding = call.getAggregationBinding();
    assertEquals(binding.getArgumentTypes(), List.of(ColumnDataType.JSON));
    assertEquals(binding.getResultType(), ColumnDataType.STRING);
    AnyValueAggregationFunction function =
        (AnyValueAggregationFunction) new AnyValueAggregationFunction.Provider().create(call, true);
    assertTypes(function, ColumnDataType.STRING);
    assertEquals(aggregate(function, raw(ColumnDataType.JSON, null, "{\"key\":42}")), "{\"key\":42}");
    assertTypes(function, ColumnDataType.STRING);
  }

  @Test
  public void testRejectsInvalidBindings() {
    assertThrows(IllegalArgumentException.class, () -> bound(ColumnDataType.STRING_ARRAY, true));
    assertThrows(IllegalArgumentException.class, () -> new AnyValueAggregationFunction(List.of(VALUE), true,
        new AggregateCallBinding(List.of(ColumnDataType.INT), ColumnDataType.STRING)));
    assertThrows(IllegalArgumentException.class, () -> new AnyValueAggregationFunction(List.of(VALUE), true,
        new AggregateCallBinding(List.of(ColumnDataType.TIMESTAMP), ColumnDataType.LONG)));
    assertThrows(IllegalArgumentException.class, () -> new AnyValueAggregationFunction(List.of(VALUE), true,
        new AggregateCallBinding(List.of(ColumnDataType.BOOLEAN), ColumnDataType.INT)));
  }

  @Test
  public void testUnboundConstructorRetainsLegacyInference() {
    AnyValueAggregationFunction legacy = new AnyValueAggregationFunction(List.of(VALUE), true);
    assertEquals(legacy.getFinalResultColumnType(), ColumnDataType.STRING);
    assertEquals(aggregate(legacy, raw(ColumnDataType.LONG, null, 123L)), 123L);
    assertEquals(legacy.getFinalResultColumnType(), ColumnDataType.LONG);
  }

  private static void assertTypes(AnyValueAggregationFunction function, ColumnDataType type) {
    assertEquals(function.getFinalResultColumnType(), type);
    assertEquals(function.getIntermediateResultColumnType(), type.getStoredType());
  }

  private static AnyValueAggregationFunction bound(ColumnDataType type, boolean nullHandlingEnabled) {
    return new AnyValueAggregationFunction(List.of(VALUE), nullHandlingEnabled,
        new AggregateCallBinding(List.of(type), type));
  }

  private static Object aggregate(AnyValueAggregationFunction function, BlockValSet block) {
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(3, holder, Map.of(VALUE, block));
    return function.extractAggregationResult(holder);
  }

  private static Object scalarRoundTrip(AnyValueAggregationFunction function, Object value)
      throws Exception {
    ColumnDataType type = function.getIntermediateResultColumnType();
    DataSchema schema = new DataSchema(new String[]{"anyValue(value)"}, new ColumnDataType[]{type});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    builder.startRow();
    AggregationFunctionUtils.setIntermediateResult(builder, type, 0, value);
    builder.finishRow();
    DataTable table = DataTableFactory.getDataTable(builder.build().toBytes());
    return AggregationFunctionUtils.getIntermediateResult(function, table, type, 0, 0);
  }

  private static Object objectRoundTrip(AnyValueAggregationFunction function, Object value) {
    AggregationFunction.SerializedIntermediateResult serialized = function.serializeIntermediateResult(value);
    return function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
  }

  private static Object comparableValue(Object value) {
    return value instanceof byte[] ? new ByteArray((byte[]) value) : value;
  }

  private static BlockValSet raw(ColumnDataType type, RoaringBitmap nulls, Object value) {
    switch (type.getStoredType()) {
      case INT:
        return SyntheticBlockValSets.Int.create(nulls, new int[]{(int) value, (int) value, (int) value});
      case LONG:
        return SyntheticBlockValSets.Long.create(nulls, new long[]{(long) value, (long) value, (long) value});
      case FLOAT:
        return SyntheticBlockValSets.Float.create(nulls, new float[]{(float) value, (float) value, (float) value});
      case DOUBLE:
        return SyntheticBlockValSets.Double.create(nulls, new double[]{(double) value, (double) value, (double) value});
      case BIG_DECIMAL:
        return SyntheticBlockValSets.BigDec.create(nulls,
            new BigDecimal[]{(BigDecimal) value, (BigDecimal) value, (BigDecimal) value});
      case STRING:
        return SyntheticBlockValSets.Str.create(nulls, new String[]{(String) value, (String) value, (String) value});
      case BYTES:
        return SyntheticBlockValSets.Bytes.create(nulls, new byte[][]{(byte[]) value, (byte[]) value, (byte[]) value});
      default:
        throw new IllegalArgumentException(type.toString());
    }
  }

  private static MutableDictionary dictionary(ColumnDataType type) {
    switch (type.getStoredType()) {
      case INT:
        return new IntOnHeapMutableDictionary();
      case LONG:
        return new LongOnHeapMutableDictionary();
      case FLOAT:
        return new FloatOnHeapMutableDictionary();
      case DOUBLE:
        return new DoubleOnHeapMutableDictionary();
      case BIG_DECIMAL:
        return new BigDecimalOnHeapMutableDictionary();
      case STRING:
        return new StringOnHeapMutableDictionary();
      case BYTES:
        return new BytesOnHeapMutableDictionary();
      default:
        throw new IllegalArgumentException(type.toString());
    }
  }
}
