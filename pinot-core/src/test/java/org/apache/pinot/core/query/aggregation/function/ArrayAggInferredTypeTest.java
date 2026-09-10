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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationFunctionBinder;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/// Verifies inferred array shapes and the existing accumulator wire format, grouping, and empty-result contracts.
@SuppressWarnings({"rawtypes", "unchecked"})
public class ArrayAggInferredTypeTest {
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");

  @DataProvider
  public Object[][] scalarTypes() {
    RoaringBitmap nulls = RoaringBitmap.bitmapOf(3);
    return new Object[][]{
        {DataType.INT, SyntheticBlockValSets.Int.create(nulls, new int[]{2, 1, 1, 9}),
            ColumnDataType.INT_ARRAY, new int[]{2, 1, 1}},
        {DataType.LONG, SyntheticBlockValSets.Long.create(nulls, new long[]{2, 1, 1, 9}),
            ColumnDataType.LONG_ARRAY, new long[]{2, 1, 1}},
        {DataType.FLOAT, SyntheticBlockValSets.Float.create(nulls, new float[]{2, 1, 1, 9}),
            ColumnDataType.FLOAT_ARRAY, new float[]{2, 1, 1}},
        {DataType.DOUBLE, SyntheticBlockValSets.Double.create(nulls, new double[]{2, 1, 1, 9}),
            ColumnDataType.DOUBLE_ARRAY, new double[]{2, 1, 1}},
        {DataType.BIG_DECIMAL, SyntheticBlockValSets.BigDec.create(nulls,
            new BigDecimal[]{new BigDecimal("2.1"), BigDecimal.ONE, BigDecimal.ONE, BigDecimal.TEN}),
            ColumnDataType.BIG_DECIMAL_ARRAY, new String[]{"2.1", "1", "1"}},
        {DataType.BOOLEAN, SyntheticBlockValSets.Int.create(nulls, new int[]{0, 1, 1, 0}),
            ColumnDataType.BOOLEAN_ARRAY, new boolean[]{false, true, true}},
        {DataType.TIMESTAMP, SyntheticBlockValSets.Long.create(nulls, new long[]{2, 1, 1, 9}),
            ColumnDataType.TIMESTAMP_ARRAY,
            new String[]{new Timestamp(2).toString(), new Timestamp(1).toString(), new Timestamp(1).toString()}},
        {DataType.STRING, SyntheticBlockValSets.Str.create(nulls, new String[]{"b", "a", "a", "z"}),
            ColumnDataType.STRING_ARRAY, new String[]{"b", "a", "a"}},
        {DataType.JSON, SyntheticBlockValSets.Str.create(nulls, new String[]{"{}", "[]", "[]", "null"}),
            ColumnDataType.STRING_ARRAY, new String[]{"{}", "[]", "[]"}},
        {DataType.BYTES, SyntheticBlockValSets.Bytes.create(nulls, new byte[][]{{2}, {1}, {1}, {9}}),
            ColumnDataType.BYTES_ARRAY, new String[]{"02", "01", "01"}},
        {DataType.UUID, SyntheticBlockValSets.Bytes.create(nulls,
            new byte[][]{uuid(2), uuid(1), uuid(1), uuid(9)}), ColumnDataType.UUID_ARRAY,
            new String[]{"00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000001"}}
    };
  }

  @Test(dataProvider = "scalarTypes")
  public void testScalarAccumulationAndWireMerge(DataType type, BlockValSet block, ColumnDataType resultType,
      Object expected) {
    AggregationFunction function = function("ARRAY_AGG(value)", type, false);
    assertEquals(function.getFinalResultColumnType(), resultType);
    assertEquals(function.getIntermediateResultColumnType(), ColumnDataType.OBJECT);
    assertEquals(function.getResultColumnName(), "arrayagg(value)");
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(4, holder, Map.of(VALUE, block));
    Object state = roundTrip(function, function.extractAggregationResult(holder));
    assertEquals(format(resultType, function.extractFinalResult(state)), expected);
    Object merged = function.merge(roundTrip(function, state), roundTrip(function, state));
    assertEquals(Array.getLength(format(resultType, function.extractFinalResult(merged))), 6);
    assertEquals(Array.getLength(format(resultType, function.extractFinalResult(null))), 0);

    AggregationFunction distinct = function("ARRAY_AGG(value, true)", type, false);
    holder = distinct.createAggregationResultHolder();
    distinct.aggregate(4, holder, Map.of(VALUE, block));
    state = roundTrip(distinct, distinct.extractAggregationResult(holder));
    merged = distinct.merge(roundTrip(distinct, state), roundTrip(distinct, state));
    assertEquals(Array.getLength(format(resultType, distinct.extractFinalResult(merged))), 2);
    GroupByResultHolder groups = distinct.createGroupByResultHolder(2, 2);
    distinct.aggregateGroupBySV(4, new int[]{0, 1, 1, 0}, groups, Map.of(VALUE, block));
    assertEquals(Array.getLength(format(resultType,
        distinct.extractFinalResult(distinct.extractGroupByResult(groups, 0)))), 1);
    assertEquals(Array.getLength(format(resultType,
        distinct.extractFinalResult(distinct.extractGroupByResult(groups, 1)))), 1);
  }

  @Test
  public void testMultiValueFlatteningAndAllNulls() {
    AggregationFunction function = function("ARRAY_AGG(value, true)", DataType.INT, true);
    assertEquals(function.getFinalResultColumnType(), ColumnDataType.INT_ARRAY);
    BlockValSet block = SyntheticBlockValSets.IntMV.create(RoaringBitmap.bitmapOf(2),
        new int[][]{{2, 1}, {1}, {9}});
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(3, holder, Map.of(VALUE, block));
    int[] result = (int[]) ColumnDataType.INT_ARRAY.convert(function.extractFinalResult(
        roundTrip(function, function.extractAggregationResult(holder))));
    Arrays.sort(result);
    assertEquals(result, new int[]{1, 2});

    holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, Map.of(VALUE,
        SyntheticBlockValSets.IntMV.create(RoaringBitmap.bitmapOf(0, 1), new int[][]{{1}, {2}})));
    assertEquals((int[]) ColumnDataType.INT_ARRAY.convert(
        function.extractFinalResult(function.extractAggregationResult(holder))), new int[0]);
  }

  @Test
  public void testExplicitTypeCompatibilityAndInvalidOptions() {
    FunctionContext context = bind("ARRAY_AGG(value, 'LONG', true)", DataType.INT, false);
    assertNull(context.getAggregationBinding());
    assertEquals(AggregationFunctionFactory.getAggregationFunction(context, true).getFinalResultColumnType(),
        ColumnDataType.LONG_ARRAY);
    assertEquals(function("ARRAY_AGG(value, false)", DataType.INT, false).getFinalResultColumnType(),
        ColumnDataType.INT_ARRAY);
    for (String expression : new String[]{"ARRAY_AGG(value, 1)", "ARRAY_AGG(value, null)", "ARRAY_AGG(value, value)"}) {
      assertThrows(RuntimeException.class, () -> AggregationFunctionFactory.getAggregationFunction(
          bind(expression, DataType.INT, false), true));
    }
  }

  private static AggregationFunction function(String expression, DataType type, boolean multiValue) {
    FunctionContext context = bind(expression, type, multiValue);
    assertNotNull(context.getAggregationBinding());
    return AggregationFunctionFactory.getAggregationFunction(context, true);
  }

  private static FunctionContext bind(String expression, DataType type, boolean multiValue) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    if (multiValue) {
      builder.addMultiValueDimension("value", type);
    } else {
      builder.addSingleValueDimension("value", type);
    }
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT " + expression + " FROM testTable");
    AggregationFunctionBinder.bind(query, builder.build());
    return RequestContextUtils.getFunction(query.getSelectList().get(0).getFunctionCall());
  }

  private static Object roundTrip(AggregationFunction function, Object state) {
    SerializedIntermediateResult serialized = function.serializeIntermediateResult(state);
    return function.deserializeIntermediateResult(new CustomObject(serialized.getType(),
        ByteBuffer.wrap(serialized.getBytes())));
  }

  private static Object format(ColumnDataType type, Object value) {
    return type.format(type.convert(value));
  }

  private static byte[] uuid(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return bytes;
  }
}
