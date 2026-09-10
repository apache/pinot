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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxObject;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Covers bound ExprMinMax schemas across older numeric segments and projection null bitmaps.
public class ExprMinMaxBoundInputTest {
  private static final ExpressionContext KEY = ExpressionContext.forIdentifier("key");
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");

  @Test
  public void testBoundLongUsesPhysicalConversionGetters()
      throws Exception {
    ParentExprMinMaxAggregationFunction function = function(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, Map.of(KEY, physicalInts(null, 2, 1), VALUE, physicalInts(null, 20, 10)));
    ExprMinMaxObject result = function.extractAggregationResult(holder);
    assertEquals(result.getExtremumKey()[0], Long.valueOf(1));
    assertEquals(result.getField(0, 0), Long.valueOf(10));
    assertEquals(result.getSchema().getColumnDataType(0), ColumnDataType.LONG);
    ExprMinMaxObject serialized = ExprMinMaxObject.fromBytes(result.toBytes());
    assertEquals(serialized.getExtremumKey()[0], Long.valueOf(1));
    assertEquals(serialized.getField(0, 0), Long.valueOf(10));

    // The same bound wrappers must also read later segments already stored as LONG.
    function.aggregate(1, holder, Map.of(KEY, SyntheticBlockValSets.Long.create(null, new long[]{0}),
        VALUE, SyntheticBlockValSets.Long.create(null, new long[]{30})));
    result = function.extractAggregationResult(holder);
    assertEquals(result.getExtremumKey()[0], Long.valueOf(0));
    assertEquals(result.getField(0, 0), Long.valueOf(30));
  }

  @Test
  public void testBoundProjectionNullSurvivesAccumulationAndSerialization()
      throws Exception {
    ParentExprMinMaxAggregationFunction function = function(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(2, holder, Map.of(KEY, physicalInts(null, 1, 2),
        VALUE, physicalInts(RoaringBitmap.bitmapOf(0), 10, 20)));
    ExprMinMaxObject result = function.extractAggregationResult(holder);
    assertEquals(result.getNumberOfRows(), 1);
    assertNull(result.getField(0, 0));
    assertNull(ExprMinMaxObject.fromBytes(result.toBytes()).getField(0, 0));

    // A subsequent block has its own null bitmap; the previous block's bit zero must not carry over.
    function.aggregate(1, holder, Map.of(KEY, physicalInts(null, 0), VALUE, physicalInts(null, 30)));
    assertEquals(function.extractAggregationResult(holder).getField(0, 0), Long.valueOf(30));
  }

  @Test
  public void testDisabledNullHandlingKeepsStoredProjection() {
    ParentExprMinMaxAggregationFunction function = function(false);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(1, holder, Map.of(KEY, physicalInts(null, 1),
        VALUE, physicalInts(RoaringBitmap.bitmapOf(0), 10)));
    assertEquals(function.extractAggregationResult(holder).getField(0, 0), Long.valueOf(10));
  }

  private static ParentExprMinMaxAggregationFunction function(boolean nullHandlingEnabled) {
    List<ExpressionContext> arguments = List.of(ExpressionContext.forLiteral(Literal.intValue(0)),
        ExpressionContext.forLiteral(Literal.intValue(1)), KEY, VALUE);
    AggregateCallBinding binding = new AggregateCallBinding(List.of(
        ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.LONG), ColumnDataType.OBJECT);
    return new ParentExprMinMaxAggregationFunction(arguments, false, nullHandlingEnabled, binding);
  }

  private static BlockValSet physicalInts(RoaringBitmap nulls, int... values) {
    BlockValSet block = mock(BlockValSet.class);
    when(block.getValueType()).thenReturn(DataType.INT);
    when(block.isSingleValue()).thenReturn(true);
    when(block.getNullBitmap()).thenReturn(nulls);
    when(block.getIntValuesSV()).thenReturn(values);
    when(block.getLongValuesSV()).thenReturn(Arrays.stream(values).asLongStream().toArray());
    return block;
  }
}
