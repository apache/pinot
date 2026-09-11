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
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxObject;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxProjectionValSetWrapper;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Covers bound selector state that must survive empty segments, serialization, and tied-key merges.
public class BoundExprMinMaxAggregationFunctionTest {
  private static final ExpressionContext KEY = ExpressionContext.forIdentifier("key");
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");
  private static final List<ExpressionContext> ARGUMENTS = List.of(literal(0), literal(1), KEY, VALUE);
  private static final AggregateCallBinding BINDING = new AggregateCallBinding(
      List.of(ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN),
      ColumnDataType.OBJECT);

  @Test
  public void testBoundSchemaBeforeFirstRowAndAfterEmptySegment()
      throws Exception {
    ParentExprMinMaxAggregationFunction function = new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, true,
        BINDING);
    ExprMinMaxObject empty = function.extractAggregationResult(function.createAggregationResultHolder());
    assertEquals(empty.getSchema().getColumnDataType(0), ColumnDataType.BOOLEAN);
    assertEquals(ExprMinMaxObject.fromBytes(empty.toBytes()).getSchema().getColumnDataType(0), ColumnDataType.BOOLEAN);
    ExprMinMaxObject result = aggregate(function, false);
    assertEquals(result.getNumberOfRows(), 1);
    assertEquals(result.getField(0, 0), 0);
  }

  @Test
  public void testLogicalSchemasAndTiedRowsSurviveSerializedMerge()
      throws Exception {
    ParentExprMinMaxAggregationFunction function = new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, true,
        BINDING);
    ExprMinMaxObject first = ExprMinMaxObject.fromBytes(aggregate(function, false).toBytes());
    ExprMinMaxObject second = ExprMinMaxObject.fromBytes(aggregate(function, true).toBytes());
    ExprMinMaxObject merged = first.merge(second, false);
    assertEquals(merged.getNumberOfRows(), 2);
    assertEquals(merged.getSchema().getColumnDataType(0), ColumnDataType.BOOLEAN);
    assertEquals(merged.getField(0, 0), 0);
    assertEquals(merged.getField(1, 0), 1);
    ExprMinMaxObject roundTrip = ExprMinMaxObject.fromBytes(merged.toBytes());
    assertEquals(roundTrip.getExtremumKey(), new Comparable[]{1_700_000_000_000L});
    assertEquals(roundTrip.getNumberOfRows(), 2);
    assertEquals(roundTrip.getField(0, 0), 0);
    assertEquals(roundTrip.getField(1, 0), 1);
  }

  @Test
  public void testSerializedNullProjectionStaysNullAfterMerge()
      throws Exception {
    ParentExprMinMaxAggregationFunction function = new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, true,
        BINDING);
    ExprMinMaxObject result = aggregate(function, false);
    ExprMinMaxProjectionValSetWrapper nullProjection = mock(ExprMinMaxProjectionValSetWrapper.class);
    result.setToNewVal(List.of(nullProjection), 0);
    ExprMinMaxObject serialized = ExprMinMaxObject.fromBytes(result.toBytes());
    assertEquals(serialized.getSchema().getColumnDataType(0), ColumnDataType.BOOLEAN);
    assertNull(serialized.getField(0, 0));
    ExprMinMaxObject merged = serialized.merge(ExprMinMaxObject.fromBytes(aggregate(function, true).toBytes()), false);
    assertNull(merged.getField(0, 0));
    assertEquals(merged.getField(1, 0), 1);
    ExprMinMaxObject roundTrip = ExprMinMaxObject.fromBytes(merged.toBytes());
    assertNull(roundTrip.getField(0, 0));
    assertEquals(roundTrip.getField(1, 0), 1);
  }

  @Test
  public void testTypedChildHasNullFinalPlaceholder() {
    ChildExprMinMaxAggregationFunction child = new ChildExprMinMaxAggregationFunction(
        List.of(literal(0), VALUE, VALUE, KEY), false, ColumnDataType.BOOLEAN);
    assertEquals(child.getIntermediateResultColumnType(), ColumnDataType.LONG);
    assertEquals(child.getFinalResultColumnType(), ColumnDataType.BOOLEAN);
    assertNull(child.extractFinalResult(0L));
    ChildExprMinMaxAggregationFunction legacy = new ChildExprMinMaxAggregationFunction(
        List.of(literal(0), VALUE, VALUE, KEY), false);
    assertEquals(legacy.getFinalResultColumnType(), ColumnDataType.UNKNOWN);
    assertEquals(legacy.extractFinalResult(0L), Long.valueOf(0L));
  }

  @Test
  public void testInvalidBindingsFailBeforeExecution() {
    assertInvalidBinding(new AggregateCallBinding(BINDING.getArgumentTypes(), ColumnDataType.BOOLEAN),
        "result type OBJECT");
    assertInvalidBinding(new AggregateCallBinding(List.of(ColumnDataType.INT, ColumnDataType.INT),
        ColumnDataType.OBJECT), "must match all 4 arguments");
    assertInvalidBinding(new AggregateCallBinding(
        List.of(ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN),
        ColumnDataType.OBJECT), "measuring column type: TIMESTAMP_ARRAY");
    assertInvalidBinding(new AggregateCallBinding(
        List.of(ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.TIMESTAMP, ColumnDataType.OBJECT),
        ColumnDataType.OBJECT), "projection column type: OBJECT");
  }

  private static void assertInvalidBinding(AggregateCallBinding binding, String message) {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, true, binding));
    assertTrue(exception.getMessage().contains(message), exception.getMessage());
  }

  private static ExprMinMaxObject aggregate(ParentExprMinMaxAggregationFunction function, boolean value) {
    BlockValSet keys = mock(BlockValSet.class);
    when(keys.isSingleValue()).thenReturn(true);
    when(keys.getValueType()).thenReturn(DataType.TIMESTAMP);
    when(keys.getLongValuesSV()).thenReturn(new long[]{1_700_000_000_000L});
    BlockValSet values = mock(BlockValSet.class);
    when(values.isSingleValue()).thenReturn(true);
    when(values.getValueType()).thenReturn(DataType.BOOLEAN);
    when(values.getIntValuesSV()).thenReturn(new int[]{value ? 1 : 0});
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(1, holder, Map.of(KEY, keys, VALUE, values));
    return function.extractAggregationResult(holder);
  }

  private static ExpressionContext literal(int value) {
    return ExpressionContext.forLiteral(new LiteralContext(DataType.INT, value));
  }
}
