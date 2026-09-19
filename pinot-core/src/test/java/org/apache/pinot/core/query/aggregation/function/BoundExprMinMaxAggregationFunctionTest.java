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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxObject;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
    ExprMinMaxObject result = aggregate(function, 0);
    assertEquals(result.getNumberOfRows(), 1);
    assertEquals(result.getField(0, 0), 0);
  }

  @DataProvider
  public Object[][] projections() {
    return new Object[][]{{0}, {null}};
  }

  @Test(dataProvider = "projections")
  public void testLogicalSchemasAndTiedRowsSurviveSerializedMerge(@Nullable Integer value)
      throws Exception {
    ParentExprMinMaxAggregationFunction function = new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, true,
        BINDING);
    ExprMinMaxObject first = ExprMinMaxObject.fromBytes(aggregate(function, value).toBytes());
    assertEquals(first.getField(0, 0), value);
    ExprMinMaxObject second = ExprMinMaxObject.fromBytes(aggregate(function, 1).toBytes());
    ExprMinMaxObject merged = first.merge(second, false);
    assertEquals(merged.getNumberOfRows(), 2);
    assertEquals(merged.getSchema().getColumnDataType(0), ColumnDataType.BOOLEAN);
    assertEquals(merged.getField(0, 0), value);
    assertEquals(merged.getField(1, 0), 1);
    ExprMinMaxObject roundTrip = ExprMinMaxObject.fromBytes(merged.toBytes());
    assertEquals(roundTrip.getExtremumKey(), new Comparable[]{1_700_000_000_000L});
    assertEquals(roundTrip.getNumberOfRows(), 2);
    assertEquals(roundTrip.getField(0, 0), value);
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

  @Test
  public void testBoundLongUsesPhysicalConversionGetters()
      throws Exception {
    ParentExprMinMaxAggregationFunction function = widenedFunction(true);
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
    ParentExprMinMaxAggregationFunction function = widenedFunction(true);
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
    ParentExprMinMaxAggregationFunction function = widenedFunction(false);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(1, holder, Map.of(KEY, physicalInts(null, 1),
        VALUE, SyntheticBlockValSets.Long.create(RoaringBitmap.bitmapOf(0), new long[]{10})));
    assertEquals(function.extractAggregationResult(holder).getField(0, 0), Long.valueOf(10));
  }

  private static ParentExprMinMaxAggregationFunction widenedFunction(boolean nullHandlingEnabled) {
    AggregateCallBinding binding = new AggregateCallBinding(List.of(
        ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.LONG), ColumnDataType.OBJECT);
    return new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, nullHandlingEnabled, binding);
  }

  private static BlockValSet physicalInts(RoaringBitmap nulls, int... values) {
    Object[][] rows = Arrays.stream(values).mapToObj(value -> new Object[]{value}).toArray(Object[][]::new);
    if (nulls != null) {
      nulls.forEach((int i) -> rows[i][0] = null);
    }
    return new RowBasedBlockValSet(ColumnDataType.INT, Arrays.asList(rows), 0, true);
  }

  private static void assertInvalidBinding(AggregateCallBinding binding, String message) {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> new ParentExprMinMaxAggregationFunction(ARGUMENTS, false, true, binding));
    assertTrue(exception.getMessage().contains(message), exception.getMessage());
  }

  private static ExprMinMaxObject aggregate(ParentExprMinMaxAggregationFunction function, @Nullable Integer value) {
    BlockValSet keys = SyntheticBlockValSets.Long.create(null, new long[]{1_700_000_000_000L});
    BlockValSet values =
        new RowBasedBlockValSet(ColumnDataType.BOOLEAN, List.<Object[]>of(new Object[]{value}), 0, true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(1, holder, Map.of(KEY, keys, VALUE, values));
    return function.extractAggregationResult(holder);
  }

  private static ExpressionContext literal(int value) {
    return ExpressionContext.forLiteral(new LiteralContext(DataType.INT, value));
  }
}
