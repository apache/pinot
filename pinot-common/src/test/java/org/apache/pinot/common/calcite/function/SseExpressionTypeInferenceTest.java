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
package org.apache.pinot.common.calcite.function;

import java.math.BigDecimal;
import java.util.List;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class SseExpressionTypeInferenceTest {

  private static Schema buildTestSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svLong", FieldSpec.DataType.LONG)
        .addSingleValueDimension("svFloat", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("svDouble", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("svBigDecimal", FieldSpec.DataType.BIG_DECIMAL)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svBoolean", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("svTimestamp", FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension("svBytes", FieldSpec.DataType.BYTES)
        .addMultiValueDimension("mvInt", FieldSpec.DataType.INT)
        .addMultiValueDimension("mvLong", FieldSpec.DataType.LONG)
        .addMultiValueDimension("mvFloat", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("mvDouble", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("mvString", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvBoolean", FieldSpec.DataType.BOOLEAN)
        .addMultiValueDimension("mvTimestamp", FieldSpec.DataType.TIMESTAMP)
        .addMultiValueDimension("mvBytes", FieldSpec.DataType.BYTES)
        .build();
  }

  @Test
  public void testInferExpressionTypeIdentifiers() {
    Schema schema = buildTestSchema();
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svInt"), schema),
        DataSchema.ColumnDataType.INT);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svLong"), schema),
        DataSchema.ColumnDataType.LONG);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svFloat"), schema),
        DataSchema.ColumnDataType.FLOAT);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svDouble"), schema),
        DataSchema.ColumnDataType.DOUBLE);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svBigDecimal"), schema),
        DataSchema.ColumnDataType.BIG_DECIMAL);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svString"), schema),
        DataSchema.ColumnDataType.STRING);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svBoolean"), schema),
        DataSchema.ColumnDataType.BOOLEAN);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svTimestamp"), schema),
        DataSchema.ColumnDataType.TIMESTAMP);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("svBytes"), schema),
        DataSchema.ColumnDataType.BYTES);

    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvInt"), schema),
        DataSchema.ColumnDataType.INT_ARRAY);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvLong"), schema),
        DataSchema.ColumnDataType.LONG_ARRAY);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvFloat"), schema),
        DataSchema.ColumnDataType.FLOAT_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvDouble"), schema),
        DataSchema.ColumnDataType.DOUBLE_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvString"), schema),
        DataSchema.ColumnDataType.STRING_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvBoolean"), schema),
        DataSchema.ColumnDataType.BOOLEAN_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvTimestamp"), schema),
        DataSchema.ColumnDataType.TIMESTAMP_ARRAY);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("mvBytes"), schema),
        DataSchema.ColumnDataType.BYTES_ARRAY);

    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getIdentifierExpression("unknownCol"), schema),
        DataSchema.ColumnDataType.UNKNOWN);
  }

  @Test
  public void testInferExpressionTypeLiterals() {
    Schema schema = buildTestSchema();
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(true), schema),
        DataSchema.ColumnDataType.BOOLEAN);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(1), schema),
        DataSchema.ColumnDataType.INT);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(1L), schema),
        DataSchema.ColumnDataType.LONG);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(1.0f), schema),
        DataSchema.ColumnDataType.FLOAT);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(1.0d), schema),
        DataSchema.ColumnDataType.DOUBLE);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new BigDecimal("123.45")),
            schema), DataSchema.ColumnDataType.BIG_DECIMAL);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression("abc"), schema),
        DataSchema.ColumnDataType.STRING);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new byte[]{1, 2}), schema),
        DataSchema.ColumnDataType.BYTES);

    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new int[]{1, 2}), schema),
        DataSchema.ColumnDataType.INT_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new long[]{1L, 2L}), schema),
        DataSchema.ColumnDataType.LONG_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new float[]{1.0f, 2.0f}),
            schema), DataSchema.ColumnDataType.FLOAT_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new double[]{1.0d, 2.0d}),
            schema), DataSchema.ColumnDataType.DOUBLE_ARRAY);
    assertEquals(
        SseExpressionTypeInference.inferReturnRelType(RequestUtils.getLiteralExpression(new String[]{"a", "b"}),
            schema), DataSchema.ColumnDataType.STRING_ARRAY);
  }

  @Test
  public void testInferExpressionTypeScalarFunction() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // abs(doubleCol) -> DOUBLE
    Expression absOnInt = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("abs"), RequestUtils.getIdentifierExpression("svDouble"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(absOnInt, schema), DataSchema.ColumnDataType.DOUBLE);

    // intDiv(10.0, 3.0) -> LONG
    Expression intDiv = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("intDiv"),
        RequestUtils.getLiteralExpression(10.0d),
        RequestUtils.getLiteralExpression(3.0d));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(intDiv, schema), DataSchema.ColumnDataType.LONG);

    // jsonExtractScalar(stringCol, '$.name', 'INT') -> INT
    Expression jsonExtractScalar = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("jsonExtractScalar"),
        RequestUtils.getIdentifierExpression("svString"),
        RequestUtils.getLiteralExpression("$.name"),
        RequestUtils.getLiteralExpression("INT"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(jsonExtractScalar, schema),
        DataSchema.ColumnDataType.INT);

    // jsonExtractScalar(stringCol, '$.name', 'DOUBLE', '3') -> DOUBLE
    Expression jsonExtractScalarWithDefault = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("jsonExtractScalar"),
        RequestUtils.getIdentifierExpression("svString"),
        RequestUtils.getLiteralExpression("$.name"),
        RequestUtils.getLiteralExpression("DOUBLE"),
        RequestUtils.getLiteralExpression("3"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(jsonExtractScalarWithDefault, schema),
        DataSchema.ColumnDataType.DOUBLE);
  }

  @Test
  public void testInferExpressionTypeScalarFunctionPolymorphic() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // plus(long, long) -> LONG
    Expression plusLongLong = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("plus"),
        RequestUtils.getIdentifierExpression("svLong"),
        RequestUtils.getIdentifierExpression("svLong"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(plusLongLong, schema), DataSchema.ColumnDataType.LONG);

    // plus(long, double) -> DOUBLE
    Expression plusLongDouble = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("plus"),
        RequestUtils.getIdentifierExpression("svLong"),
        RequestUtils.getIdentifierExpression("svDouble"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(plusLongDouble, schema),
        DataSchema.ColumnDataType.DOUBLE);
  }

  @Test
  public void testInferExpressionTypeNestedFunctions() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // ln(abs(3.0)) -> DOUBLE
    Expression absExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("abs"), RequestUtils.getLiteralExpression(3.0d));
    Expression lnAbs = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("ln"), absExpression);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(lnAbs, schema), DataSchema.ColumnDataType.DOUBLE);

    // reverse(lower(CAST(intCol AS STRING))) -> STRING
    Expression castToString = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("cast"),
        RequestUtils.getIdentifierExpression("svInt"),
        RequestUtils.getLiteralExpression("INT"));
    Expression trimExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("lower"), castToString);
    Expression reverseExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("reverse"), trimExpression);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(reverseExpression, schema),
        DataSchema.ColumnDataType.STRING);

    // cast(plus(intCol, 10) AS DOUBLE) -> DOUBLE
    Expression plusExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("plus"),
        RequestUtils.getIdentifierExpression("svInt"),
        RequestUtils.getLiteralExpression(10));
    Expression castAsDouble = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("cast"),
        plusExpression,
        RequestUtils.getLiteralExpression("DOUBLE"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(plusExpression, schema), DataSchema.ColumnDataType.INT);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(castAsDouble, schema),
        DataSchema.ColumnDataType.DOUBLE);
  }

  @Test
  public void testInferExpressionTypeUnknownFunction() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // Unknown function
    Expression unknownFn = RequestUtils.getFunctionExpression("notafunction",
        List.of(RequestUtils.getIdentifierExpression("svInt")));
    assertThrows(IllegalArgumentException.class,
        () -> SseExpressionTypeInference.inferReturnRelType(unknownFn, schema));
  }

  @Test
  public void testInferExpressionTypeAggregationFunction() {
    Schema schema = buildTestSchema();
    FunctionRegistry.init();

    // sum(intCol) -> LONG
    Expression sumExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("sum"),
        RequestUtils.getIdentifierExpression("svInt"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(sumExpression, schema), DataSchema.ColumnDataType.LONG);

    // min(doubleCol) -> DOUBLE
    Expression minExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("min"),
        RequestUtils.getIdentifierExpression("svDouble"));
    assertEquals(SseExpressionTypeInference.inferReturnRelType(minExpression, schema),
        DataSchema.ColumnDataType.DOUBLE);

    // max(reverse(stringCol)) -> STRING
    Expression reverseExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("reverse"),
        RequestUtils.getIdentifierExpression("svString"));
    Expression maxExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("max"),
        reverseExpression);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(maxExpression, schema),
        DataSchema.ColumnDataType.STRING);

    // reverse(max(stringCol)) -> STRING
    Expression maxStringExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("max"),
        RequestUtils.getIdentifierExpression("svString"));
    Expression reverseMaxExpression = RequestUtils.getFunctionExpression(
        FunctionRegistry.canonicalize("reverse"),
        maxStringExpression);
    assertEquals(SseExpressionTypeInference.inferReturnRelType(reverseMaxExpression, schema),
        DataSchema.ColumnDataType.STRING);
  }
}
