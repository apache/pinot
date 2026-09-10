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
package org.apache.pinot.core.query.aggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Verifies schema inference against independently constructed execution transforms without evaluating any rows.
public class ExpressionTypeResolverTest {
  static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension("intValue", DataType.INT)
      .addSingleValueDimension("longValue", DataType.LONG)
      .addSingleValueDimension("floatValue", DataType.FLOAT)
      .addSingleValueDimension("doubleValue", DataType.DOUBLE)
      .addSingleValueDimension("decimalValue", DataType.BIG_DECIMAL)
      .addSingleValueDimension("stringValue", DataType.STRING)
      .addSingleValueDimension("flag", DataType.BOOLEAN)
      .addSingleValueDimension("timestampValue", DataType.TIMESTAMP)
      .addMultiValueDimension("mvValues", DataType.INT)
      .build();

  @DataProvider
  public Object[][] expressionTypes() {
    return new Object[][]{
        {"intValue", ColumnDataType.INT},
        {"flag", ColumnDataType.BOOLEAN},
        {"timestampValue", ColumnDataType.TIMESTAMP},
        {"mvValues", ColumnDataType.INT_ARRAY},
        {"'literal'", ColumnDataType.STRING},
        {"true", ColumnDataType.BOOLEAN},
        {"CAST(stringValue AS TIMESTAMP)", ColumnDataType.TIMESTAMP},
        {"CAST(intValue AS BOOLEAN)", ColumnDataType.BOOLEAN},
        {"CAST(mvValues AS BIGINT)", ColumnDataType.LONG_ARRAY},
        {"CASE WHEN flag THEN intValue ELSE longValue END", ColumnDataType.LONG},
        {"CASE WHEN flag THEN floatValue ELSE longValue END", ColumnDataType.FLOAT},
        {"CASE WHEN flag THEN timestampValue ELSE '2026-01-01 00:00:00' END", ColumnDataType.TIMESTAMP},
        {"CASE WHEN flag THEN '1' ELSE intValue END", ColumnDataType.INT},
        {"CASE WHEN flag THEN NULL ELSE stringValue END", ColumnDataType.STRING},
        {"CASE WHEN flag THEN NULL ELSE NULL END", ColumnDataType.STRING},
        {"coalesce(intValue, longValue)", ColumnDataType.LONG},
        {"coalesce(stringValue, 'fallback')", ColumnDataType.STRING},
        {"add(intValue, longValue)", ColumnDataType.DOUBLE},
        {"add(decimalValue, longValue)", ColumnDataType.BIG_DECIMAL},
        {"abs(intValue)", ColumnDataType.DOUBLE},
        {"negate(intValue)", ColumnDataType.INT},
        {"negate(longValue)", ColumnDataType.LONG},
        {"trim(stringValue)", ColumnDataType.STRING},
        {"arrayMin(mvValues)", ColumnDataType.INT},
        {"arrayLength(mvValues)", ColumnDataType.INT},
        {"least(floatValue, longValue)", ColumnDataType.DOUBLE},
        {"least(timestampValue, timestampValue)", ColumnDataType.TIMESTAMP},
        {"jsonExtractScalar(stringValue, '$.value', 'TIMESTAMP')", ColumnDataType.TIMESTAMP},
        {"jsonExtractScalar(stringValue, '$.values', 'BOOLEAN_ARRAY')", ColumnDataType.BOOLEAN_ARRAY},
        {"dateTimeConvert(longValue, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '1:SECONDS')", ColumnDataType.LONG},
        {"year(timestampValue)", ColumnDataType.INT},
        {"monthOfYear(timestampValue)", ColumnDataType.INT},
        {"dayOfMonth(timestampValue)", ColumnDataType.INT},
        {"hour(timestampValue)", ColumnDataType.INT},
        {"millisecond(timestampValue)", ColumnDataType.INT}
    };
  }

  @Test(dataProvider = "expressionTypes")
  public void testMatchesExecutionType(String text, ColumnDataType expected) {
    ExpressionContext expression = RequestContextUtils.getExpression(text);
    assertEquals(new ExpressionTypeResolver(SCHEMA).resolve(expression), expected);
    Map<String, DataSource> sources = new HashMap<>();
    for (FieldSpec field : SCHEMA.getAllFieldSpecs()) {
      DataSource source = mock(DataSource.class);
      DataSourceMetadata metadata = mock(DataSourceMetadata.class);
      when(source.getDataSourceMetadata()).thenReturn(metadata);
      when(metadata.getDataType()).thenReturn(field.getDataType());
      when(metadata.isSingleValue()).thenReturn(field.isSingleValueField());
      sources.put(field.getName(), source);
    }
    TransformResultMetadata actual =
        TransformFunctionFactory.getNullHandlingEnabled(expression, sources).getResultMetadata();
    assertEquals(ColumnDataType.fromDataType(actual.getDataType(), actual.isSingleValue()), expected);
  }

  @DataProvider
  public Object[][] unsupportedExpressions() {
    return new Object[][]{
        {"missingColumn"},
        {"notRegistered(stringValue)"},
        {"negate(intValue, longValue)"},
        {"CASE WHEN flag THEN stringValue ELSE intValue END"},
        {"CASE WHEN flag THEN mvValues ELSE intValue END"},
        {"CASE WHEN flag THEN 'not-a-number' ELSE intValue END"},
        {"jsonExtractScalar(stringValue, '$.value', 'INVALID')"}
    };
  }

  @Test(dataProvider = "unsupportedExpressions")
  public void testUnsupportedExpressionFails(String expression) {
    expectThrows(IllegalArgumentException.class,
        () -> new ExpressionTypeResolver(SCHEMA).resolve(RequestContextUtils.getExpression(expression)));
  }

  @Test
  public void testDirectExpressionRejectsNonLiteralTypeArgument() {
    ExpressionContext expression = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM,
        "jsonExtractScalar", List.of(ExpressionContext.forIdentifier("stringValue"),
        RequestContextUtils.getExpression("'$.value'"), ExpressionContext.forIdentifier("stringValue"))));
    expectThrows(IllegalArgumentException.class, () -> new ExpressionTypeResolver(SCHEMA).resolve(expression));
  }
}
