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

import java.util.List;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver.TypeInferenceUnavailableException;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.ExprMinMaxRewriter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;


/// Exercises generic overload selection without resolving expressions in legacy explicitly typed calls.
public class AdditionalPolymorphicBindingTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension("value", DataType.TIMESTAMP)
      .addSingleValueDimension("flag", DataType.BOOLEAN)
      .addMultiValueDimension("mv", DataType.LONG)
      .build();

  @Test
  public void testInferredArrayAndScalarTypes() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT ANY_VALUE(value), ARRAY_AGG(value), ARRAY_AGG(flag, true), ARRAY_AGG(mv) FROM events");
    AggregationFunctionBinder.bind(query, SCHEMA);
    assertEquals(query.getSelectList().stream().map(expression ->
        RequestContextUtils.getExpression(expression).getFunction().getAggregationBinding().getResultType()).toList(),
        List.of(ColumnDataType.TIMESTAMP, ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.BOOLEAN_ARRAY,
            ColumnDataType.LONG_ARRAY));
  }

  @Test
  public void testLegacyArrayTypeSkipsSchemaResolution() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT ARRAY_AGG(notRegistered(missing), 'STRING'), ARRAY_AGG(value, 'LONG', true) FROM events");
    PinotQuery original = query.deepCopy();
    AggregationFunctionBinder.bind(query, SCHEMA);
    assertEquals(query, original);
    assertNull(query.getSelectList().get(0).getFunctionCall().getAggregationBinding());
  }

  @Test
  public void testInvalidInferredDistinctOption() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT ARRAY_AGG(value, flag) FROM events");
    expectThrows(IllegalArgumentException.class, () -> AggregationFunctionBinder.bind(query, SCHEMA));
  }

  @Test
  public void testLegacyNativeInputsRemainUnbound() {
    for (String input : List.of("GROOVY('{\"returnType\":\"INT\",\"isSingleValue\":true}', '1')",
        "LOOKUP('dimension', 'amount', 'id', value)")) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery(
          "SELECT MODE(" + input + "), ANY_VALUE(" + input + ") FROM events");
      PinotQuery original = query.deepCopy();
      AggregationFunctionBinder.bind(query, SCHEMA);
      assertEquals(query, original);
    }
  }

  @Test
  public void testLegacyExprNativeInputsRemainUnbound() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT EXPR_MIN(LOOKUP('dimension', 'amount', 'id', value), value) FROM events");
    new ExprMinMaxRewriter().rewrite(query);
    PinotQuery original = query.deepCopy();
    AggregationFunctionBinder.bind(query, SCHEMA);
    assertEquals(query, original);
  }

  @Test
  public void testNewInferredOverloadsRequireNativeMetadata() {
    String input = "LOOKUP('dimension', 'amount', 'id', value)";
    for (String expression : List.of("FIRST_WITH_TIME(" + input + ", value)", "ARRAY_AGG(" + input + ")")) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT " + expression + " FROM events");
      expectThrows(TypeInferenceUnavailableException.class, () -> AggregationFunctionBinder.bind(query, SCHEMA));
    }
  }

  @Test
  public void testInvalidExpressionsDoNotUseLegacyFallback() {
    for (String input : List.of("notRegistered(value)", "LOOKUP('dimension', 'amount', 'id', missing)")) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT ANY_VALUE(" + input + ") FROM events");
      IllegalArgumentException error =
          expectThrows(IllegalArgumentException.class, () -> AggregationFunctionBinder.bind(query, SCHEMA));
      assertEquals(error.getClass(), IllegalArgumentException.class);
    }
  }
}
