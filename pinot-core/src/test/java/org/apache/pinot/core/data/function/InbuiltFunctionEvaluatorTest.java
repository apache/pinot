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
package org.apache.pinot.core.data.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.segment.local.function.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class InbuiltFunctionEvaluatorTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression,
        GenericRowToFieldSchemaMap.inferFieldMap(row));
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test
  public void testColumnExpression() {
    String expression = "testColumn";
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      testFunction(expression, ImmutableList.of("testColumn"), row, value);
    }
  }

  @Test
  public void testLiteralExpression() {
    String expression = "'testValue'";
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      testFunction(expression, ImmutableList.of(), row, "testValue");
    }
  }

  @Test
  public void testScalarWrapperWithReservedKeywordExpression() {
    String expression = "dateTrunc('MONTH', \"date\")";
    GenericRow row = new GenericRow();
    for (int i = 1; i < 9; i++) {
      DateTime dt = new DateTime(String.format("2020-0%d-15T12:00:00", i));
      long millis = dt.getMillis();
      DateTime truncDt = dt.withZone(DateTimeZone.UTC).withDayOfMonth(1).withHourOfDay(0).withMillisOfDay(0);
      row.putValue("date", millis);
      testFunction(expression, ImmutableList.of("date"), row, truncDt.getMillis());
    }
  }

  @Test
  public void testScalarWrapperNameWithOverrides() {
    String expr = String.format("regexp_extract(testColumn, '%s')", "(.*)([\\d]+)");
    String exprWithGroup = String.format("regexp_extract(testColumn, '%s', 2)", "(.*)([\\d]+)");
    String exprWithGroupAndDefault = String.format("regexp_extract(testColumn, '%s', 3, 'null')", "(.*)([\\d]+)");

    GenericRow row = new GenericRow();
    row.putValue("testColumn", "testValue0");

    testFunction(expr, ImmutableList.of("testColumn"), row, "testValue0");
    testFunction(exprWithGroup, ImmutableList.of("testColumn"), row, "0");
    testFunction(exprWithGroupAndDefault, ImmutableList.of("testColumn"), row, "null");
  }

  @Test
  public void testFunctionWithColumn() {
    String expression = "reverse(testColumn)";
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      testFunction(expression, ImmutableList.of("testColumn"), row, new StringBuilder(value).reverse().toString());
    }
  }

  @Test
  public void testFunctionWithLiteral() {
    String expression = "reverse('12345')";
    GenericRow row = new GenericRow();
    testFunction(expression, ImmutableList.of(), row, "54321");
  }

  @Test
  public void testNestedFunction() {
    String expression = "reverse(reverse(testColumn))";
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      testFunction(expression, ImmutableList.of("testColumn"), row, value);
    }
  }

  @Test
  public void testStateSharedBetweenRowsForExecution()
      throws Exception {
    MyFunc myFunc = new MyFunc();
    Method method = myFunc.getClass().getDeclaredMethod("appendToStringAndReturn", String.class);
    FunctionRegistry.registerFunction(method, false, false, false);
    String expression = "appendToStringAndReturn('test ')";
    GenericRow row = new GenericRow();

    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression,
        GenericRowToFieldSchemaMap.inferFieldMap(row));
    assertTrue(evaluator.getArguments().isEmpty());
    assertEquals(evaluator.evaluate(row), "test ");
    assertEquals(evaluator.evaluate(row), "test test ");
    assertEquals(evaluator.evaluate(row), "test test test ");
  }

  @Test
  public void testNullReturnedByInbuiltFunctionEvaluatorThatCannotTakeNull() {
    String[] expressions = {
        "fromDateTime(\"NULL\", 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')",
        "toDateTime(1648010797, \"NULL\", \"NULL\")",
        "toDateTime(\"NULL\", 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', 'UTC')"
    };
    for (String expression : expressions) {
      GenericRow row = new GenericRow();
      Map<String, FieldSpec> specMap = new HashMap<>(GenericRowToFieldSchemaMap.inferFieldMap(row));
      specMap.put("NULL", new DimensionFieldSpec("invalid_identifier", FieldSpec.DataType.LONG, true));
      InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, specMap);
      assertNull(evaluator.evaluate(row));
    }
  }

  @Test
  public void shouldHandleNullInputsIfFunctionHasNullableParams() {
    // Given:
    String expression = "isNull(foo)";
    GenericRow row = new GenericRow();
    row.putValue("foo", null);
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression,
        ImmutableMap.of("foo", new DimensionFieldSpec("foo", FieldSpec.DataType.INT, true)));

    // When:
    Object result = evaluator.evaluate(row);

    // Then:
    Assert.assertEquals(result, true);
    Assert.assertEquals(evaluator.getArguments(), ImmutableList.of("foo"));
  }

  public static class MyFunc {
    String _baseString = "";

    public String appendToStringAndReturn(String addedString) {
      _baseString += addedString;
      return _baseString;
    }
  }
}
