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

import java.util.Collections;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.segment.local.function.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class InbuiltFunctionEvaluatorTest {

  @Test
  public void testColumnExpression() {
    String expression = "testColumn";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), value);
    }
  }

  @Test
  public void testLiteralExpression() {
    String expression = "'testValue'";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      assertEquals(evaluator.evaluate(row), "testValue");
    }
  }

  @Test
  public void testScalarWrapperNameWithOverrides() {
    String expr = String.format("regexp_extract(testColumn, '%s')", "(.*)([\\d]+)");
    String exprWithOccurrence = String.format("regexp_extract(testColumn, '%s', 3)", "(.*)([\\d]+)");
    String exprWithOccurrenceAndDefault = String.format("regexp_extract(testColumn, '%s', 4, 'null')", "(.*)([\\d]+)");
    GenericRow row = new GenericRow();
    row.putValue("testColumn", "testValue0");
    InbuiltFunctionEvaluator evaluator;
    evaluator = new InbuiltFunctionEvaluator(expr);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    assertEquals(evaluator.evaluate(row), "testValue0");
    evaluator = new InbuiltFunctionEvaluator(exprWithOccurrence);
    assertEquals(evaluator.evaluate(row), "0");
    evaluator = new InbuiltFunctionEvaluator(exprWithOccurrenceAndDefault);
    assertEquals(evaluator.evaluate(row), "null");
  }

  @Test
  public void testFunctionWithColumn() {
    String expression = "reverse(testColumn)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), new StringBuilder(value).reverse().toString());
    }
  }

  @Test
  public void testFunctionWithLiteral() {
    String expression = "reverse(12345)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    assertEquals(evaluator.evaluate(row), "54321");
  }

  @Test
  public void testNestedFunction() {
    String expression = "reverse(reverse(testColumn))";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), value);
    }
  }

  @Test
  public void testStateSharedBetweenRowsForExecution()
      throws Exception {
    MyFunc myFunc = new MyFunc();
    FunctionRegistry.registerFunction(myFunc.getClass().getDeclaredMethod("appendToStringAndReturn", String.class));
    String expression = "appendToStringAndReturn('test ')";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    assertEquals(evaluator.evaluate(row), "test ");
    assertEquals(evaluator.evaluate(row), "test test ");
    assertEquals(evaluator.evaluate(row), "test test test ");
  }

  private static class MyFunc {
    String _baseString = "";

    String appendToStringAndReturn(String addedString) {
      _baseString += addedString;
      return _baseString;
    }
  }
}
