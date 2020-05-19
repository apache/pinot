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

import com.google.common.collect.Lists;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InbuiltFunctionEvaluatorTest {

  @Test
  public void testExpressionWithColumn()
      throws Exception {
    MyFunc myFunc = new MyFunc();
    FunctionRegistry.registerFunction(myFunc.getClass().getDeclaredMethod("reverseString", String.class));
    FunctionInfo functionInfo = FunctionRegistry
        .getFunctionByNameWithApplicableArgumentTypes("reverseString", new Class<?>[]{Object.class});
    System.out.println(functionInfo);
    String expression = "reverseString(testColumn)";

    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Assert.assertEquals(evaluator.getArguments(), Lists.newArrayList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putField("testColumn", value);
      Object result = evaluator.evaluate(row);
      Assert.assertEquals(result, new StringBuilder(value).reverse().toString());
    }
  }

  @Test
  public void testExpressionWithConstant()
      throws Exception {
    MyFunc myFunc = new MyFunc();
    FunctionRegistry
        .registerFunction(myFunc.getClass().getDeclaredMethod("daysSinceEpoch", String.class, String.class));
    String input = "1980-01-01";
    String format = "yyyy-MM-dd";
    String expression = String.format("daysSinceEpoch('%s', '%s')", input, format);
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Assert.assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    Object result = evaluator.evaluate(row);
    Assert.assertEquals(result, myFunc.daysSinceEpoch(input, format));
  }

  @Test
  public void testMultiFunctionExpression()
      throws Exception {
    MyFunc myFunc = new MyFunc();
    FunctionRegistry.registerFunction(myFunc.getClass().getDeclaredMethod("reverseString", String.class));
    FunctionRegistry
        .registerFunction(myFunc.getClass().getDeclaredMethod("daysSinceEpoch", String.class, String.class));
    String input = "1980-01-01";
    String reversedInput = myFunc.reverseString(input);
    String format = "yyyy-MM-dd";
    String expression = String.format("daysSinceEpoch(reverseString('%s'), '%s')", reversedInput, format);
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Assert.assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    Object result = evaluator.evaluate(row);
    Assert.assertEquals(result, myFunc.daysSinceEpoch(input, format));
  }

  @Test
  public void testStateSharedBetweenRowsForExecution()
      throws Exception {
    MyFunc myFunc = new MyFunc();
    FunctionRegistry
        .registerFunction(myFunc.getClass().getDeclaredMethod("appendToStringAndReturn", String.class));
    String expression = String.format("appendToStringAndReturn('%s')", "test ");
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Assert.assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    Assert.assertEquals(evaluator.evaluate(row), "test ");
    Assert.assertEquals(evaluator.evaluate(row), "test test ");
    Assert.assertEquals(evaluator.evaluate(row), "test test test ");
  }
}

class MyFunc {
  String reverseString(String input) {
    return new StringBuilder(input).reverse().toString();
  }

  MutableDateTime EPOCH_START = new MutableDateTime();

  public MyFunc() {
    EPOCH_START.setDate(0L);
  }

  int daysSinceEpoch(String input, String format) {
    DateTime dateTime = DateTimeFormat.forPattern(format).parseDateTime(input);
    return Days.daysBetween(EPOCH_START, dateTime).getDays();
  }

  private String baseString = "";

  String appendToStringAndReturn(String addedString) {
    baseString += addedString;
    return baseString;
  }
}
