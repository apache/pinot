/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.function;

import com.linkedin.pinot.core.data.GenericRow;
import java.lang.reflect.Method;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.collection.mutable.StringBuilder;


public class FunctionExpressionEvaluatorTest {

  @Test
  public void testExpressionWithColumn() throws Exception {
    Method method = MyFunc.class.getDeclaredMethod("reverseString", String.class);
    FunctionRegistry.registerStaticFunction(method);
    FunctionInfo functionInfo = FunctionRegistry.resolve("reverseString", new Class<?>[]{Object.class});
    System.out.println(functionInfo);
    String expression = "reverseString(testColumn)";

    FunctionExpressionEvaluator evaluator = new FunctionExpressionEvaluator(expression);
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putField("testColumn", value);
      Object result = evaluator.evaluate(row);
      Assert.assertEquals(result, new StringBuilder(value).reverse().toString());
    }
  }

  @Test
  public void testExpressionWithConstant() throws Exception {
    FunctionRegistry.registerStaticFunction(
        MyFunc.class.getDeclaredMethod("daysSinceEpoch", String.class, String.class));
    String input = "1980-01-01";
    String format = "yyyy-MM-dd";
    String expression = String.format("daysSinceEpoch('%s', '%s')", input, format);
    FunctionExpressionEvaluator evaluator = new FunctionExpressionEvaluator(expression);
    GenericRow row = new GenericRow();
    Object result = evaluator.evaluate(row);
    Assert.assertEquals(result, MyFunc.daysSinceEpoch(input, format));
  }

  @Test
  public void testMultiFunctionExpression() throws Exception {
    FunctionRegistry.registerStaticFunction(MyFunc.class.getDeclaredMethod("reverseString", String.class));
    FunctionRegistry.registerStaticFunction(
        MyFunc.class.getDeclaredMethod("daysSinceEpoch", String.class, String.class));
    String input = "1980-01-01";
    String reversedInput = MyFunc.reverseString(input);
    String format = "yyyy-MM-dd";
    String expression = String.format("daysSinceEpoch(reverseString('%s'), '%s')", reversedInput, format);
    FunctionExpressionEvaluator evaluator = new FunctionExpressionEvaluator(expression);
    GenericRow row = new GenericRow();
    Object result = evaluator.evaluate(row);
    Assert.assertEquals(result, MyFunc.daysSinceEpoch(input, format));
  }

  private static class MyFunc {
    static String reverseString(String input) {
      return new StringBuilder(input).reverse().toString();
    }

    static MutableDateTime EPOCH_START = new MutableDateTime();

    static {
      EPOCH_START.setDate(0L); // Set to Epoch time
    }

    static int daysSinceEpoch(String input, String format) {
      DateTime dateTime = DateTimeFormat.forPattern(format).parseDateTime(input);
      return Days.daysBetween(EPOCH_START, dateTime).getDays();
    }
  }
}
