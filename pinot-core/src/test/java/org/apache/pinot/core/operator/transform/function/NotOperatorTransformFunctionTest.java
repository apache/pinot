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
package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NotOperatorTransformFunctionTest extends BaseTransformFunctionTest {
  // Test not true returns false.
  @Test
  public void testNotTrueOperatorTransformFunction() {
    ExpressionContext expr =
        RequestContextUtils.getExpression(String.format("Not (%d = %d)", _intSVValues[0], _intSVValues[0]));
    TransformFunction func = TransformFunctionFactory.get(expr, _dataSourceMap);
    Assert.assertEquals(func.getName(), "not");
    int[] notTrueExpectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      notTrueExpectedIntValues[i] = 0;
    }
    testTransformFunction(func, notTrueExpectedIntValues);
  }

  // Test not false returns true.
  @Test
  public void testNotFalseOperatorTransformFunction() {
    ExpressionContext expr =
        RequestContextUtils.getExpression(String.format("Not (%d != %d)", _intSVValues[0], _intSVValues[0]));
    TransformFunction func = TransformFunctionFactory.get(expr, _dataSourceMap);
    Assert.assertEquals(func.getName(), "not");
    int[] notTrueExpectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      notTrueExpectedIntValues[i] = 1;
    }
    testTransformFunction(func, notTrueExpectedIntValues);
  }

  // Test that not operator also works for not literal
  @Test
  public void testNonLiteralSupport() {
    ExpressionContext expr =
        RequestContextUtils.getExpression(String.format("Not (%s != %d)", INT_SV_COLUMN, _intSVValues[0]));
    TransformFunction func = TransformFunctionFactory.get(expr, _dataSourceMap);
    Assert.assertEquals(func.getName(), "not");
    int[] expectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (_intSVValues[i] == _intSVValues[0]) {
        expectedIntValues[i] = 1;
      } else {
        expectedIntValues[i] = 0;
      }
    }
    testTransformFunction(func, expectedIntValues);
  }

  // Test illegal arguments for not transform.
  @Test
  public void testIllegalNotOperatorTransformFunction() {
    // Wrong argument type.
    ExpressionContext exprWrongType = RequestContextUtils.getExpression(String.format("Not(%s)", "test"));
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(exprWrongType, _dataSourceMap);
    });
    // Wrong number of arguments.
    ExpressionContext exprNumArg =
        RequestContextUtils.getExpression(String.format("Not(%d, %d)", _intSVValues[0], _intSVValues[1]));
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(exprNumArg, _dataSourceMap);
    });
  }
}
