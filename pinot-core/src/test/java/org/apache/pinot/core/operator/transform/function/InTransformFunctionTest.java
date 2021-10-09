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

import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class InTransformFunctionTest extends BaseTransformFunctionTest {
  protected static final String STRING_SV_COLUMN = "stringSV";

  @Test
  public void testIntInTransformFunction() {
    String expressionStr = String.format("%s in(1,2,9,5)", INT_SV_COLUMN);
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = 0;
      if (_intSVValues[i] == 1 || _intSVValues[i] == 2 || _intSVValues[i] == 9 || _intSVValues[i] == 5) {
        expected = 1;
      }
      assertEquals(intValues[i], expected);
    }
  }

  @Test
  public void testIntMVInTransformFunction() {
    String expressionStr = String.format("%s in(1,2,9,5)", INT_MV_COLUMN);
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = 0;
      for (int intValue : _intMVValues[i]) {
        if (intValue == 1 || intValue == 2 || intValue == 9 || intValue == 5) {
          expected = 1;
          break;
        }
      }
      assertEquals(intValues[i], expected);
    }
  }

  @Test
  public void testIntInTransformFunctionWithTransformedValues() {
    String expressionStr = String.format("%s in(1,1+1,4+5,2+3)", INT_SV_COLUMN);
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);

    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = 0;
      if (_intSVValues[i] == 1 || _intSVValues[i] == 2 || _intSVValues[i] == 9 || _intSVValues[i] == 5) {
        expected = 1;
      }
      assertEquals(intValues[i], expected);
    }
  }

  @Test
  public void testLongInTransformFunction() {
    String expressionStr =
        String.format("%s in(%d,%d, %d)", LONG_SV_COLUMN, _longSVValues[2], _longSVValues[7], _longSVValues[11]);
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = 0;
      if (i == 2 || i == 7 || i == 11) {
        expected = 1;
      }
      assertEquals(intValues[i], expected);
    }
  }

  @Test
  public void testFloatInTransformFunction() {
    String expressionStr =
        String.format("%s in(%f,%f, %f)", FLOAT_SV_COLUMN, _floatSVValues[3], _floatSVValues[7], _floatSVValues[9]);
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = 0;
      if (i == 3 || i == 7 || i == 9) {
        expected = 1;
      }
      assertEquals(intValues[i], expected);
    }
  }

  @Test
  public void testStringInTransformFunction() {
    String expressionStr =
        String.format("%s in('a','b','%s','%s')", STRING_SV_COLUMN, _stringSVValues[2], _stringSVValues[5]);
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int expected = 0;
      if (i == 2 || i == 5) {
        expected = 1;
      }
      assertEquals(intValues[i], expected);
    }
  }
}
