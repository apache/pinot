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
import org.apache.pinot.common.request.context.RequestContextConvertUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * BinaryOperatorTransformFunctionTest abstracts common test methods for EqualsTransformFunctionTest,
 * NotEqualsTransformFunctionTest, GreaterThanOrEqualTransformFunctionTest, GreaterThanTransformFunctionTest,
 * LessThanOrEqualTransformFunctionTest, LessThanTransformFunctionTest
 *
 */
public abstract class BinaryOperatorTransformFunctionTest extends BaseTransformFunctionTest {

  abstract int getExpectedValue(int value, int toCompare);

  abstract int getExpectedValue(long value, long toCompare);

  abstract int getExpectedValue(float value, float toCompare);

  abstract int getExpectedValue(double value, double toCompare);

  abstract int getExpectedValue(String value, String toCompare);

  abstract String getFuncName();

  @Test
  public void testBinaryOperatorTransformFunction() {
    ExpressionContext expression = RequestContextConvertUtils
        .getExpression(String.format("%s(%s, %d)", getFuncName(), INT_SV_COLUMN, _intSVValues[0]));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), getFuncName().toLowerCase());
    int[] expectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntValues[i] = getExpectedValue(_intSVValues[i], _intSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedIntValues);

    expression = RequestContextConvertUtils
        .getExpression(String.format("%s(%s, %d)", getFuncName(), LONG_SV_COLUMN, _longSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedLongValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedLongValues[i] = getExpectedValue(_longSVValues[i], _longSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedLongValues);

    expression = RequestContextConvertUtils
        .getExpression(String.format("%s(%s, %f)", getFuncName(), FLOAT_SV_COLUMN, _floatSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedFloatValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedFloatValues[i] = getExpectedValue(_floatSVValues[i], _floatSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedFloatValues);

    expression = RequestContextConvertUtils
        .getExpression(String.format("%s(%s, %.20f)", getFuncName(), DOUBLE_SV_COLUMN, _doubleSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedDoubleValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleValues[i] = getExpectedValue(_doubleSVValues[i], _doubleSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedDoubleValues);

    expression = RequestContextConvertUtils
        .getExpression(String.format("%s(%s, '%s')", getFuncName(), STRING_SV_COLUMN, _stringSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedStringValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = getExpectedValue(_stringSVValues[i], _stringSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedStringValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextConvertUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{new Object[]{String.format("%s(%s)", getFuncName(),
        INT_SV_COLUMN)}, new Object[]{String.format("%s(%s, %s, %s)", getFuncName(), LONG_SV_COLUMN, INT_SV_COLUMN,
        STRING_SV_COLUMN)}};
  }
}
