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
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * BinaryOperatorTransformFunctionTest abstracts common test methods for EqualsTransformFunctionTest,
 * NotEqualsTransformFunctionTest, GreaterThanOrEqualTransformFunctionTest, GreaterThanTransformFunctionTest,
 * LessThanOrEqualTransformFunctionTest, LessThanTransformFunctionTest
 *
 */
public abstract class BinaryOperatorTransformFunctionTest extends BaseTransformFunctionTest {

  abstract boolean getExpectedValue(int compareResult);

  abstract String getFunctionName();

  @Test
  public void testBinaryOperatorTransformFunction() {
    String functionName = getFunctionName();
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("%s(%s, %d)", functionName, INT_SV_COLUMN, _intSVValues[0]));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(transformFunction.getName(), functionName);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.BOOLEAN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    boolean[] expectedValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(Integer.compare(_intSVValues[i], _intSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, %d)", functionName, LONG_SV_COLUMN, _longSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(Long.compare(_longSVValues[i], _longSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, %f)", functionName, FLOAT_SV_COLUMN, _floatSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(Float.compare(_floatSVValues[i], _floatSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, %.20f)", functionName, DOUBLE_SV_COLUMN, _doubleSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(Double.compare(_doubleSVValues[i], _doubleSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    // Note: defining decimal literals within quotes ('%s') preserves precision.
    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, '%s')", functionName, BIG_DECIMAL_SV_COLUMN, _bigDecimalSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(_bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, '%s')", functionName, STRING_SV_COLUMN, _stringSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(_stringSVValues[i].compareTo(_stringSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    // Test with heterogeneous arguments (long on left-side, double on right-side)
    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, '%s')", functionName, LONG_SV_COLUMN, _doubleSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(Double.compare(_longSVValues[i], _doubleSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    // Test with heterogeneous arguments (double on left-side, long on right-side)
    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, '%s')", functionName, DOUBLE_SV_COLUMN, _longSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(Double.compare(_doubleSVValues[i], _longSVValues[0]));
    }
    testTransformFunction(transformFunction, expectedValues);

    // Test with null literal
    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, null)", functionName, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);

    // Test with null column.
    expression = RequestContextUtils.getExpression(
        String.format("%s(%s, %d)", functionName, INT_SV_NULL_COLUMN, _intSVValues[0]));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(transformFunction.getName(), functionName);
    resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.BOOLEAN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    expectedValues = new boolean[NUM_ROWS];
    bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = getExpectedValue(Integer.compare(_intSVValues[i], _intSVValues[0]));
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{
            String.format("%s(%s)", getFunctionName(), INT_SV_COLUMN)
        }, new Object[]{
        String.format("%s(%s, %s, %s)", getFunctionName(), LONG_SV_COLUMN, INT_SV_COLUMN, STRING_SV_COLUMN)
    }
    };
  }
}
