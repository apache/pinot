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

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DivisionTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testDivisionTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("div(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), DivisionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = (double) _intSVValues[i] / (double) _longSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("div(%s,%s)", LONG_SV_COLUMN, FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = (double) _longSVValues[i] / Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString());
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("div(%s,%s)", FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) / _doubleSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("div(%s,%s)", DOUBLE_SV_COLUMN, STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _doubleSVValues[i] / Double.parseDouble(_stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("div(%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.parseDouble(_stringSVValues[i]) / (double) _intSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("div(div(div(div(div(12,%s),%s),div(div(%s,%s),0.34)),%s),%s)", STRING_SV_COLUMN,
            DOUBLE_SV_COLUMN, FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = (((((12d / Double.parseDouble(_stringSVValues[i])) / _doubleSVValues[i]) / (
          (Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) / (double) _longSVValues[i]) / 0.34))
          / (double) _intSVValues[i]) / _doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpression(String.format("div(%s,%s)", DOUBLE_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] =
          BigDecimal.valueOf(_doubleSVValues[i]).divide(_bigDecimalSVValues[i], RoundingMode.HALF_EVEN);
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(
        String.format("div(div(div('3430.34473923874923809',div(%s,0.34)),%s),%s)", STRING_SV_COLUMN,
            BIG_DECIMAL_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    BigDecimal constant1 = BigDecimal.valueOf(0.34d);
    BigDecimal constant2 = new BigDecimal("3430.34473923874923809");
    for (int i = 0; i < NUM_ROWS; i++) {
      BigDecimal val1 = new BigDecimal(_stringSVValues[i]).divide(constant1, RoundingMode.HALF_EVEN);
      BigDecimal val2 = constant2.divide(val1, RoundingMode.HALF_EVEN);
      BigDecimal val3 = val2.divide(_bigDecimalSVValues[i], RoundingMode.HALF_EVEN);
      expectedBigDecimalValues[i] = val3.divide(BigDecimal.valueOf(_intSVValues[i]), RoundingMode.HALF_EVEN);
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("div(%s)", INT_SV_COLUMN)}, new Object[]{
        String.format("div(%s, %s)", INT_MV_COLUMN, LONG_SV_COLUMN)
    }, new Object[]{
        String.format("div(%s, %s)", LONG_SV_COLUMN, INT_MV_COLUMN)
    }
    };
  }

  @Test
  public void testDivisionNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("div(%s,null)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), DivisionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intSVValues[i] / Integer.MIN_VALUE;
    }
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testDivisionNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("div(%s,%s)", INT_SV_COLUMN, INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof DivisionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), DivisionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = 1;
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }
}
