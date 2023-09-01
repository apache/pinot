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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultiplicationTransformFunctionTest extends BaseTransformFunctionTest {
  @Test
  public void testMultiplicationTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("mult(%s,%s,%s,%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN, FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN,
            STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    Assert.assertEquals(transformFunction.getName(), MultiplicationTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] =
          (double) _intSVValues[i] * (double) _longSVValues[i]
              * Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) * _doubleSVValues[i]
              * Double.parseDouble(_stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("mult(%s,%s,%s)", INT_SV_COLUMN, FLOAT_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    Assert.assertEquals(transformFunction.getName(), MultiplicationTransformFunction.FUNCTION_NAME);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = BigDecimal.valueOf(_intSVValues[i]).multiply(BigDecimal.valueOf(_floatSVValues[i]))
          .multiply(_bigDecimalSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(
        String.format("mult(mult(%s,%s,%s),%s,%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN, FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN,
            STRING_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    Assert.assertEquals(transformFunction.getName(), MultiplicationTransformFunction.FUNCTION_NAME);
    expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] =
          BigDecimal.valueOf((double) _intSVValues[i] * (double) _longSVValues[i]
                  * Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()))
              .multiply(BigDecimal.valueOf(_doubleSVValues[i])).multiply(new BigDecimal(_stringSVValues[i]))
              .multiply(_bigDecimalSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(
        String.format("mult(mult(12,%s),%s,mult(mult(%s,%s),0.34,%s),%s)", STRING_SV_COLUMN, DOUBLE_SV_COLUMN,
            FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = ((12d * Double.parseDouble(_stringSVValues[i])) * _doubleSVValues[i] * (
          (Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) * (double) _longSVValues[i]) * 0.34
              * (double) _intSVValues[i]) * _doubleSVValues[i]);
    }
  }

  @Test
  public void testMultiplicationNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("mult(null, 0)"));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "mult");
    double[] expectedValues = new double[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testModuloNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("mult(%s, %s)", INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof MultiplicationTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "mult");
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = (double) _intSVValues[i] * (double) _intSVValues[i];
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("mult(%s)", INT_SV_COLUMN)}, new Object[]{
        String.format("mult(%s, %s)", LONG_SV_COLUMN, INT_MV_COLUMN)
    }
    };
  }
}
