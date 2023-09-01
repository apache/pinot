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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SubtractionTransformFunctionTest extends BaseTransformFunctionTest {
  @Test
  public void testSubtractionTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("sub(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), SubtractionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = (double) _intSVValues[i] - (double) _longSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sub(%s,%s)", LONG_SV_COLUMN, FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = (double) _longSVValues[i] - Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString());
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sub(%s,%s)", FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) - _doubleSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sub(%s,%s)", DOUBLE_SV_COLUMN, STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _doubleSVValues[i] - Double.parseDouble(_stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sub(%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.parseDouble(_stringSVValues[i]) - (double) _intSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("sub(sub(sub(sub(sub(12,%s),%s),sub(sub(%s,%s),0.34)),%s),%s)", STRING_SV_COLUMN,
            DOUBLE_SV_COLUMN, FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = (((((12d - Double.parseDouble(_stringSVValues[i])) - _doubleSVValues[i]) - (
          ((Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) - (double) _longSVValues[i]) - 0.34))
          - (double) _intSVValues[i]) - _doubleSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("sub(sub(sub(sub(sub(12,%s),%s),sub(sub(%s,%s),0.34)),%s),%s)", STRING_SV_COLUMN,
            DOUBLE_SV_COLUMN, FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = (BigDecimal.valueOf(
              (((12d - Double.parseDouble(_stringSVValues[i])) - _doubleSVValues[i]) - (
                  (Double.parseDouble(Float.valueOf(_floatSVValues[i]).toString()) - (double) _longSVValues[i]) - 0.34))
                  - (double) _intSVValues[i])
          .subtract(_bigDecimalSVValues[i]));
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
        new Object[]{String.format("sub(%s)", INT_SV_COLUMN)}, new Object[]{
        String.format("sub(%s, %s)", INT_MV_COLUMN, LONG_SV_COLUMN)
    }, new Object[]{
        String.format("sub(%s, %s)", LONG_SV_COLUMN, INT_MV_COLUMN)
    }
    };
  }

  @Test
  public void testSubtractionNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression("sub(null, 1)");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.SUB.getName());
    double[] expectedValues = new double[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testSubtractionNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("sub(%s, 0)", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SubtractionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.SUB.getName());
    double[] expectedValues = new double[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = _intSVValues[i];
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }
}
