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


public class AdditionTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testAdditionTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("add(%s,%s,%s,%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN, FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN,
            STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), AdditionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] =
          (double) _intSVValues[i] + (double) _longSVValues[i] + (double) _floatSVValues[i] + _doubleSVValues[i]
              + Double.parseDouble(_stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("add(add(12,%s),%s,add(add(%s,%s),0.34,%s),%s)", STRING_SV_COLUMN, DOUBLE_SV_COLUMN,
            FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = ((12d + Double.parseDouble(_stringSVValues[i])) + _doubleSVValues[i] + (
          ((double) _floatSVValues[i] + (double) _longSVValues[i]) + 0.34 + (double) _intSVValues[i])
          + _doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpression(String.format("add(%s,%s)", DOUBLE_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = BigDecimal.valueOf(_doubleSVValues[i]).add(_bigDecimalSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(
        String.format("add(add(12,%s),%s,add(add(%s,%s),'12110.34556677889901122335678',%s),%s)", STRING_SV_COLUMN,
            DOUBLE_SV_COLUMN, FLOAT_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    BigDecimal val4 = new BigDecimal("12110.34556677889901122335678");
    expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      double val1 = 12d + Double.parseDouble(_stringSVValues[i]);
      double val2 = _doubleSVValues[i];
      double val3 = (double) _floatSVValues[i] + (double) _longSVValues[i];
      BigDecimal val6 = BigDecimal.valueOf(val3).add(val4).add(BigDecimal.valueOf(_intSVValues[i]));
      expectedBigDecimalValues[i] =
          BigDecimal.valueOf(val1).add(BigDecimal.valueOf(val2)).add(val6).add(_bigDecimalSVValues[i]);
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
        new Object[]{String.format("add(%s)", INT_SV_COLUMN)}, new Object[]{
        String.format("add(%s, %s)", LONG_SV_COLUMN, INT_MV_COLUMN)
    }
    };
  }

  @Test
  public void testAdditionNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("add(%s,null)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), AdditionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intSVValues[i];
    }
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testAdditionNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("add(%s,%s)", INT_SV_COLUMN, INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    Assert.assertEquals(transformFunction.getName(), AdditionTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedValues[i] = (double) Integer.MIN_VALUE + (double) _intSVValues[i];
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = (double) _intSVValues[i] * 2;
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }
}
