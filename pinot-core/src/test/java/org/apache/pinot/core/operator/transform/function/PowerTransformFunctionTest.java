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
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PowerTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testPowerTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("power(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    Assert.assertEquals(transformFunction.getName(), PowerTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow((double) _intSVValues[i], (double) _longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpression(String.format("power(%s,%s)", LONG_SV_COLUMN, FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow((double) _longSVValues[i], (double) _floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpression(String.format("power(%s,%s)", FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow((double) _floatSVValues[i], _doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpression(String.format("power(%s,%s)", DOUBLE_SV_COLUMN, STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow(_doubleSVValues[i], Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpression(String.format("power(%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow(Double.parseDouble(_stringSVValues[i]), (double) _intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testPowerTransformFunctionWithLiteralExponents() {
    Double exponent = 2.5;
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("power(%s, %f)", INT_SV_COLUMN, exponent));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    Assert.assertEquals(transformFunction.getName(), PowerTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.pow(_intSVValues[i], exponent);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testPowerNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("power(%s,null)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.POWER.getName());
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = 1;
    }
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testPowerNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("power(%s,%s)", INT_SV_NULL_COLUMN, 0));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof PowerTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.POWER.getName());
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
