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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TruncateDecimalTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testTruncateDecimalTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("truncate(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof TruncateDecimalTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TruncateDecimalTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];

    expression = RequestContextUtils.getExpression(String.format("truncate(%s,2)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof TruncateDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = truncate(_doubleSVValues[i], 2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("truncate(%s, -2)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof TruncateDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = truncate(_doubleSVValues[i], -2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("truncate(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof TruncateDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = truncate(_doubleSVValues[i], 0);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  public Double truncate(double a, int b) {
    return BigDecimal.valueOf(a).setScale(b, RoundingMode.DOWN).doubleValue();
  }

  @Test
  public void testTruncateNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression("truncate(null, 1)");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof TruncateDecimalTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.TRUNCATE.getName());
    double[] expectedValues = new double[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testTruncateNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("truncate(%s, 0)", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof TruncateDecimalTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.TRUNCATE.getName());
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
