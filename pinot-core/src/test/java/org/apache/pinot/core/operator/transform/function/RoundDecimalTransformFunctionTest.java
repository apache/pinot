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
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoundDecimalTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testRoundDecimalTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("round_decimal(%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    Assert.assertEquals(transformFunction.getName(), RoundDecimalTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];

    expression = RequestContextUtils.getExpression(String.format("round_decimal(%s,2)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = round(_doubleSVValues[i], 2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("round_decimal(%s, -2)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = round(_doubleSVValues[i], -2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("round_decimal(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof RoundDecimalTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = round(_doubleSVValues[i], 0);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  public Double round(double a, int b) {
    return BigDecimal.valueOf(a).setScale(b, RoundingMode.HALF_UP).doubleValue();
  }
}
