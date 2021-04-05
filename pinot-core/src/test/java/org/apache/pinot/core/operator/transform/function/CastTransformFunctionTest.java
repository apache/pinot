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


public class CastTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testCastTransformFunction() {
    ExpressionContext expression =
        RequestContextConvertUtils.getExpression(String.format("cast(%s,%s)", INT_SV_COLUMN, "'String'"));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Integer.toString(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextConvertUtils.getExpression(
        String.format("cast(add(cast(%s, 'STRING'), %s), 'string')", STRING_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.toString(Double.parseDouble(_stringSVValues[i]) + _doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextConvertUtils.getExpression(
        String.format("cast(cast(add(cast(%s, 'STRING'), %s), 'string'), 'double')", STRING_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    double[] expectedDoubleValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleValues[i] = Double.parseDouble(_stringSVValues[i]) + _intSVValues[i];
    }
    testTransformFunction(transformFunction, expectedDoubleValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextConvertUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{new Object[]{String.format("cast(%s)", INT_SV_COLUMN)}, new Object[]{String.format(
        "cast(%s, %s, %s)", LONG_SV_COLUMN, "'STRING'", "'STRING'")}};
  }
}
