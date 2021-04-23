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
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AdditionTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testAdditionTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(String
        .format("add(%s,%s,%s,%s,%s)", INT_SV_COLUMN, LONG_SV_COLUMN, FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN,
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

    expression = RequestContextUtils.getExpressionFromSQL(String
        .format("add(add(12,%s),%s,add(add(%s,%s),0.34,%s),%s)", STRING_SV_COLUMN, DOUBLE_SV_COLUMN, FLOAT_SV_COLUMN,
            LONG_SV_COLUMN, INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AdditionTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = ((12d + Double.parseDouble(_stringSVValues[i])) + _doubleSVValues[i] + (
          ((double) _floatSVValues[i] + (double) _longSVValues[i]) + 0.34 + (double) _intSVValues[i])
          + _doubleSVValues[i]);
    }
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{new Object[]{String.format("add(%s)", INT_SV_COLUMN)}, new Object[]{String.format(
        "add(%s, %s)", LONG_SV_COLUMN, INT_MV_COLUMN)}};
  }
}
