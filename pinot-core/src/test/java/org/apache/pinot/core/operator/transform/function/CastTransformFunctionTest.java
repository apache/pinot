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

import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.request.context.ExpressionContext;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CastTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testCastTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpressionFromSQL(String.format("CAST(%s AS string)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Integer.toString(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        RequestContextUtils.getExpressionFromSQL(String.format("CAST(CAST(%s as INT) as FLOAT)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    float[] expectedFloatValues = new float[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedFloatValues[i] = (int) _floatSVValues[i];
    }
    testTransformFunction(transformFunction, expectedFloatValues);

    expression = RequestContextUtils.getExpressionFromSQL(
        String.format("CAST(ADD(CAST(%s AS LONG), %s) AS STRING)", DOUBLE_SV_COLUMN, LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.toString((double) (long) _doubleSVValues[i] + (double) _longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpressionFromSQL(
        String.format("caSt(cAst(casT(%s as inT) + %s aS sTring) As DouBle)", FLOAT_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    double[] expectedDoubleValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleValues[i] = (double) (int) _floatSVValues[i] + (double) _intSVValues[i];
    }
    testTransformFunction(transformFunction, expectedDoubleValues);

    expression = RequestContextUtils.getExpressionFromSQL(String
        .format("CAST(CAST(%s AS INT) - CAST(%s AS FLOAT) / CAST(%s AS DOUBLE) AS LONG)", DOUBLE_SV_COLUMN,
            LONG_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    long[] expectedLongValues = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedLongValues[i] =
          (long) ((double) (int) _doubleSVValues[i] - (double) (float) _longSVValues[i] / (double) _intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedLongValues);
  }
}
