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
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.DataTypeConversionFunctions.cast;
import static org.testng.Assert.assertEquals;


public class CastTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testCastTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CAST(%s AS string)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    String[] expectedValues = new String[NUM_ROWS];
    String[] scalarStringValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Integer.toString(_intSVValues[i]);
      scalarStringValues[i] = (String) cast(_intSVValues[i], "string");
    }
    testTransformFunction(transformFunction, expectedValues);
    assertEquals(expectedValues, scalarStringValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(CAST(%s as INT) as FLOAT)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    float[] expectedFloatValues = new float[NUM_ROWS];
    float[] scalarFloatValues = new float[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedFloatValues[i] = (int) _floatSVValues[i];
      scalarFloatValues[i] = (float) cast(cast(_floatSVValues[i], "int"), "float");
    }
    testTransformFunction(transformFunction, expectedFloatValues);
    assertEquals(expectedFloatValues, scalarFloatValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(ADD(CAST(%s AS LONG), %s) AS STRING)", DOUBLE_SV_COLUMN, LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Double.toString((double) (long) _doubleSVValues[i] + (double) _longSVValues[i]);
      scalarStringValues[i] =
          (String) cast((double) (long) cast(_doubleSVValues[i], "long") + (double) _longSVValues[i], "string");
    }
    testTransformFunction(transformFunction, expectedValues);
    assertEquals(expectedValues, scalarStringValues);

    expression = RequestContextUtils.getExpression(
        String.format("caSt(cAst(casT(%s as inT) + %s aS sTring) As DouBle)", FLOAT_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    double[] expectedDoubleValues = new double[NUM_ROWS];
    double[] scalarDoubleValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleValues[i] = (double) (int) _floatSVValues[i] + (double) _intSVValues[i];
      scalarDoubleValues[i] =
          (double) cast(cast((double) (int) cast(_floatSVValues[i], "int") + (double) _intSVValues[i], "string"),
              "double");
    }
    testTransformFunction(transformFunction, expectedDoubleValues);
    assertEquals(expectedDoubleValues, scalarDoubleValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(CAST(%s AS INT) - CAST(%s AS FLOAT) / CAST(%s AS DOUBLE) AS LONG)", DOUBLE_SV_COLUMN,
            LONG_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    long[] expectedLongValues = new long[NUM_ROWS];
    long[] longScalarValues = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedLongValues[i] =
          (long) ((double) (int) _doubleSVValues[i] - (double) (float) _longSVValues[i] / (double) _intSVValues[i]);
      longScalarValues[i] = (long) cast((double) (int) cast(_doubleSVValues[i], "int")
          - (double) (float) cast(_longSVValues[i], "float") / (double) cast(_intSVValues[i], "double"), "long");
    }
    testTransformFunction(transformFunction, expectedLongValues);
    assertEquals(expectedLongValues, longScalarValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(%s AS BIG_DECIMAL)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    BigDecimal[] bigDecimalScalarValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = BigDecimal.valueOf(_longSVValues[i]);
      bigDecimalScalarValues[i] = (BigDecimal) cast(_longSVValues[i], "BIG_DECIMAL");
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);
    assertEquals(expectedBigDecimalValues, bigDecimalScalarValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(CAST(%s AS DOUBLE) - CAST(%s AS DOUBLE) / CAST(%s AS DOUBLE) AS BIG_DECIMAL)",
            BIG_DECIMAL_SV_COLUMN, LONG_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = BigDecimal.valueOf(
          _bigDecimalSVValues[i].doubleValue() - (double) _longSVValues[i] / (double) _intSVValues[i]);
      double d =
          (double) cast(_bigDecimalSVValues[i], "double") - (double) cast(_longSVValues[i], "double") / (double) cast(
              _intSVValues[i], "double");
      bigDecimalScalarValues[i] = (BigDecimal) cast(d, "BIG_DECIMAL");
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);
    assertEquals(expectedBigDecimalValues, bigDecimalScalarValues);
  }
}
