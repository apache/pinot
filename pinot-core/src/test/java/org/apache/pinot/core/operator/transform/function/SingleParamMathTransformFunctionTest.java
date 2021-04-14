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
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.AbsTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.CeilTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.ExpTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.FloorTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.LnTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.SqrtTransformFunction;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SingleParamMathTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testAbsTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("abs(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    Assert.assertEquals(transformFunction.getName(), AbsTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.abs(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("abs(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.abs(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("abs(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.abs(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("abs(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.abs(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("abs(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.abs(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testCeilTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("ceil(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CeilTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.ceil(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ceil(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.ceil(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ceil(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.ceil(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ceil(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.ceil(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ceil(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.ceil(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testExpTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("exp(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    Assert.assertEquals(transformFunction.getName(), ExpTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.exp(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("exp(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.exp(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("exp(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.exp(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("exp(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.exp(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("exp(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.exp(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testFloorTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("floor(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    Assert.assertEquals(transformFunction.getName(), FloorTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.floor(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("floor(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.floor(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("floor(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.floor(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("floor(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.floor(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("floor(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.floor(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testLnTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("ln(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    Assert.assertEquals(transformFunction.getName(), LnTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ln(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ln(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ln(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ln(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testSqrtTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("sqrt(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    Assert.assertEquals(transformFunction.getName(), SqrtTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.sqrt(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sqrt(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.sqrt(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sqrt(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.sqrt(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sqrt(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.sqrt(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sqrt(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.sqrt(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
  }
}
