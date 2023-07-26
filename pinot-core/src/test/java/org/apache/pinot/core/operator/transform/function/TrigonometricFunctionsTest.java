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

import java.util.function.DoubleUnaryOperator;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.AcosTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.AsinTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.Atan2TransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.AtanTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.CosTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.CoshTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.DegreesTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.RadiansTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.SinTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.SinhTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.TanTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.TanhTransformFunction;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TrigonometricFunctionsTest extends BaseTransformFunctionTest {

  @Test
  public void testSinTransformFunction() {
    testTrigonometricTransformFunction(SinTransformFunction.class, "sin", Math::sin);
  }

  @Test
  public void testCosTransformFunction() {
    testTrigonometricTransformFunction(CosTransformFunction.class, "cos", Math::cos);
  }

  @Test
  public void testTanTransformFunction() {
    testTrigonometricTransformFunction(TanTransformFunction.class, "tan", Math::tan);
  }

  @Test
  public void testAsinTransformFunction() {
    testTrigonometricTransformFunction(AsinTransformFunction.class, "asin", Math::asin);
  }

  @Test
  public void testACosTransformFunction() {
    testTrigonometricTransformFunction(AcosTransformFunction.class, "acos", Math::acos);
  }

  @Test
  public void testAtanTransformFunction() {
    testTrigonometricTransformFunction(AtanTransformFunction.class, "atan", Math::atan);
  }

  @Test
  public void testSinHTransformFunction() {
    testTrigonometricTransformFunction(SinhTransformFunction.class, "sinh", Math::sinh);
  }

  @Test
  public void testCosHTransformFunction() {
    testTrigonometricTransformFunction(CoshTransformFunction.class, "cosh", Math::cosh);
  }

  @Test
  public void testTanHTransformFunction() {
    testTrigonometricTransformFunction(TanhTransformFunction.class, "tanh", Math::tanh);
  }

  @Test
  public void testDegreesTransformFunction() {
    testTrigonometricTransformFunction(DegreesTransformFunction.class, "degrees", Math::toDegrees);
  }

  @Test
  public void testRadiansTransformFunction() {
    testTrigonometricTransformFunction(RadiansTransformFunction.class, "radians", Math::toRadians);
  }

  @Test
  public void testAtan2TransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("atan2(%s, %s)", DOUBLE_SV_COLUMN, FLOAT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Atan2TransformFunction);
    Assert.assertEquals(transformFunction.getName(), Atan2TransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.atan2(_doubleSVValues[i], _floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  public void testTrigonometricTransformFunction(Class<?> clazz, String sqlFunction, DoubleUnaryOperator op) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("%s(%s)", sqlFunction, INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(clazz.isInstance(transformFunction));
    Assert.assertEquals(transformFunction.getName(), sqlFunction);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = op.applyAsDouble(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("%s(%s)", sqlFunction, LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(clazz.isInstance(transformFunction));
    Assert.assertEquals(transformFunction.getName(), sqlFunction);
    expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = op.applyAsDouble(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("%s(%s)", sqlFunction, FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(clazz.isInstance(transformFunction));
    Assert.assertEquals(transformFunction.getName(), sqlFunction);
    expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = op.applyAsDouble(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("%s(%s)", sqlFunction, DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(clazz.isInstance(transformFunction));
    Assert.assertEquals(transformFunction.getName(), sqlFunction);
    expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = op.applyAsDouble(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("%s(%s)", sqlFunction, STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(clazz.isInstance(transformFunction));
    Assert.assertEquals(transformFunction.getName(), sqlFunction);
    expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = op.applyAsDouble(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("%s(%s)", sqlFunction, INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(clazz.isInstance(transformFunction));
    Assert.assertEquals(transformFunction.getName(), sqlFunction);
    expectedValues = new double[NUM_ROWS];
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = op.applyAsDouble(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }
}
