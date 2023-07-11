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
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.AbsTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.CeilTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.ExpTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.FloorTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.LnTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.Log10TransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.Log2TransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.SignTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.SqrtTransformFunction;
import org.roaringbitmap.RoaringBitmap;
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

    expression = RequestContextUtils.getExpression(String.format("abs(%s)", BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = _bigDecimalSVValues[i].abs();
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(String.format("abs(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof AbsTransformFunction);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.abs(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
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

    expression = RequestContextUtils.getExpression(String.format("ceil(%s)", BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = _bigDecimalSVValues[i].setScale(0, RoundingMode.CEILING);
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);
    expression = RequestContextUtils.getExpression(String.format("ceil(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CeilTransformFunction);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.ceil(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
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

    // exp(x) always return double. Cast BigDecimal values first to double, then call exp(x).
    expression = RequestContextUtils.getExpression(String.format("exp(CAST(%s AS DOUBLE))", BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.exp(_bigDecimalSVValues[i].doubleValue());
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("exp(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ExpTransformFunction);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.exp(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
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

    expression = RequestContextUtils.getExpression(String.format("floor(%s)", BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = _bigDecimalSVValues[i].setScale(0, RoundingMode.FLOOR);
      ;
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(String.format("floor(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof FloorTransformFunction);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.floor(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
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

    // ln(x) always return double. Cast BigDecimal values first to double, then call ln(x).
    expression = RequestContextUtils.getExpression(String.format("ln(CAST(%s AS DOUBLE))", BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_bigDecimalSVValues[i].doubleValue());
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("ln(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LnTransformFunction);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.log(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
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

    // sqrt(x) always return double. Cast BigDecimal values first to double, then call sqrt(x).
    expression = RequestContextUtils.getExpression(String.format("sqrt(CAST(%s AS DOUBLE))", BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.sqrt(_bigDecimalSVValues[i].doubleValue());
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sqrt(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SqrtTransformFunction);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.sqrt(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  @Test
  public void testCombinationMathTransformFunctions() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("add(ceil(%s), abs(floor(%s)))", INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.ceil(_intSVValues[i]) + Math.abs(Math.floor(_doubleSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(
        String.format("add(ceil(%s), abs(floor(%s)))", INT_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    BigDecimal[] expectedBigDecimalValues = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBigDecimalValues[i] = _bigDecimalSVValues[i].setScale(0, RoundingMode.FLOOR).abs()
          .add(BigDecimal.valueOf(Math.ceil(_intSVValues[i])));
    }
    testTransformFunction(transformFunction, expectedBigDecimalValues);

    expression = RequestContextUtils.getExpression(
        String.format("add(ceil(%s), sqrt(%s))", INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.sqrt(_intSVValues[i]) + Math.ceil(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  @Test
  public void testLog10TransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("log10(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log10TransformFunction);
    Assert.assertEquals(transformFunction.getName(), Log10TransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log10(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log10(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log10TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log10(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log10(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log10TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log10(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log10(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log10TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log10(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log10(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log10TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log10(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log10(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.log10(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  @Test
  public void testLog2TransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("log2(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log2TransformFunction);
    Assert.assertEquals(transformFunction.getName(), Log2TransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_intSVValues[i]) / Math.log(2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log2(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log2TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_longSVValues[i]) / Math.log(2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log2(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log2TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_floatSVValues[i]) / Math.log(2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log2(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log2TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(_doubleSVValues[i]) / Math.log(2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log2(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof Log2TransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.log(Double.parseDouble(_stringSVValues[i])) / Math.log(2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("log2(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.log(_intSVValues[i]) / Math.log(2);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  @Test
  public void testSignTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("sign(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SignTransformFunction);
    Assert.assertEquals(transformFunction.getName(), SignTransformFunction.FUNCTION_NAME);
    double[] expectedValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.signum(_intSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sign(%s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SignTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.signum(_longSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sign(%s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SignTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.signum(_floatSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sign(%s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SignTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.signum(_doubleSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = RequestContextUtils.getExpression(String.format("sign(%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof SignTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Math.signum(Double.parseDouble(_stringSVValues[i]));
    }
    testTransformFunction(transformFunction, expectedValues);
    expression = RequestContextUtils.getExpression(String.format("sign(%s)", INT_SV_NULL_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = Math.signum(_intSVValues[i]);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }
}
