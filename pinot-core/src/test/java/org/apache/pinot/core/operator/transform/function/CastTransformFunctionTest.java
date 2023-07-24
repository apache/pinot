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
import java.sql.Timestamp;
import java.util.Arrays;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.DataTypeConversionFunctions.cast;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class CastTransformFunctionTest extends BaseTransformFunctionTest {
  @Test
  public void testCastTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CAST(%s AS string)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    String[] expectedStringValues = new String[NUM_ROWS];
    String[] scalarStringValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = Integer.toString(_intSVValues[i]);
      scalarStringValues[i] = (String) cast(_intSVValues[i], "string");
    }
    testTransformFunction(transformFunction, expectedStringValues);
    assertEquals(expectedStringValues, scalarStringValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(CAST(%s as INT) as FLOAT)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    float[] expectedFloatValues = new float[NUM_ROWS];
    float[] scalarFloatValues = new float[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedFloatValues[i] = (int) _floatSVValues[i];
      scalarFloatValues[i] = (float) cast(cast(_floatSVValues[i], "int"), "float");
    }
    testTransformFunction(transformFunction, expectedFloatValues);
    assertEquals(expectedFloatValues, scalarFloatValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(CAST(%s as BOOLEAN) as STRING)", INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = Boolean.toString(_intSVValues[i] != 0);
      scalarStringValues[i] = (String) cast(cast(_intSVValues[i], "boolean"), "string");
    }
    testTransformFunction(transformFunction, expectedStringValues);
    assertEquals(expectedStringValues, scalarStringValues);

    expression =
        RequestContextUtils.getExpression(String.format("CAST(CAST(%s as TIMESTAMP) as STRING)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = new Timestamp(_longSVValues[i]).toString();
      scalarStringValues[i] = (String) cast(cast(_longSVValues[i], "timestamp"), "string");
    }
    testTransformFunction(transformFunction, expectedStringValues);
    assertEquals(expectedStringValues, scalarStringValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(CAST(%s as BOOLEAN) as INT)", INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    int[] expectedIntValues = new int[NUM_ROWS];
    int[] scalarIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntValues[i] = _intSVValues[i] != 0 ? 1 : 0;
      scalarIntValues[i] = (int) cast(cast(_intSVValues[i], "boolean"), "int");
    }
    testTransformFunction(transformFunction, expectedIntValues);
    assertEquals(expectedIntValues, scalarIntValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(ADD(CAST(%s AS LONG), %s) AS STRING)", DOUBLE_SV_COLUMN, LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = Double.toString((double) (long) _doubleSVValues[i] + (double) _longSVValues[i]);
      scalarStringValues[i] =
          (String) cast((double) (long) cast(_doubleSVValues[i], "long") + (double) _longSVValues[i], "string");
    }
    testTransformFunction(transformFunction, expectedStringValues);
    assertEquals(expectedStringValues, scalarStringValues);

    expression = RequestContextUtils.getExpression(
        String.format("caSt(cAst(casT(%s as inT) + %s aS sTring) As DouBle)", FLOAT_SV_COLUMN, INT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
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
    assertTrue(transformFunction instanceof CastTransformFunction);
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
    assertTrue(transformFunction instanceof CastTransformFunction);
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
    assertTrue(transformFunction instanceof CastTransformFunction);
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

  @Test
  public void testCastTransformFunctionMV() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CAST(%s AS LONG)", STRING_LONG_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    long[][] expectedLongValues = new long[NUM_ROWS][];
    ArrayCopyUtils.copy(_stringLongFormatMVValues, expectedLongValues, NUM_ROWS);
    testTransformFunctionMV(transformFunction, expectedLongValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(CAST(CAST(%s AS LONG) as DOUBLE) as INT)", STRING_LONG_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    long[][] innerLongValues = new long[NUM_ROWS][];
    ArrayCopyUtils.copy(_stringLongFormatMVValues, innerLongValues, NUM_ROWS);
    double[][] innerDoubleValues = new double[NUM_ROWS][];
    ArrayCopyUtils.copy(innerLongValues, innerDoubleValues, NUM_ROWS);
    int[][] expectedIntValues = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(innerDoubleValues, expectedIntValues, NUM_ROWS);
    testTransformFunctionMV(transformFunction, expectedIntValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(CAST(%s AS INT) as FLOAT)", FLOAT_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    int[][] innerLayerInt = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(_floatMVValues, innerLayerInt, NUM_ROWS);
    float[][] expectedFloatValues = new float[NUM_ROWS][];
    ArrayCopyUtils.copy(innerLayerInt, expectedFloatValues, NUM_ROWS);
    testTransformFunctionMV(transformFunction, expectedFloatValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(CAST(CAST(%s AS FLOAT) as INT) as STRING)", INT_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    float[][] innerFloatValues = new float[NUM_ROWS][];
    ArrayCopyUtils.copy(_intMVValues, innerFloatValues, NUM_ROWS);
    innerLayerInt = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(innerFloatValues, innerLayerInt, NUM_ROWS);
    String[][] expectedStringValues = new String[NUM_ROWS][];
    ArrayCopyUtils.copy(innerLayerInt, expectedStringValues, NUM_ROWS);
    testTransformFunctionMV(transformFunction, expectedStringValues);

    expression = RequestContextUtils.getExpression(String.format("CAST(CAST(%s as BOOLEAN) as STRING)", INT_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      int length = _intMVValues[i].length;
      expectedStringValues[i] = new String[length];
      for (int j = 0; j < length; j++) {
        expectedStringValues[i][j] = Boolean.toString(_intMVValues[i][j] != 0);
      }
    }
    testTransformFunctionMV(transformFunction, expectedStringValues);

    expression =
        RequestContextUtils.getExpression(String.format("CAST(CAST(%s as TIMESTAMP) as STRING)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof CastTransformFunction);
    for (int i = 0; i < NUM_ROWS; i++) {
      int length = _longMVValues[i].length;
      expectedStringValues[i] = new String[length];
      for (int j = 0; j < length; j++) {
        expectedStringValues[i][j] = new Timestamp(_longMVValues[i][j]).toString();
      }
    }

    expression = RequestContextUtils.getExpression(String.format("arrayMax(cAst(%s AS INT))", DOUBLE_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    FieldSpec.DataType resultDataType = transformFunction.getResultMetadata().getDataType();
    assertEquals(resultDataType, FieldSpec.DataType.INT);

    // checks that arraySum triggers transformToDoubleMV in cast function which correctly cast to INT
    expression = RequestContextUtils.getExpression(String.format("arraySum(cAst(%s AS INT))", DOUBLE_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[][] afterCast = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(_doubleMVValues, afterCast, NUM_ROWS);
    double[] expectedArraySums = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedArraySums[i] = Arrays.stream(afterCast[i]).sum();
    }
    testTransformFunction(transformFunction, expectedArraySums);
  }

  @Test
  public void testCastNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression("cast(null AS INT)");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testCastNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("cast(%s AS INT)", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    int[] expectedValues = new int[NUM_ROWS];
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
