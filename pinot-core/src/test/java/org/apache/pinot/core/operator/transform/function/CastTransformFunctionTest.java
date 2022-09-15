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
import java.util.Arrays;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.DataTypeConversionFunctions.cast;
import static org.testng.Assert.assertEquals;


public class CastTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testCastTransformFunctionMV() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CAST(%s AS LONG)", STRING_LONG_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    long[][] expectedLongValues = new long[NUM_ROWS][];
    ArrayCopyUtils.copy(_stringLongFormatMVValues, expectedLongValues, NUM_ROWS);
    testCastTransformFunctionMV(transformFunction, expectedLongValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(CAST(CAST(%s AS LONG) as DOUBLE) as INT)", STRING_LONG_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    long[][] innerLongValues = new long[NUM_ROWS][];
    ArrayCopyUtils.copy(_stringLongFormatMVValues, innerLongValues, NUM_ROWS);
    double[][] innerDoubleValues = new double[NUM_ROWS][];
    ArrayCopyUtils.copy(innerLongValues, innerDoubleValues, NUM_ROWS);
    int[][] expectedIntValues = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(innerDoubleValues, expectedIntValues, NUM_ROWS);
    testCastTransformFunctionMV(transformFunction, expectedIntValues);

    expression =
        RequestContextUtils.getExpression(String.format("CAST(CAST(%s AS INT) as FLOAT)", FLOAT_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    int[][] innerLayerInt = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(_floatMVValues, innerLayerInt, NUM_ROWS);
    float[][] expectedFloatValues = new float[NUM_ROWS][];
    ArrayCopyUtils.copy(innerLayerInt, expectedFloatValues, NUM_ROWS);
    testCastTransformFunctionMV(transformFunction, expectedFloatValues);

    expression = RequestContextUtils.getExpression(
        String.format("CAST(CAST(CAST(%s AS FLOAT) as INT) as STRING)", INT_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CastTransformFunction);
    assertEquals(transformFunction.getName(), CastTransformFunction.FUNCTION_NAME);
    float[][] innerFloatValues = new float[NUM_ROWS][];
    ArrayCopyUtils.copy(_intMVValues, innerFloatValues, NUM_ROWS);
    innerLayerInt = new int[NUM_ROWS][];
    ArrayCopyUtils.copy(innerFloatValues, innerLayerInt, NUM_ROWS);
    String[][] expectedStringValues = new String[NUM_ROWS][];
    ArrayCopyUtils.copy(innerLayerInt, expectedStringValues, NUM_ROWS);
    testCastTransformFunctionMV(transformFunction, expectedStringValues);

    expression = RequestContextUtils.getExpression(String.format("arrayMax(cAst(%s AS INT))", DOUBLE_MV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    FieldSpec.DataType resultDataType = transformFunction.getResultMetadata().getDataType();
    Assert.assertEquals(resultDataType, FieldSpec.DataType.INT);

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

  private void testCastTransformFunctionMV(TransformFunction transformFunction, int[][] expectedValues) {
    int[][] intMVValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longMVValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatMVValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleMVValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringMVValues = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int rowLen = expectedValues[i].length;
      for (int j = 0; j < rowLen; j++) {
        Assert.assertEquals(intMVValues[i][j], expectedValues[i][j]);
        Assert.assertEquals(longMVValues[i][j], (long) expectedValues[i][j]);
        Assert.assertEquals(floatMVValues[i][j], (float) expectedValues[i][j]);
        Assert.assertEquals(doubleMVValues[i][j], (double) expectedValues[i][j]);
        Assert.assertEquals(stringMVValues[i][j], Integer.toString(expectedValues[i][j]));
      }
    }
  }

  private void testCastTransformFunctionMV(TransformFunction transformFunction, long[][] expectedValues) {
    int[][] intMVValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longMVValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatMVValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleMVValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringMVValues = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int rowLen = expectedValues[i].length;
      for (int j = 0; j < rowLen; j++) {
        Assert.assertEquals(intMVValues[i][j], (int) expectedValues[i][j]);
        Assert.assertEquals(longMVValues[i][j], expectedValues[i][j]);
        Assert.assertEquals(floatMVValues[i][j], (float) expectedValues[i][j]);
        Assert.assertEquals(doubleMVValues[i][j], (double) expectedValues[i][j]);
        Assert.assertEquals(stringMVValues[i][j], Long.toString(expectedValues[i][j]));
      }
    }
  }

  private void testCastTransformFunctionMV(TransformFunction transformFunction, float[][] expectedValues) {
    int[][] intMVValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longMVValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatMVValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleMVValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringMVValues = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int rowLen = expectedValues[i].length;
      for (int j = 0; j < rowLen; j++) {
        Assert.assertEquals(intMVValues[i][j], (int) expectedValues[i][j]);
        Assert.assertEquals(longMVValues[i][j], (long) expectedValues[i][j]);
        Assert.assertEquals(floatMVValues[i][j], expectedValues[i][j]);
        Assert.assertEquals(doubleMVValues[i][j], (double) expectedValues[i][j]);
        Assert.assertEquals(stringMVValues[i][j], Float.toString(expectedValues[i][j]));
      }
    }
  }

  private void testCastTransformFunctionMV(TransformFunction transformFunction, double[][] expectedValues) {
    int[][] intMVValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longMVValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatMVValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleMVValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringMVValues = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int rowLen = expectedValues[i].length;
      for (int j = 0; j < rowLen; j++) {
        Assert.assertEquals(intMVValues[i][j], (int) expectedValues[i][j]);
        Assert.assertEquals(longMVValues[i][j], (long) expectedValues[i][j]);
        Assert.assertEquals(floatMVValues[i][j], (float) expectedValues[i][j]);
        Assert.assertEquals(doubleMVValues[i][j], expectedValues[i][j]);
        Assert.assertEquals(stringMVValues[i][j], Double.toString(expectedValues[i][j]));
      }
    }
  }

  private void testCastTransformFunctionMV(TransformFunction transformFunction, String[][] expectedValues) {
    int[][] intMVValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longMVValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatMVValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleMVValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringMVValues = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int rowLen = expectedValues[i].length;
      for (int j = 0; j < rowLen; j++) {
        Assert.assertEquals(intMVValues[i][j], Integer.parseInt(expectedValues[i][j]));
        Assert.assertEquals(longMVValues[i][j], Long.parseLong(expectedValues[i][j]));
        Assert.assertEquals(floatMVValues[i][j], Float.parseFloat(expectedValues[i][j]));
        Assert.assertEquals(doubleMVValues[i][j], Double.parseDouble(expectedValues[i][j]));
        Assert.assertEquals(stringMVValues[i][j], expectedValues[i][j]);
      }
    }
  }
}
