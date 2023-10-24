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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TupleSelectionTransformFunctionsTest extends BaseTransformFunctionTest {

  @DataProvider
  public static Object[][] rejectedParameters() {
    return new Object[][]{
        {"()"}, {String.format("(%s)", INT_MV_COLUMN)}, {String.format("(%s)", LONG_MV_COLUMN)}, {
        String.format("(%s)", FLOAT_MV_COLUMN)
    }, {String.format("(%s)", DOUBLE_MV_COLUMN)}, {String.format("(%s)", STRING_MV_COLUMN)}, {
        String.format("(%s, %s)", INT_MV_COLUMN, INT_SV_COLUMN)
    }, {String.format("(%s, %s)", STRING_SV_COLUMN, INT_SV_COLUMN)}, {
        String.format("(%s, %s)", STRING_SV_COLUMN, LONG_SV_COLUMN)
    }, {String.format("(%s, %s)", STRING_SV_COLUMN, FLOAT_SV_COLUMN)}, {
        String.format("(%s, %s)", STRING_SV_COLUMN, DOUBLE_SV_COLUMN)
    }, {String.format("(%s, %s)", STRING_SV_COLUMN, TIMESTAMP_COLUMN)}, {
        String.format("(%s, %s)", INT_SV_COLUMN, TIMESTAMP_COLUMN)
    }, {String.format("(%s, %s)", FLOAT_SV_COLUMN, TIMESTAMP_COLUMN)}, {
        String.format("(%s, %s)", DOUBLE_SV_COLUMN, TIMESTAMP_COLUMN)
    }, {String.format("(%s, %s)", TIMESTAMP_COLUMN, INT_SV_COLUMN)}, {
        String.format("(%s, %s)", INT_SV_COLUMN, INT_MV_COLUMN)
    }, {String.format("(%s, %s)", INT_SV_COLUMN, STRING_SV_COLUMN)}, {
        String.format("(%s, %s)", TIMESTAMP_COLUMN, INT_SV_COLUMN)
    }, {String.format("(%s, %s)", TIMESTAMP_COLUMN, FLOAT_SV_COLUMN)}, {
        String.format("(%s, %s)", TIMESTAMP_COLUMN, DOUBLE_SV_COLUMN)
    }, {String.format("(%s, %s)", TIMESTAMP_COLUMN, STRING_SV_COLUMN)}, {
        String.format("(%s, %s)", TIMESTAMP_COLUMN, TIME_COLUMN)
    }
    };
  }

  @Test
  public void testLeastTransformFunctionInt() {
    // -1 will be passed in as a long.
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, %d, cast(%s as INT))", INT_SV_COLUMN, -1, FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], Math.min(Math.min(_intSVValues[i], -1), (int) _floatSVValues[i]));
    }
  }

  @Test
  public void testLeastTransformFunctionString() {
    TransformFunction transformFunction = testLeastPreconditions(
        String.format("least(cast(%s as STRING), cast(%s as STRING))", INT_SV_COLUMN, FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.STRING);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      String left = Integer.toString(_intSVValues[i]);
      String right = Float.toString(_floatSVValues[i]);
      assertEquals(stringValues[i], left.compareTo(right) < 0 ? left : right);
    }
  }

  @Test
  public void testLeastTransformFunctionUnaryInt() {
    TransformFunction transformFunction = testLeastPreconditions(String.format("least(%s)", INT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], _intSVValues[i]);
    }
  }

  @Test
  public void testLeastTransformFunctionIntLong() {
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, %d, %s)", INT_SV_COLUMN, -1, LONG_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.LONG);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], Math.min(Math.min((long) _intSVValues[i], -1), _longSVValues[i]));
    }
  }

  @Test
  public void testLeastTransformFunctionUnaryLong() {
    TransformFunction transformFunction = testLeastPreconditions(String.format("least(%s)", LONG_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.LONG);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], _longSVValues[i]);
    }
  }

  @Test
  public void testLeastTransformFunctionIntFloat() {
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, %d, %s)", INT_SV_COLUMN, -1, FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(doubleValues[i], Math.min(Math.min(_intSVValues[i], -1), Double.valueOf(_floatSVValues[i])));
    }
  }

  @Test
  public void testLeastTransformFunctionUnaryFloat() {
    TransformFunction transformFunction = testLeastPreconditions(String.format("least(%s)", FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.FLOAT);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(floatValues[i], _floatSVValues[i]);
    }
  }

  @Test
  public void testLeastTransformFunctionIntDouble() {
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, %d, %s)", INT_SV_COLUMN, -1, DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(doubleValues[i], Math.min(Math.min(_intSVValues[i], -1), _doubleSVValues[i]));
    }
  }

  @Test
  public void testLeastTransformFunctionUnaryDouble() {
    TransformFunction transformFunction = testLeastPreconditions(String.format("least(%s)", DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(doubleValues[i], _doubleSVValues[i]);
    }
  }

  @Test
  public void testLeastTransformFunctionNumericTypes1() {
    TransformFunction transformFunction = testLeastPreconditions(
        String.format("least(%s, %s, %s)", LONG_SV_COLUMN, FLOAT_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.BIG_DECIMAL);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(bigDecimalValues[i], (BigDecimal.valueOf(_longSVValues[i])).min(
          BigDecimal.valueOf(_floatSVValues[i]).min(_bigDecimalSVValues[i])));
    }
  }

  @Test
  public void testLeastTransformFunctionNumericTypes2() {
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, %s, %s)", INT_SV_COLUMN, FLOAT_SV_COLUMN, LONG_SV_COLUMN));
    // Note: In the current code, least return type is DOUBLE if there is a float input, but output values are float!
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(floatValues[i], Math.min(_intSVValues[i], Math.min(_floatSVValues[i], _longSVValues[i])));
    }
  }

  @Test
  public void testLeastTransformFunctionTime() {
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, cast(%s AS TIMESTAMP))", TIMESTAMP_COLUMN, LONG_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.TIMESTAMP);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], Math.min(_longSVValues[i], _timeValues[i]));
    }
  }

  @Test
  public void testLeastTransformFunctionUnaryString() {
    TransformFunction transformFunction = testLeastPreconditions(String.format("least(%s)", STRING_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.STRING);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(stringValues[i], _stringSVValues[i]);
    }
  }

  @Test
  public void testLeastTransformFunctionTimestampWithLegacyTimeColumn() {
    TransformFunction transformFunction =
        testLeastPreconditions(String.format("least(%s, cast(%s as TIMESTAMP))", TIMESTAMP_COLUMN, TIME_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.TIMESTAMP);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], _timeValues[i]);
    }
  }

  @Test
  public void testLeastTransformFunctionNullLiteral() {
    TransformFunction transformFunction = testLeastPreconditionsNullHandlingEnabled(
        String.format("least(%s, null, %s)", INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0, (long) NUM_ROWS);

    testNullBitmap(transformFunction, nullBitmap);
  }

  @Test
  public void testLeastTransformFunctionNullColumn() {
    TransformFunction transformFunction = testLeastPreconditionsNullHandlingEnabled(
        String.format("least(%s, %s)", INT_SV_NULL_COLUMN, DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    RoaringBitmap nullBitmap = transformFunction.getNullBitmap(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        assertTrue(nullBitmap.contains(i));
      } else {
        assertEquals(doubleValues[i], Math.min(_intSVValues[i], _doubleSVValues[i]));
      }
    }
  }

  @Test
  public void testLeastTransformFunctionAllNulls() {
    TransformFunction transformFunction =
        testLeastPreconditionsNullHandlingEnabled(String.format("least(null, null, null)"));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.UNKNOWN);
    RoaringBitmap expectedNull = new RoaringBitmap();
    expectedNull.add(0L, NUM_ROWS);
    assertEquals(transformFunction.getNullBitmap(_projectionBlock), expectedNull);
  }

  @Test
  public void testLeastTransformFunctionPartialAllNulls() {
    TransformFunction transformFunction = testLeastPreconditionsNullHandlingEnabled(
        String.format("least(%s, %s, %s)", INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
   int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    RoaringBitmap expectedNull = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNull.add(i);
      } else {
        assertEquals(intValues[i], _intSVValues[i]);
      }
    }
    testNullBitmap(transformFunction, expectedNull);
  }

  @Test(dataProvider = "rejectedParameters", expectedExceptions = BadQueryRequestException.class)
  public void testRejectLeast(String params) {
    testGreatestPreconditions("least" + params);
  }

  @Test
  public void testGreatestTransformFunctionInt() {
    TransformFunction transformFunction = testGreatestPreconditions(
        String.format("greatest(%s, %d, cast(%s as INT))", INT_SV_COLUMN, -1, FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], Math.max(Math.max(_intSVValues[i], -1), (int) _floatSVValues[i]));
    }
  }

  @Test
  public void testGreatestTransformFunctionUnaryInt() {
    TransformFunction transformFunction = testGreatestPreconditions(String.format("greatest(%s)", INT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], _intSVValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionIntLong() {
    TransformFunction transformFunction =
        testGreatestPreconditions(String.format("greatest(%s, %d, %s)", INT_SV_COLUMN, -1, LONG_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.LONG);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], Math.max(Math.max((long) _intSVValues[i], -1), _longSVValues[i]));
    }
  }

  @Test
  public void testGreatestTransformFunctionUnaryLong() {
    TransformFunction transformFunction = testGreatestPreconditions(String.format("greatest(%s)", LONG_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.LONG);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], _longSVValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionIntFloat() {
    TransformFunction transformFunction =
        testGreatestPreconditions(String.format("greatest(%s, %d, %s)", INT_SV_COLUMN, -1, FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(doubleValues[i], Math.max(Math.max(_intSVValues[i], -1), Double.valueOf(_floatSVValues[i])));
    }
  }

  @Test
  public void testGreatestTransformFunctionUnaryFloat() {
    TransformFunction transformFunction = testGreatestPreconditions(String.format("greatest(%s)", FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.FLOAT);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(floatValues[i], _floatSVValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionUnaryString() {
    TransformFunction transformFunction = testGreatestPreconditions(String.format("greatest(%s)", STRING_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.STRING);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(stringValues[i], _stringSVValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionIntDouble() {
    TransformFunction transformFunction =
        testGreatestPreconditions(String.format("greatest(%s, %d, %s)", INT_SV_COLUMN, -1, DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(doubleValues[i], Math.max(Math.max(_intSVValues[i], -1), _doubleSVValues[i]));
    }
  }

  @Test
  public void testGreatestTransformFunctionNullLiteral() {
    TransformFunction transformFunction = testGreatestPreconditionsNullHandlingEnabled(
        String.format("greatest(%s, null, %s)", INT_SV_COLUMN, DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0, (long) NUM_ROWS);

    testNullBitmap(transformFunction, nullBitmap);
  }

  @Test
  public void testGreatestTransformFunctionNullColumn() {
    TransformFunction transformFunction = testGreatestPreconditionsNullHandlingEnabled(
        String.format("greatest(%s, %s)", INT_SV_NULL_COLUMN, DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    RoaringBitmap nullBitmap = transformFunction.getNullBitmap(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        assertTrue(nullBitmap.contains(i));
      } else {
        assertEquals(doubleValues[i], Math.max(_intSVValues[i], _doubleSVValues[i]));
      }
    }
  }

  @Test
  public void testGreatestTransformFunctionAllNulls() {
    TransformFunction transformFunction =
        testGreatestPreconditionsNullHandlingEnabled(String.format("greatest(null, null, null)"));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.UNKNOWN);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    RoaringBitmap expectedNull = new RoaringBitmap();
    expectedNull.add(0L, NUM_ROWS);
    testNullBitmap(transformFunction, expectedNull);
  }

  @Test
  public void testGreatestTransformFunctionPartialAllNulls() {
    TransformFunction transformFunction = testGreatestPreconditionsNullHandlingEnabled(
        String.format("greatest(%s, %s, %s)", INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    RoaringBitmap expectedNull = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNull.add(i);
      } else {
        assertEquals(intValues[i], _intSVValues[i]);
      }
    }
    testNullBitmap(transformFunction, expectedNull);
  }

  @Test
  public void testGreatestTransformFunctionString() {
    TransformFunction transformFunction = testGreatestPreconditions(
        String.format("greatest(cast(%s as STRING), cast(%s as STRING))", INT_SV_COLUMN, FLOAT_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.STRING);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      String left = Integer.toString(_intSVValues[i]);
      String right = Float.toString(_floatSVValues[i]);
      assertEquals(stringValues[i], left.compareTo(right) >= 0 ? left : right);
    }
  }

  @Test
  public void testGreatestTransformFunctionUnaryDouble() {
    TransformFunction transformFunction = testGreatestPreconditions(String.format("greatest(%s)", DOUBLE_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(doubleValues[i], _doubleSVValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionUnaryBigDecimal() {
    TransformFunction transformFunction =
        testGreatestPreconditions(String.format("greatest(%s)", BIG_DECIMAL_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.BIG_DECIMAL);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(bigDecimalValues[i], _bigDecimalSVValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionNumericTypes() {
    TransformFunction transformFunction = testGreatestPreconditions(
        String.format("greatest(%s, %s, %s)", INT_SV_COLUMN, DOUBLE_SV_COLUMN, BIG_DECIMAL_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.BIG_DECIMAL);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(bigDecimalValues[i], (BigDecimal.valueOf(_intSVValues[i])).max(
          _bigDecimalSVValues[i].max(BigDecimal.valueOf(_doubleSVValues[i]))));
    }
  }

  @Test
  public void testGreatestTransformFunctionTime() {
    TransformFunction transformFunction = testGreatestPreconditions(
        String.format("greatest(%s, cast(%s AS TIMESTAMP))", TIMESTAMP_COLUMN, LONG_SV_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.TIMESTAMP);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], Math.max(_longSVValues[i], -1), _timeValues[i]);
    }
  }

  @Test
  public void testGreatestTransformFunctionTimestampWithLegacyTimeColumn() {
    TransformFunction transformFunction =
        testGreatestPreconditions(String.format("greatest(%s, cast(%s as TIMESTAMP))", TIMESTAMP_COLUMN, TIME_COLUMN));
    assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.TIMESTAMP);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(longValues[i], _timeValues[i]);
    }
  }

  @Test(dataProvider = "rejectedParameters", expectedExceptions = BadQueryRequestException.class)
  public void testRejectGreatest(String params) {
    testGreatestPreconditions("greatest" + params);
  }

  private TransformFunction testLeastPreconditions(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof LeastTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.LEAST.getName());
    return transformFunction;
  }

  private TransformFunction testGreatestPreconditions(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof GreatestTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.GREATEST.getName());
    return transformFunction;
  }

  private TransformFunction testLeastPreconditionsNullHandlingEnabled(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.getNullHandlingEnabled(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof LeastTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.LEAST.getName());
    return transformFunction;
  }

  private TransformFunction testGreatestPreconditionsNullHandlingEnabled(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.getNullHandlingEnabled(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof GreatestTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.GREATEST.getName());
    return transformFunction;
  }
}
