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

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class InTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testIntInTransformFunction() {
    String expressionStr =
        String.format("%s IN (%d, %d, %d)", INT_SV_COLUMN, _intSVValues[2], _intSVValues[5], _intSVValues[9]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<Integer> inValues = Sets.newHashSet(_intSVValues[2], _intSVValues[5], _intSVValues[9]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 2 || i == 5 || i == 9) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_intSVValues[i]) ? 1 : 0);
    }
  }
  @Test
  public void testIntNotInTransformFunction() {
    String expressionStr =
        String.format("%s NOT IN (%d, %d, %d)", INT_SV_COLUMN, _intSVValues[2], _intSVValues[5], _intSVValues[9]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.NOT_IN.getName());

    Set<Integer> inValues = Sets.newHashSet(_intSVValues[2], _intSVValues[5], _intSVValues[9]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i != 2 && i != 5 && i != 9) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_intSVValues[i]) ? 0 : 1);
    }
  }

  @Test
  public void testIntMVInTransformFunction() {
    String expressionStr =
        String.format("%s IN (%d, %d, %d)", INT_MV_COLUMN, _intMVValues[2][0], _intMVValues[5][0], _intMVValues[9][0]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<Integer> inValues = Sets.newHashSet(_intMVValues[2][0], _intMVValues[5][0], _intMVValues[9][0]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 2 || i == 5 || i == 9) {
        assertEquals(intValues[i], 1);
      }
      int expected = 0;
      for (int intValue : _intMVValues[i]) {
        if (inValues.contains(intValue)) {
          expected = 1;
          break;
        }
      }
      assertEquals(intValues[i], expected);
    }
  }

  @Test
  public void testIntInTransformFunctionWithTransformedValues() {
    String expressionStr = String.format("%s IN (%d, 1+1, 4+5)", INT_SV_COLUMN, _intSVValues[2]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<Integer> inValues = Sets.newHashSet(_intSVValues[2], 2, 9);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 2) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_intSVValues[i]) ? 1 : 0);
    }
  }

  @Test
  public void testLongInTransformFunction() {
    String expressionStr =
        String.format("%s IN (%d, %d, %d)", LONG_SV_COLUMN, _longSVValues[2], _longSVValues[7], _longSVValues[11]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<Long> inValues = Sets.newHashSet(_longSVValues[2], _longSVValues[7], _longSVValues[11]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 2 || i == 7 || i == 11) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_longSVValues[i]) ? 1 : 0);
    }
  }

  @Test
  public void testFloatInTransformFunction() {
    String expressionStr =
        String.format("%s IN (%s, %s, %s)", FLOAT_SV_COLUMN, _floatSVValues[3], _floatSVValues[7], _floatSVValues[9]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<Float> inValues = Sets.newHashSet(_floatSVValues[3], _floatSVValues[7], _floatSVValues[9]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 3 || i == 7 || i == 9) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_floatSVValues[i]) ? 1 : 0);
    }
  }

  @Test
  public void testDoubleInTransformFunction() {
    String expressionStr = String.format("%s IN (%s, %s, %s)", DOUBLE_SV_COLUMN, _doubleSVValues[3], _doubleSVValues[7],
        _doubleSVValues[9]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<Double> inValues = Sets.newHashSet(_doubleSVValues[3], _doubleSVValues[7], _doubleSVValues[9]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 3 || i == 7 || i == 9) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_doubleSVValues[i]) ? 1 : 0);
    }
  }

  @Test
  public void testStringInTransformFunction() {
    String expressionStr =
        String.format("%s IN ('a','b','%s','%s')", STRING_SV_COLUMN, _stringSVValues[2], _stringSVValues[5]);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<String> inValues = Sets.newHashSet("a", "b", _stringSVValues[2], _stringSVValues[5]);
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 2 || i == 5) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(_stringSVValues[i]) ? 1 : 0);
    }
  }

  @Test
  public void testBytesInTransformFunction() {
    String expressionStr =
        String.format("%s IN ('%s','%s')", BYTES_SV_COLUMN, BytesUtils.toHexString(_bytesSVValues[2]),
            BytesUtils.toHexString(_bytesSVValues[5]));
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof InTransformFunction);
    assertEquals(transformFunction.getName(), TransformFunctionType.IN.getName());

    Set<ByteArray> inValues = Sets.newHashSet(new ByteArray(_bytesSVValues[2]), new ByteArray(_bytesSVValues[5]));
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i == 2 || i == 5) {
        assertEquals(intValues[i], 1);
      }
      assertEquals(intValues[i], inValues.contains(new ByteArray(_bytesSVValues[i])) ? 1 : 0);
    }
  }
}
