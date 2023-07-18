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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class ArrayBaseTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testArrayTransformFunction() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("%s(%s)", getFunctionName(), INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getClass().getName(), getArrayFunctionClass().getName());
    Assert.assertEquals(transformFunction.getName(), getFunctionName());
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), getResultDataType(FieldSpec.DataType.INT));
    Assert.assertTrue(transformFunction.getResultMetadata().isSingleValue());
    Assert.assertFalse(transformFunction.getResultMetadata().hasDictionary());

    switch (getResultDataType(FieldSpec.DataType.INT)) {
      case INT:
        int[] intResults = transformFunction.transformToIntValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          Assert.assertEquals(intResults[i], getExpectResult(_intMVValues[i]));
        }
        break;
      case LONG:
        long[] longResults = transformFunction.transformToLongValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          Assert.assertEquals(longResults[i], getExpectResult(_intMVValues[i]));
        }
        break;
      case FLOAT:
        float[] floatResults = transformFunction.transformToFloatValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          Assert.assertEquals(floatResults[i], getExpectResult(_intMVValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleResults = transformFunction.transformToDoubleValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          Assert.assertEquals(doubleResults[i], getExpectResult(_intMVValues[i]));
        }
        break;
      case STRING:
        String[] stringResults = transformFunction.transformToStringValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          Assert.assertEquals(stringResults[i], getExpectResult(_intMVValues[i]));
        }
        break;
      default:
        break;
    }
  }

  @Test
  public void testArrayNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("%s(%s)", getFunctionName(), INT_MV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getClass().getName(), getArrayFunctionClass().getName());
    Assert.assertEquals(transformFunction.getName(), getFunctionName());
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), getResultDataType(FieldSpec.DataType.INT));
    Assert.assertTrue(transformFunction.getResultMetadata().isSingleValue());
    Assert.assertFalse(transformFunction.getResultMetadata().hasDictionary());

    RoaringBitmap expectedNulls = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNulls.add(i);
      }
    }
    testNullBitmap(transformFunction, expectedNulls);

    switch (getResultDataType(FieldSpec.DataType.INT)) {
      case INT:
        int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          if (!isNullRow(i)) {
            Assert.assertEquals(intValues[i], getExpectResult(_intMVValues[i]));
          }
        }
        break;
      case LONG:
        long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          if (!isNullRow(i)) {
            Assert.assertEquals(longValues[i], getExpectResult(_intMVValues[i]));
          }
        }
        break;
      case FLOAT:
        float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          if (!isNullRow(i)) {
            Assert.assertEquals(floatValues[i], getExpectResult(_intMVValues[i]));
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          if (!isNullRow(i)) {
            Assert.assertEquals(doubleValues[i], getExpectResult(_intMVValues[i]));
          }
        }
        break;
      case STRING:
        String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          if (!isNullRow(i)) {
            Assert.assertEquals(stringValues[i], getExpectResult(_intMVValues[i]));
          }
        }
        break;
      default:
        break;
    }
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{
            String.format("%s(%s,1)", getFunctionName(), INT_MV_COLUMN)
        }, new Object[]{String.format("%s(2)", getFunctionName())}, new Object[]{
        String.format("%s(%s)", getFunctionName(), LONG_SV_COLUMN)
    }
    };
  }

  abstract String getFunctionName();

  abstract Object getExpectResult(int[] intArray);

  abstract Class getArrayFunctionClass();

  abstract FieldSpec.DataType getResultDataType(FieldSpec.DataType inputDataType);
}
