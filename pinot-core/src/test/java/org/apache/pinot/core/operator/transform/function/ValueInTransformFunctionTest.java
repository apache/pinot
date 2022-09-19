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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ValueInTransformFunctionTest extends BaseTransformFunctionTest {

  @Test(dataProvider = "testValueInTransformFunction")
  public void testValueInTransformFunction(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ValueInTransformFunction);
    assertEquals(transformFunction.getName(), ValueInTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.INT);
    assertFalse(resultMetadata.isSingleValue());
    assertTrue(resultMetadata.hasDictionary());
    int[][] dictIdsMV = transformFunction.transformToDictIdsMV(_projectionBlock);
    int[][] intValuesMV = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValuesMV = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValuesMV = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValuesMV = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);

    Dictionary dictionary = transformFunction.getDictionary();
    for (int i = 0; i < NUM_ROWS; i++) {
      IntList expectedList = new IntArrayList();
      for (int value : _intMVValues[i]) {
        if (value == 1 || value == 2 || value == 9 || value == 5) {
          expectedList.add(value);
        }
      }
      int[] expectedValues = expectedList.toIntArray();

      int numValues = expectedValues.length;
      assertEquals(dictIdsMV[i].length, numValues);
      assertEquals(intValuesMV[i].length, numValues);
      assertEquals(longValuesMV[i].length, numValues);
      assertEquals(floatValuesMV[i].length, numValues);
      assertEquals(doubleValuesMV[i].length, numValues);
      assertEquals(stringValuesMV[i].length, numValues);
      for (int j = 0; j < numValues; j++) {
        int expected = expectedValues[j];
        assertEquals(dictIdsMV[i][j], dictionary.indexOf(expected));
        assertEquals(intValuesMV[i][j], expected);
        assertEquals(longValuesMV[i][j], expected);
        assertEquals(floatValuesMV[i][j], (float) expected);
        assertEquals(doubleValuesMV[i][j], (double) expected);
        assertEquals(stringValuesMV[i][j], Integer.toString(expected));
      }
    }
  }

  @DataProvider(name = "testValueInTransformFunction")
  public Object[][] testValueInTransformFunction() {
    return new Object[][]{
        new Object[]{String.format("valueIn(%s,1,2,9,5)", INT_MV_COLUMN)}, new Object[]{
        String.format("valueIn(valueIn(valueIn(%s,9,6,5,3,2,1),1,2,3,5,9),1,2,9,5)", INT_MV_COLUMN)
    }
    };
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("valueIn(%s)", INT_MV_COLUMN)}, new Object[]{
        String.format("valueIn(%s, 1)", INT_SV_COLUMN)
    }, new Object[]{
        String.format("valueIn(%s, %s)", INT_MV_COLUMN, LONG_SV_COLUMN)
    }
    };
  }
}
