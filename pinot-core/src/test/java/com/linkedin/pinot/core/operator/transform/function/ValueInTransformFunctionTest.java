/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ValueInTransformFunctionTest extends BaseTransformFunctionTest {

  @Test(dataProvider = "testValueInTransformFunction")
  public void testValueInTransformFunction(String expressionStr) {
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ValueInTransformFunction);
    Assert.assertEquals(transformFunction.getName(), ValueInTransformFunction.FUNCTION_NAME);
    Assert.assertTrue(transformFunction.getResultMetadata().hasDictionary());
    int[][] dictIds = transformFunction.transformToDictIdsMV(_projectionBlock);
    int[][] intValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValues = transformFunction.transformToStringValuesMV(_projectionBlock);

    Dictionary dictionary = transformFunction.getDictionary();
    for (int i = 0; i < NUM_ROWS; i++) {
      IntList expectedList = new IntArrayList();
      for (int value : _intMVValues[i]) {
        if (value == 1 || value == 2 || value == 9 || value == 5) {
          expectedList.add(value);
        }
      }
      int[] expectedValues = expectedList.toIntArray();
      // NOTE: need to sort the expected array because we sort the dictionary Ids for multi-valued entries
      Arrays.sort(expectedValues);

      int numValues = expectedValues.length;
      for (int j = 0; j < numValues; j++) {
        int expected = expectedValues[j];
        Assert.assertEquals(dictIds[i][j], dictionary.indexOf(expected));
        Assert.assertEquals(intValues[i][j], expected);
        Assert.assertEquals(longValues[i][j], (long) expected);
        Assert.assertEquals(floatValues[i][j], (float) expected);
        Assert.assertEquals(doubleValues[i][j], (double) expected);
        Assert.assertEquals(stringValues[i][j], Integer.toString(expected));
      }
    }
  }

  @DataProvider(name = "testValueInTransformFunction")
  public Object[][] testValueInTransformFunction() {
    return new Object[][]{
        new Object[]{String.format("valueIn(%s,1,2,9,5)", INT_MV_COLUMN)},
        new Object[]{String.format("valueIn(valueIn(valueIn(%s,9,6,5,3,2,1),1,2,3,5,9),1,2,9,5)", INT_MV_COLUMN)}};
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("valueIn(%s)", INT_MV_COLUMN)},
        new Object[]{String.format("valueIn(%s, 1)", INT_SV_COLUMN)},
        new Object[]{String.format("valueIn(%s, %s)", INT_MV_COLUMN, LONG_SV_COLUMN)}
    };
  }
}
