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

import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ArrayLengthTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testLengthTransformFunction() {
    TransformExpressionTree expression =
        TransformExpressionTree.compileToExpressionTree(String.format("arrayLength(%s)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ArrayLengthTransformFunction);
    Assert.assertEquals(transformFunction.getName(), ArrayLengthTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    Assert.assertTrue(transformFunction.getResultMetadata().isSingleValue());
    Assert.assertFalse(transformFunction.getResultMetadata().hasDictionary());

    int[] results = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(results[i], _intMVValues[i].length);
    }
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("arrayLength(%s,1)", INT_MV_COLUMN)},
        new Object[]{"arrayLength(2)"},
        new Object[]{String.format("arrayLength(%s)", LONG_SV_COLUMN)}};
  }
}
