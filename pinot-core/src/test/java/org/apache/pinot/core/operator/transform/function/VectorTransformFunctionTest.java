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

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class VectorTransformFunctionTest extends BaseTransformFunctionTest {

  @Test(dataProvider = "testVectorTransformFunctionDataProvider")
  public void testVectorTransformFunction(String expressionStr, double lowerBound, double upperBound) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    double[] doubleValuesSV = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertTrue(doubleValuesSV[i] >= lowerBound, doubleValuesSV[i] + " < " + lowerBound);
      assertTrue(doubleValuesSV[i] <= upperBound, doubleValuesSV[i] + " > " + upperBound);
    }
  }

  @Test
  public void testVectorDimsTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression("vectorDims(vector1)");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] intValuesSV = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValuesSV[i], VECTOR_DIM_SIZE);
    }

    expression = RequestContextUtils.getExpression("vectorDims(vector2)");
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    intValuesSV = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValuesSV[i], VECTOR_DIM_SIZE);
    }
  }

  @DataProvider(name = "testVectorTransformFunctionDataProvider")
  public Object[][] testVectorTransformFunctionDataProvider() {
    String zeroVectorLiteral = "ARRAY[0.0"
        + StringUtils.repeat(",0.0", VECTOR_DIM_SIZE - 1)
        + "]";
    return new Object[][]{
        new Object[]{"cosineDistance(vector1, vector2)", 0.1, 0.4},
        new Object[]{"cosineDistance(vector1, vector2, 0)", 0.1, 0.4},
        new Object[]{"cosineDistance(vector1, zeroVector, 0)", 0.0, 0.0},
        new Object[]{"innerProduct(vector1, vector2)", 100, 160},
        new Object[]{"l1Distance(vector1, vector2)", 140, 210},
        new Object[]{"l2Distance(vector1, vector2)", 8, 11},
        new Object[]{"vectorNorm(vector1)", 10, 16},
        new Object[]{"vectorNorm(vector2)", 10, 16},

        new Object[]{String.format("cosineDistance(vector1, %s, 0)", zeroVectorLiteral), 0.0, 0.0},
        new Object[]{String.format("innerProduct(vector1, %s)", zeroVectorLiteral), 0.0, 0.0},
        new Object[]{String.format("l1Distance(vector1, %s)", zeroVectorLiteral), 0, VECTOR_DIM_SIZE},
        new Object[]{String.format("l2Distance(vector1, %s)", zeroVectorLiteral), 0, VECTOR_DIM_SIZE},
        new Object[]{String.format("vectorDims(%s)", zeroVectorLiteral), VECTOR_DIM_SIZE, VECTOR_DIM_SIZE},
        new Object[]{String.format("vectorNorm(%s)", zeroVectorLiteral), 0.0, 0.0},
    };
  }
}
