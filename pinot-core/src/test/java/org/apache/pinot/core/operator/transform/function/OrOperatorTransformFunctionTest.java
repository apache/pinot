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

import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrOperatorTransformFunctionTest extends LogicalOperatorTransformFunctionTest {

  @Override
  boolean getExpectedValue(boolean left, boolean right) {
    return left || right;
  }

  @Override
  String getFunctionName() {
    return TransformFunctionType.OR.getName();
  }

  @Test
  public void testOrNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("or(%s,null)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof OrOperatorTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.OR.getName());
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (_intSVValues[i] != 0) {
        expectedValues[i] = 1;
      } else {
        roaringBitmap.add(i);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testOrNullColumn() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("or(%s,%s)", INT_SV_COLUMN, INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof OrOperatorTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.OR.getName());
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (_intSVValues[i] != 0) {
        expectedValues[i] = 1;
      } else if (isNullRow(i)) {
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = 0;
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }
}
