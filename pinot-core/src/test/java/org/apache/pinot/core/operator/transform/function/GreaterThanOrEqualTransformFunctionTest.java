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
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GreaterThanOrEqualTransformFunctionTest extends BinaryOperatorTransformFunctionTest {

  @Override
  boolean getExpectedValue(int compareResult) {
    return compareResult >= 0;
  }

  @Override
  String getFunctionName() {
    return new GreaterThanOrEqualTransformFunction().getName();
  }

  @Test
  public void testGreaterThanOrEqualNullLiteral() {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("greater_than_or_equal(null, %s)", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof GreaterThanOrEqualTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "greater_than_or_equal");
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testEqualsNullColumn() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("greater_than_or_equal(%s, %s)", INT_SV_NULL_COLUMN, INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof GreaterThanOrEqualTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "greater_than_or_equal");
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = 1;
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }
}
