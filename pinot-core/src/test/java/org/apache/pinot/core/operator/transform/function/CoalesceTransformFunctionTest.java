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

import org.apache.pinot.common.request.context.RequestContextUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CoalesceTransformFunctionTest extends BaseTransformFunctionTest {
  @Test
  public void testCoalesceIntColumns() {
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", INT_SV_NULL_COLUMN, LONG_SV_COLUMN)),
        _dataSourceMap);

    long[] expectedResults = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedResults[i] = _longSVValues[i];
      } else {
        expectedResults[i] = _intSVValues[i];
      }
    }
    testTransformFunction(coalesceFunc, expectedResults);
  }

  @Test
  public void testCoalesceIntColumnsAndLiterals() {
    final int intLiteral = 313;
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", INT_SV_NULL_COLUMN, intLiteral)),
        _dataSourceMap);
    Assert.assertEquals(coalesceFunc.getName(), "coalesce");
    int[] expectedResults = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedResults[i] = intLiteral;
      } else {
        expectedResults[i] = _intSVValues[i];
      }
    }
    testTransformFunction(coalesceFunc, expectedResults);
  }

  @Test
  public void testDifferentLiteralArgs() {
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, '%s')", STRING_SV_NULL_COLUMN, 234)),
        _dataSourceMap);
    String[] expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedResults[i] = "234";
      } else {
        expectedResults[i] = _stringSVValues[i];
      }
    }
    testTransformFunction(coalesceFunc, expectedResults);
  }


  @Test
  public void testCoalesceNullLiteral() {
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE((1 + null), %s)", INT_SV_NULL_COLUMN)),
        _dataSourceMap);
    double[] expectedResults = new double[NUM_ROWS];
    RoaringBitmap expectedNull = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNull.add(i);
      } else {
        expectedResults[i] = _intSVValues[i];
      }
    }
    testTransformFunctionWithNull(coalesceFunc, expectedResults, expectedNull);
  }

  @Test
  public void testCoalesceNullNullLiteral() {
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression("COALESCE(null, null)"), _dataSourceMap);
    double[] expectedResults = new double[NUM_ROWS];
    RoaringBitmap expectedNull = new RoaringBitmap();
    expectedNull.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(coalesceFunc, expectedResults, expectedNull);
  }
}
