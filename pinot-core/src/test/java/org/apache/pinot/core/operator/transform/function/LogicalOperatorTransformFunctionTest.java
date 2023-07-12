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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * LogicalOperatorTransformFunctionTest abstracts common test methods for:
 *     AndTransformFunctionTest
 *     OrTransformFunctionTest
 *
 */
public abstract class LogicalOperatorTransformFunctionTest extends BaseTransformFunctionTest {

  abstract boolean getExpectedValue(boolean left, boolean right);

  abstract String getFunctionName();

  @Test
  public void testLogicalOperatorTransformFunction() {
    ExpressionContext intEqualsExpr =
        RequestContextUtils.getExpression(String.format("EQUALS(%s, %d)", INT_SV_COLUMN, _intSVValues[0]));
    ExpressionContext longEqualsExpr =
        RequestContextUtils.getExpression(String.format("EQUALS(%s, %d)", LONG_SV_COLUMN, _longSVValues[0]));
    String functionName = getFunctionName();
    ExpressionContext expression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, functionName,
            Arrays.asList(intEqualsExpr, longEqualsExpr)));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(transformFunction.getName(), functionName);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.BOOLEAN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    boolean[] expectedValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = getExpectedValue(_intSVValues[i] == _intSVValues[0], _longSVValues[i] == _longSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String[] expressions) {
    List<ExpressionContext> expressionContextList = new ArrayList<>();
    for (String expression : expressions) {
      expressionContextList.add(RequestContextUtils.getExpression(expression));
    }
    TransformFunctionFactory.get(ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, getFunctionName(), expressionContextList)), _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    String intEqualsExpr = String.format("EQUALS(%s, %d)", INT_SV_COLUMN, _intSVValues[0]);
    return new Object[][]{new Object[]{intEqualsExpr}, new Object[]{intEqualsExpr, STRING_SV_COLUMN}};
  }

  @Test
  public void testLogicalOperatorNullLiteral() {
    ExpressionContext intEqualsExpr =
        RequestContextUtils.getExpression(String.format("EQUALS(%s, null)", INT_SV_COLUMN));
    ExpressionContext longEqualsExpr =
        RequestContextUtils.getExpression(String.format("EQUALS(%s, null)", LONG_SV_COLUMN));
    String functionName = getFunctionName();
    ExpressionContext expression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, functionName,
            Arrays.asList(intEqualsExpr, longEqualsExpr)));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(transformFunction.getName(), functionName);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.BOOLEAN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    boolean[] expectedValues = new boolean[NUM_ROWS];
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, nullBitmap);
  }

  @Test
  public void testLogicalOperatorNullColumn() {
    ExpressionContext intEqualsExpr =
        RequestContextUtils.getExpression(String.format("EQUALS(%s, %s)", INT_SV_COLUMN, INT_SV_NULL_COLUMN));
    String functionName = getFunctionName();
    ExpressionContext expression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, functionName, Arrays.asList(intEqualsExpr, intEqualsExpr)));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertEquals(transformFunction.getName(), functionName);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.BOOLEAN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    boolean[] expectedValues = new boolean[NUM_ROWS];
    RoaringBitmap nullBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        nullBitmap.add(i);
      } else {
        expectedValues[i] = true;
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, nullBitmap);
  }
}
