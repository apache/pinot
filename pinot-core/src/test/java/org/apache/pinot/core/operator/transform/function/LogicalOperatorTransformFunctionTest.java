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
import org.apache.pinot.common.request.context.RequestContextConvertUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * LogicalOperatorTransformFunctionTest abstracts common test methods for:
 *     AndTransformFunctionTest
 *     OrTransformFunctionTest
 *
 */
public abstract class LogicalOperatorTransformFunctionTest extends BaseTransformFunctionTest {

  abstract int getExpectedValue(boolean arg1, boolean arg2);

  abstract String getFuncName();

  @Test
  public void testLogicalOperatorTransformFunction() {
    ExpressionContext intEqualsExpr =
        RequestContextConvertUtils.getExpression(String.format("EQUALS(%s, %d)", INT_SV_COLUMN, _intSVValues[0]));
    ExpressionContext longEqualsExpr =
        RequestContextConvertUtils.getExpression(String.format("EQUALS(%s, %d)", LONG_SV_COLUMN, _longSVValues[0]));
    ExpressionContext expression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, getFuncName(),
            Arrays.asList(intEqualsExpr, longEqualsExpr)));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), getFuncName().toLowerCase());
    int[] expectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntValues[i] = getExpectedValue(_intSVValues[i] == _intSVValues[0], _longSVValues[i] == _longSVValues[0]);
    }
    testTransformFunction(transformFunction, expectedIntValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String[] expressions) {
    List<ExpressionContext> expressionContextList = new ArrayList<>();
    for (int i = 0; i < expressions.length; i++) {
      expressionContextList.add(RequestContextConvertUtils.getExpression(expressions[i]));
    }
    TransformFunctionFactory.get(ExpressionContext
            .forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, getFuncName(), expressionContextList)),
        _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    String intEqualsExpr = String.format("EQUALS(%s, %d)", INT_SV_COLUMN, _intSVValues[0]);
    return new Object[][]{new Object[]{intEqualsExpr}, new Object[]{intEqualsExpr, STRING_SV_COLUMN}};
  }
}
