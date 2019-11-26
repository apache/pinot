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
import org.apache.pinot.core.operator.transform.function.SingleParamStringTransformFunction.LowerTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamStringTransformFunction.UpperTransformFunction;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.util.Locale.ENGLISH;


public class SingleParamStringTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testUpperTransformFunction() {
    TransformExpressionTree expression =
            TransformExpressionTree.compileToExpressionTree(String.format("upper(%s)", STRING_ALPHA_NUMERIC_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof UpperTransformFunction);
    Assert.assertEquals(transformFunction.getName(), UpperTransformFunction.FUNCTION_NAME);
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].toUpperCase(ENGLISH);
    }
    testStringAlphaNumericTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testLowerTransformFunction() {
    TransformExpressionTree expression =
            TransformExpressionTree.compileToExpressionTree(String.format("lower(%s)", STRING_ALPHA_NUMERIC_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LowerTransformFunction);
    Assert.assertEquals(transformFunction.getName(), LowerTransformFunction.FUNCTION_NAME);
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].toLowerCase(ENGLISH);
    }
    testStringAlphaNumericTransformFunction(transformFunction, expectedValues);
  }
}
