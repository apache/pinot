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
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ScalarTransformFunctionWrapperTest extends BaseTransformFunctionTest {

  @Test
  public void testStringLowerTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("lower(%s)", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "lower");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].toLowerCase();
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringUpperTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("UPPER(%s)", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "upper");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].toUpperCase();
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringReverseTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("rEvErSe(%s)", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "reverse");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = new StringBuilder(_stringAlphaNumericSVValues[i]).reverse().toString();
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringSubStrTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("sub_str(%s, 0, 2)", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "substr");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].substring(0, 2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        QueryContextConverterUtils.getExpression(String.format("substr(%s, '2', '-1')", STRING_ALPHANUM_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "substr");
    expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].substring(2);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringConcatTransformFunction() {
    ExpressionContext expression = QueryContextConverterUtils
        .getExpression(String.format("concat(%s, %s, '-')", STRING_ALPHANUM_SV_COLUMN, STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "concat");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i] + "-" + _stringAlphaNumericSVValues[i];
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringReplaceTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("replace(%s, 'A', 'B')", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "replace");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].replaceAll("A", "B");
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringPadTransformFunction() {
    int padLength = 50;
    String padString = "#";
    ExpressionContext expression = QueryContextConverterUtils
        .getExpression(String.format("lpad(%s, %d, '%s')", STRING_ALPHANUM_SV_COLUMN, padLength, padString));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "lpad");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = StringUtils.leftPad(_stringAlphaNumericSVValues[i], padLength, padString);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = QueryContextConverterUtils
        .getExpression(String.format("rpad(%s, %d, '%s')", STRING_ALPHANUM_SV_COLUMN, padLength, padString));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "rpad");
    expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = StringUtils.rightPad(_stringAlphaNumericSVValues[i], padLength, padString);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testStringTrimTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("ltrim(lpad(%s, 50, ' '))", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "ltrim");
    testTransformFunction(transformFunction, _stringAlphaNumericSVValues);

    expression =
        QueryContextConverterUtils.getExpression(String.format("rtrim(rpad(%s, 50, ' '))", STRING_ALPHANUM_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "rtrim");
    testTransformFunction(transformFunction, _stringAlphaNumericSVValues);

    expression = QueryContextConverterUtils
        .getExpression(String.format("trim(rpad(lpad(%s, 50, ' '), 100, ' '))", STRING_ALPHANUM_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    Assert.assertEquals(transformFunction.getName(), "trim");
    testTransformFunction(transformFunction, _stringAlphaNumericSVValues);
  }
}
