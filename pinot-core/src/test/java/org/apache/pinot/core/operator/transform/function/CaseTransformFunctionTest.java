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

import java.util.Random;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CaseTransformFunctionTest extends BaseTransformFunctionTest {

  private final int INDEX_TO_COMPARE = new Random(System.currentTimeMillis()).nextInt(NUM_ROWS);
  private final TransformFunctionType[] BINARY_OPERATOR_TRANSFORM_FUNCTIONS =
      new TransformFunctionType[]{TransformFunctionType.EQUALS, TransformFunctionType.NOT_EQUALS, TransformFunctionType.GREATER_THAN, TransformFunctionType.GREATER_THAN_OR_EQUAL, TransformFunctionType.LESS_THAN, TransformFunctionType.LESS_THAN_OR_EQUAL};

  @Test
  public void testCaseTransformFunctionWithIntResults() {
    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
          String.format("%d", _intSVValues[INDEX_TO_COMPARE])), getExpectedIntResults(INT_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
          String.format("%d", _longSVValues[INDEX_TO_COMPARE])), getExpectedIntResults(LONG_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
          String.format("%f", _floatSVValues[INDEX_TO_COMPARE])), getExpectedIntResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
          String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getExpectedIntResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
          String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getExpectedIntResults(STRING_SV_COLUMN, functionType));
    }
  }

  @Test
  public void testCaseTransformFunctionWithDoubleResults() {
    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueryWithDoubleResults(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
          String.format("%d", _intSVValues[INDEX_TO_COMPARE])), getExpectedDoubleResults(INT_SV_COLUMN, functionType));
      testCaseQueryWithDoubleResults(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
          String.format("%d", _longSVValues[INDEX_TO_COMPARE])),
          getExpectedDoubleResults(LONG_SV_COLUMN, functionType));
      testCaseQueryWithDoubleResults(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
          String.format("%f", _floatSVValues[INDEX_TO_COMPARE])),
          getExpectedDoubleResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueryWithDoubleResults(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
          String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getExpectedDoubleResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueryWithDoubleResults(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
          String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getExpectedDoubleResults(STRING_SV_COLUMN, functionType));
    }
  }

  @Test
  public void testCaseTransformFunctionWithStringResults() {
    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueryWithStringResults(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
          String.format("%d", _intSVValues[INDEX_TO_COMPARE])), getExpectedStringResults(INT_SV_COLUMN, functionType));
      testCaseQueryWithStringResults(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
          String.format("%d", _longSVValues[INDEX_TO_COMPARE])),
          getExpectedStringResults(LONG_SV_COLUMN, functionType));
      testCaseQueryWithStringResults(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
          String.format("%f", _floatSVValues[INDEX_TO_COMPARE])),
          getExpectedStringResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueryWithStringResults(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
          String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getExpectedStringResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueryWithStringResults(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
          String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getExpectedStringResults(STRING_SV_COLUMN, functionType));
    }
  }

  private void testCaseQueryWithIntResults(String predicate, int[] expectedValues) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CASE(%s, 100, 10)", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    testTransformFunction(transformFunction, expectedValues);
  }

  private void testCaseQueryWithDoubleResults(String predicate, double[] expectedValues) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CASE(%s, 100.0, 10.0)", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    testTransformFunction(transformFunction, expectedValues);
  }

  private void testCaseQueryWithStringResults(String predicate, String[] expectedValues) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CASE(%s, 'aaa', 'bbb')", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    Assert.assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{new Object[]{String.format("case(%s)", INT_SV_COLUMN)}, new Object[]{String.format(
        "case(%s, %s)", LONG_SV_COLUMN, 10)}};
  }

  private int[] getExpectedIntResults(String column, TransformFunctionType type) {
    int[] result = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      switch (column) {
        case INT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_intSVValues[i] == _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_intSVValues[i] != _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_intSVValues[i] > _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] >= _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_intSVValues[i] < _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] <= _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case LONG_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_longSVValues[i] == _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_longSVValues[i] != _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_longSVValues[i] > _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] >= _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_longSVValues[i] < _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] <= _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case FLOAT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_floatSVValues[i] == _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_floatSVValues[i] != _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_floatSVValues[i] > _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] >= _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_floatSVValues[i] < _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] <= _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case DOUBLE_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_doubleSVValues[i] == _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_doubleSVValues[i] != _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_doubleSVValues[i] > _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] >= _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_doubleSVValues[i] < _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] <= _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case STRING_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) == 0) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) != 0) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) > 0) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) >= 0) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) < 0) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) <= 0) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
      }
    }
    return result;
  }

  private double[] getExpectedDoubleResults(String column, TransformFunctionType type) {
    double[] result = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      switch (column) {
        case INT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_intSVValues[i] == _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_intSVValues[i] != _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_intSVValues[i] > _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] >= _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_intSVValues[i] < _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] <= _intSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case LONG_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_longSVValues[i] == _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_longSVValues[i] != _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_longSVValues[i] > _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] >= _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_longSVValues[i] < _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] <= _longSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case FLOAT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_floatSVValues[i] == _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_floatSVValues[i] != _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_floatSVValues[i] > _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] >= _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_floatSVValues[i] < _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] <= _floatSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case DOUBLE_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_doubleSVValues[i] == _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_doubleSVValues[i] != _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_doubleSVValues[i] > _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] >= _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_doubleSVValues[i] < _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] <= _doubleSVValues[INDEX_TO_COMPARE]) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case STRING_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) == 0) ? 100 : 10;
              break;
            case NOT_EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) != 0) ? 100 : 10;
              break;
            case GREATER_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) > 0) ? 100 : 10;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) >= 0) ? 100 : 10;
              break;
            case LESS_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) < 0) ? 100 : 10;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) <= 0) ? 100 : 10;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
      }
    }
    return result;
  }

  private String[] getExpectedStringResults(String column, TransformFunctionType type) {
    String[] result = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      switch (column) {
        case INT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_intSVValues[i] == _intSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case NOT_EQUALS:
              result[i] = (_intSVValues[i] != _intSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN:
              result[i] = (_intSVValues[i] > _intSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] >= _intSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN:
              result[i] = (_intSVValues[i] < _intSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] <= _intSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case LONG_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_longSVValues[i] == _longSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case NOT_EQUALS:
              result[i] = (_longSVValues[i] != _longSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN:
              result[i] = (_longSVValues[i] > _longSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] >= _longSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN:
              result[i] = (_longSVValues[i] < _longSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] <= _longSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case FLOAT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_floatSVValues[i] == _floatSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case NOT_EQUALS:
              result[i] = (_floatSVValues[i] != _floatSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN:
              result[i] = (_floatSVValues[i] > _floatSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] >= _floatSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN:
              result[i] = (_floatSVValues[i] < _floatSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] <= _floatSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case DOUBLE_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_doubleSVValues[i] == _doubleSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case NOT_EQUALS:
              result[i] = (_doubleSVValues[i] != _doubleSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN:
              result[i] = (_doubleSVValues[i] > _doubleSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] >= _doubleSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN:
              result[i] = (_doubleSVValues[i] < _doubleSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] <= _doubleSVValues[INDEX_TO_COMPARE]) ? "aaa" : "bbb";
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case STRING_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) == 0) ? "aaa" : "bbb";
              break;
            case NOT_EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) != 0) ? "aaa" : "bbb";
              break;
            case GREATER_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) > 0) ? "aaa" : "bbb";
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) >= 0) ? "aaa" : "bbb";
              break;
            case LESS_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) < 0) ? "aaa" : "bbb";
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) <= 0) ? "aaa" : "bbb";
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
      }
    }
    return result;
  }
}
