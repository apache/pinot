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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class CaseTransformFunctionTest extends BaseTransformFunctionTest {
  private static final int INDEX_TO_COMPARE = new Random(System.currentTimeMillis()).nextInt(NUM_ROWS);
  private static final TransformFunctionType[] BINARY_OPERATOR_TRANSFORM_FUNCTIONS = new TransformFunctionType[]{
      TransformFunctionType.EQUALS, TransformFunctionType.NOT_EQUALS, TransformFunctionType.GREATER_THAN,
      TransformFunctionType.GREATER_THAN_OR_EQUAL, TransformFunctionType.LESS_THAN,
      TransformFunctionType.LESS_THAN_OR_EQUAL
  };

  @DataProvider
  public Object[][] params() {
    return Stream.of(INT_SV_COLUMN, LONG_SV_COLUMN, FLOAT_SV_COLUMN, DOUBLE_SV_COLUMN).flatMap(
            col -> Stream.of(new int[]{3, 2, 1}, new int[]{1, 2, 3},
                    new int[]{Integer.MAX_VALUE / 2, Integer.MAX_VALUE / 4, 0},
                    new int[]{0, Integer.MAX_VALUE / 4, Integer.MAX_VALUE / 2},
                    new int[]{0, Integer.MIN_VALUE / 4, Integer.MIN_VALUE}, new int[]{Integer.MIN_VALUE, 0, 1},
                    new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE, 1},
                    new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 2})
                .map(thresholds -> new Object[]{col, thresholds[0], thresholds[1], thresholds[2]}))
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "params")
  public void testCasePriorityObserved(String column, int threshold1, int threshold2, int threshold3) {
    String statement =
        String.format("CASE WHEN %s > %d THEN 3 WHEN %s > %d THEN 2 WHEN %s > %d THEN 1 ELSE -1 END", column,
            threshold1, column, threshold2, column, threshold3);
    ExpressionContext expression = RequestContextUtils.getExpression(statement);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedIntResults = new int[NUM_ROWS];
    for (int i = 0; i < expectedIntResults.length; i++) {
      switch (column) {
        case INT_SV_COLUMN:
          expectedIntResults[i] = _intSVValues[i] > threshold1 ? 3
              : _intSVValues[i] > threshold2 ? 2 : _intSVValues[i] > threshold3 ? 1 : -1;
          break;
        case LONG_SV_COLUMN:
          expectedIntResults[i] = _longSVValues[i] > threshold1 ? 3
              : _longSVValues[i] > threshold2 ? 2 : _longSVValues[i] > threshold3 ? 1 : -1;
          break;
        case FLOAT_SV_COLUMN:
          expectedIntResults[i] = _floatSVValues[i] > threshold1 ? 3
              : _floatSVValues[i] > threshold2 ? 2 : _floatSVValues[i] > threshold3 ? 1 : -1;
          break;
        case DOUBLE_SV_COLUMN:
          expectedIntResults[i] = _doubleSVValues[i] > threshold1 ? 3
              : _doubleSVValues[i] > threshold2 ? 2 : _doubleSVValues[i] > threshold3 ? 1 : -1;
          break;
        default:
      }
    }
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    assertEquals(expectedIntResults, intValues);
  }

  @Test
  public void testCaseTransformFunctionWithIntResults() {
    int[] expectedIntResults = new int[NUM_ROWS];
    Arrays.fill(expectedIntResults, 100);
    testCaseQueryWithIntResults("true", expectedIntResults);
    Arrays.fill(expectedIntResults, 10);
    testCaseQueryWithIntResults("false", expectedIntResults);

    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
          String.format("%d", _intSVValues[INDEX_TO_COMPARE])), getExpectedIntResults(INT_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
          String.format("%d", _longSVValues[INDEX_TO_COMPARE])), getExpectedIntResults(LONG_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
              String.format("%f", _floatSVValues[INDEX_TO_COMPARE])),
          getExpectedIntResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
              String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getExpectedIntResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueryWithIntResults(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
              String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getExpectedIntResults(STRING_SV_COLUMN, functionType));
    }
  }

  @Test
  public void testCaseTransformFunctionWithFloatResults() {
    float[] expectedFloatResults = new float[NUM_ROWS];
    Arrays.fill(expectedFloatResults, 100);
    testCaseQueryWithFloatResults("true", expectedFloatResults);
    Arrays.fill(expectedFloatResults, 10);
    testCaseQueryWithFloatResults("false", expectedFloatResults);

    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueryWithFloatResults(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
          String.format("%d", _intSVValues[INDEX_TO_COMPARE])), getExpectedFloatResults(INT_SV_COLUMN, functionType));
      testCaseQueryWithFloatResults(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
              String.format("%d", _longSVValues[INDEX_TO_COMPARE])),
          getExpectedFloatResults(LONG_SV_COLUMN, functionType));
      testCaseQueryWithFloatResults(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
              String.format("%f", _floatSVValues[INDEX_TO_COMPARE])),
          getExpectedFloatResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueryWithFloatResults(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
              String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getExpectedFloatResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueryWithFloatResults(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
              String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getExpectedFloatResults(STRING_SV_COLUMN, functionType));
    }
  }

  @Test
  public void testCaseTransformFunctionWithBigDecimalResults() {
    BigDecimal val1 = new BigDecimal("100.99887766554433221");
    BigDecimal val2 = new BigDecimal("10.1122334455667788909");
    BigDecimal[] expectedBigDecimalResults = new BigDecimal[NUM_ROWS];
    Arrays.fill(expectedBigDecimalResults, val1);
    testCaseQueryWithBigDecimalResults("true", expectedBigDecimalResults);
    Arrays.fill(expectedBigDecimalResults, val2);
    testCaseQueryWithBigDecimalResults("false", expectedBigDecimalResults);

    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueryWithBigDecimalResults(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
              String.format("%d", _intSVValues[INDEX_TO_COMPARE])),
          getExpectedBigDecimalResults(INT_SV_COLUMN, functionType));
      testCaseQueryWithBigDecimalResults(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
              String.format("%d", _longSVValues[INDEX_TO_COMPARE])),
          getExpectedBigDecimalResults(LONG_SV_COLUMN, functionType));
      testCaseQueryWithBigDecimalResults(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
              String.format("%f", _floatSVValues[INDEX_TO_COMPARE])),
          getExpectedBigDecimalResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueryWithBigDecimalResults(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
              String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getExpectedBigDecimalResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueryWithBigDecimalResults(String.format("%s(%s, %s)", functionType.getName(), BIG_DECIMAL_SV_COLUMN,
              String.format("'%s'", _bigDecimalSVValues[INDEX_TO_COMPARE].toPlainString())),
          getExpectedBigDecimalResults(BIG_DECIMAL_SV_COLUMN, functionType));
      testCaseQueryWithBigDecimalResults(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
              String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getExpectedBigDecimalResults(STRING_SV_COLUMN, functionType));
    }
  }

  @Test
  public void testCaseTransformFunctionWithStringResults() {
    String[] expectedStringResults = new String[NUM_ROWS];
    Arrays.fill(expectedStringResults, "aaa");
    testCaseQueryWithStringResults("true", expectedStringResults);
    Arrays.fill(expectedStringResults, "bbb");
    testCaseQueryWithStringResults("false", expectedStringResults);

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
        RequestContextUtils.getExpression(String.format("CASE WHEN %s THEN 100 ELSE 10 END", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    testTransformFunction(transformFunction, expectedValues);
  }

  private void testCaseQueryWithFloatResults(String predicate, float[] expectedValues) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CASE WHEN %s THEN 100.0 ELSE 10.0 END", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.FLOAT);
    testTransformFunction(transformFunction, expectedValues);
  }

  private void testCaseQueryWithBigDecimalResults(String predicate, BigDecimal[] expectedValues) {
    // Note: defining decimal literals within quotes preserves precision.
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("CASE WHEN %s THEN '100.99887766554433221' ELSE '10.1122334455667788909' END", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.BIG_DECIMAL);
    testTransformFunction(transformFunction, expectedValues);
  }

  private void testCaseQueryWithStringResults(String predicate, String[] expectedValues) {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("CASE WHEN %s THEN 'aaa' ELSE 'bbb' END", predicate));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    assertEquals(transformFunction.getName(), CaseTransformFunction.FUNCTION_NAME);
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    testTransformFunction(transformFunction, expectedValues);
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
        default:
          break;
      }
    }
    return result;
  }

  private float[] getExpectedFloatResults(String column, TransformFunctionType type) {
    float[] result = new float[NUM_ROWS];
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
        default:
          break;
      }
    }
    return result;
  }

  private BigDecimal[] getExpectedBigDecimalResults(String column, TransformFunctionType type) {
    BigDecimal[] result = new BigDecimal[NUM_ROWS];
    BigDecimal val1 = new BigDecimal("100.99887766554433221");
    BigDecimal val2 = new BigDecimal("10.1122334455667788909");
    for (int i = 0; i < NUM_ROWS; i++) {
      switch (column) {
        case INT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_intSVValues[i] == _intSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case NOT_EQUALS:
              result[i] = (_intSVValues[i] != _intSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN:
              result[i] = (_intSVValues[i] > _intSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] >= _intSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN:
              result[i] = (_intSVValues[i] < _intSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_intSVValues[i] <= _intSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case LONG_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_longSVValues[i] == _longSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case NOT_EQUALS:
              result[i] = (_longSVValues[i] != _longSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN:
              result[i] = (_longSVValues[i] > _longSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] >= _longSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN:
              result[i] = (_longSVValues[i] < _longSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_longSVValues[i] <= _longSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case FLOAT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_floatSVValues[i] == _floatSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case NOT_EQUALS:
              result[i] = (_floatSVValues[i] != _floatSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN:
              result[i] = (_floatSVValues[i] > _floatSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] >= _floatSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN:
              result[i] = (_floatSVValues[i] < _floatSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_floatSVValues[i] <= _floatSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case DOUBLE_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_doubleSVValues[i] == _doubleSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case NOT_EQUALS:
              result[i] = (_doubleSVValues[i] != _doubleSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN:
              result[i] = (_doubleSVValues[i] > _doubleSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] >= _doubleSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN:
              result[i] = (_doubleSVValues[i] < _doubleSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_doubleSVValues[i] <= _doubleSVValues[INDEX_TO_COMPARE]) ? val1 : val2;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case BIG_DECIMAL_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = _bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[INDEX_TO_COMPARE]) == 0 ? val1 : val2;
              break;
            case NOT_EQUALS:
              result[i] = _bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[INDEX_TO_COMPARE]) != 0 ? val1 : val2;
              break;
            case GREATER_THAN:
              result[i] = _bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[INDEX_TO_COMPARE]) > 0 ? val1 : val2;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = _bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[INDEX_TO_COMPARE]) >= 0 ? val1 : val2;
              break;
            case LESS_THAN:
              result[i] = _bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[INDEX_TO_COMPARE]) < 0 ? val1 : val2;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = _bigDecimalSVValues[i].compareTo(_bigDecimalSVValues[INDEX_TO_COMPARE]) <= 0 ? val1 : val2;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        case STRING_SV_COLUMN:
          switch (type) {
            case EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) == 0) ? val1 : val2;
              break;
            case NOT_EQUALS:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) != 0) ? val1 : val2;
              break;
            case GREATER_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) > 0) ? val1 : val2;
              break;
            case GREATER_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) >= 0) ? val1 : val2;
              break;
            case LESS_THAN:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) < 0) ? val1 : val2;
              break;
            case LESS_THAN_OR_EQUAL:
              result[i] = (_stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) <= 0) ? val1 : val2;
              break;
            default:
              throw new IllegalStateException("Not supported type - " + type);
          }
          break;
        default:
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
        default:
          break;
      }
    }
    return result;
  }
}
