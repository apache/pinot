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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;
import org.roaringbitmap.RoaringBitmap;
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
    boolean[] predicateResults = new boolean[NUM_ROWS];
    Arrays.fill(predicateResults, true);
    testCaseQueries("true", predicateResults);
    Arrays.fill(predicateResults, false);
    testCaseQueries("false", predicateResults);

    for (TransformFunctionType functionType : BINARY_OPERATOR_TRANSFORM_FUNCTIONS) {
      testCaseQueries(String.format("%s(%s, %s)", functionType.getName(), INT_SV_COLUMN,
          String.format("%d", _intSVValues[INDEX_TO_COMPARE])), getPredicateResults(INT_SV_COLUMN, functionType));
      testCaseQueries(String.format("%s(%s, %s)", functionType.getName(), LONG_SV_COLUMN,
          String.format("%d", _longSVValues[INDEX_TO_COMPARE])), getPredicateResults(LONG_SV_COLUMN, functionType));
      testCaseQueries(String.format("%s(%s, %s)", functionType.getName(), FLOAT_SV_COLUMN,
          String.format("%f", _floatSVValues[INDEX_TO_COMPARE])), getPredicateResults(FLOAT_SV_COLUMN, functionType));
      testCaseQueries(String.format("%s(%s, %s)", functionType.getName(), DOUBLE_SV_COLUMN,
              String.format("%.20f", _doubleSVValues[INDEX_TO_COMPARE])),
          getPredicateResults(DOUBLE_SV_COLUMN, functionType));
      testCaseQueries(String.format("%s(%s, %s)", functionType.getName(), STRING_SV_COLUMN,
              String.format("'%s'", _stringSVValues[INDEX_TO_COMPARE])),
          getPredicateResults(STRING_SV_COLUMN, functionType));
    }
  }

  @DataProvider
  public static String[] illegalExpressions() {
    //@formatter:off
    return new String[] {
        // '10.0' cannot be parsed as INT/LONG/TIMESTAMP/BYTES
        String.format("CASE WHEN true THEN %s ELSE '10.0' END", INT_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE '10.0' END", LONG_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE '10.0' END", TIMESTAMP_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE '10.0' END", BYTES_SV_COLUMN),
        // 'abc' cannot be parsed as any type other than STRING
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", INT_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", LONG_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", FLOAT_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", DOUBLE_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", BIG_DECIMAL_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", TIMESTAMP_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE 'abc' END", BYTES_SV_COLUMN),
        // Cannot mix 2 types that are not both numeric
        String.format("CASE WHEN true THEN %s ELSE %s END", INT_SV_COLUMN, TIMESTAMP_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE %s END", INT_SV_COLUMN, STRING_SV_COLUMN),
        String.format("CASE WHEN true THEN %s ELSE %s END", INT_SV_COLUMN, BYTES_SV_COLUMN),
        String.format("CASE WHEN true THEN 100 ELSE %s END", TIMESTAMP_COLUMN),
        String.format("CASE WHEN true THEN 100 ELSE %s END", STRING_SV_COLUMN),
        String.format("CASE WHEN true THEN 100 ELSE %s END", BYTES_SV_COLUMN)
    };
    //@formatter:on
  }

  @Test(dataProvider = "illegalExpressions", expectedExceptions = Exception.class)
  public void testInvalidCaseTransformFunction(String expression) {
    TransformFunctionFactory.get(RequestContextUtils.getExpression(expression), _dataSourceMap);
  }

  @Test
  public void testCaseTransformationWithNullColumn() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("CASE WHEN %s IS NULL THEN 'aaa' ELSE 'bbb' END", STRING_ALPHANUM_NULL_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.getNullHandlingEnabled(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "case");
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);

    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedValues[i] = "aaa";
      } else {
        expectedValues[i] = "bbb";
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, new RoaringBitmap());
  }

  @Test
  public void testCaseTransformationWithNullThenClause() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("CASE WHEN %s IS NULL THEN NULL ELSE 'bbb' END", STRING_ALPHANUM_NULL_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.getNullHandlingEnabled(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "case");
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    String[] expectedValues = new String[NUM_ROWS];
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        bitmap.add(i);
      } else {
        expectedValues[i] = "bbb";
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  @Test
  public void testCaseTransformationWithNullElseClause() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("CASE WHEN %s IS NULL THEN 'aaa' END", STRING_ALPHANUM_NULL_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.getNullHandlingEnabled(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "case");
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);

    String[] expectedValues = new String[NUM_ROWS];
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedValues[i] = "aaa";
      } else {
        bitmap.add(i);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, bitmap);
  }

  private void testCaseQueries(String predicate, boolean[] predicateResults) {
    // INT
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 10 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, INT_SV_COLUMN));
      int[] expectedValues = new int[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _intSVValues[i] : 10;
      }
      testCaseQueryWithIntResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN 100 ELSE %s END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100' ELSE %s END", predicate, INT_SV_COLUMN));
      int[] expectedValues = new int[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? 100 : _intSVValues[i];
      }
      testCaseQueryWithIntResults(expressions, expectedValues);
    }

    // LONG
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 10 END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, LONG_SV_COLUMN));
      long[] expectedValues = new long[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _longSVValues[i] : 10L;
      }
      testCaseQueryWithLongResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN 100 ELSE %s END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100' ELSE %s END", predicate, LONG_SV_COLUMN));
      long[] expectedValues = new long[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? 100L : _longSVValues[i];
      }
      testCaseQueryWithLongResults(expressions, expectedValues);
    }
    // Cast INT to LONG
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS LONG) ELSE 10 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS LONG) ELSE '10' END", predicate, INT_SV_COLUMN));
      long[] expectedValues = new long[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _intSVValues[i] : 10L;
      }
      testCaseQueryWithLongResults(expressions, expectedValues);
    }
    // Literal upcast INT to LONG
    {
      List<String> expressions = Collections.singletonList(
          String.format("CASE WHEN %s THEN %s ELSE %d END", predicate, INT_SV_COLUMN, 10L + Integer.MAX_VALUE));
      long[] expectedValues = new long[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _intSVValues[i] : 10L + Integer.MAX_VALUE;
      }
      testCaseQueryWithLongResults(expressions, expectedValues);
    }

    // FLOAT
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 10 END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10.0' END", predicate, FLOAT_SV_COLUMN));
      float[] expectedValues = new float[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _floatSVValues[i] : 10.0f;
      }
      testCaseQueryWithFloatResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN 100 ELSE %s END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100' ELSE %s END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100.0' ELSE %s END", predicate, FLOAT_SV_COLUMN));
      float[] expectedValues = new float[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? 100.0f : _floatSVValues[i];
      }
      testCaseQueryWithFloatResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN %s ELSE '1.23' END", predicate, FLOAT_SV_COLUMN));
      float[] expectedValues = new float[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _floatSVValues[i] : 1.23f;
      }
      testCaseQueryWithFloatResults(expressions, expectedValues);
    }
    // Cast INT/LONG to FLOAT
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS FLOAT) ELSE 10 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS FLOAT) ELSE '10' END", predicate, INT_SV_COLUMN));
      float[] expectedValues = new float[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _intSVValues[i] : 10.0f;
      }
      testCaseQueryWithFloatResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS FLOAT) ELSE 10 END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS FLOAT) ELSE '10' END", predicate, LONG_SV_COLUMN));
      float[] expectedValues = new float[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _longSVValues[i] : 10.0f;
      }
      testCaseQueryWithFloatResults(expressions, expectedValues);
    }

    // DOUBLE
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 10 END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE 10.0 END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10.0' END", predicate, DOUBLE_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _doubleSVValues[i] : 10.0;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN 100 ELSE %s END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN 100.0 ELSE %s END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100' ELSE %s END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100.0' ELSE %s END", predicate, DOUBLE_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? 100.0 : _doubleSVValues[i];
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 1.23 END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '1.23' END", predicate, DOUBLE_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _doubleSVValues[i] : 1.23;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    // Cast INT/LONG/FLOAT to DOUBLE
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE 10 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE 10.0 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE '10' END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE '10.0' END", predicate, INT_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _intSVValues[i] : 10.0;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE 10 END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE 10.0 END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE '10' END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE '10.0' END", predicate, LONG_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _longSVValues[i] : 10.0;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE 10 END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE 10.0 END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE '10' END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DOUBLE) ELSE '10.0' END", predicate, FLOAT_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _floatSVValues[i] : 10.0;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    // Literal upcast INT/LONG/FLOAT to DOUBLE
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN %s ELSE 1.23 END", predicate, INT_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _intSVValues[i] : 1.23;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN %s ELSE 1.23 END", predicate, LONG_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _longSVValues[i] : 1.23;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN %s ELSE 1.23 END", predicate, FLOAT_SV_COLUMN));
      double[] expectedValues = new double[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _floatSVValues[i] : 1.23;
      }
      testCaseQueryWithDoubleResults(expressions, expectedValues);
    }

    // BIG_DECIMAL
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 10 END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE 10.0 END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '10.0' END", predicate, BIG_DECIMAL_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _bigDecimalSVValues[i] : BigDecimal.TEN;
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN 100 ELSE %s END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN 100.0 ELSE %s END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100' ELSE %s END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN '100.0' ELSE %s END", predicate, BIG_DECIMAL_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? BigDecimal.valueOf(100) : _bigDecimalSVValues[i];
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE 1.23 END", predicate, BIG_DECIMAL_SV_COLUMN),
              String.format("CASE WHEN %s THEN %s ELSE '1.23' END", predicate, BIG_DECIMAL_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _bigDecimalSVValues[i] : new BigDecimal("1.23");
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }
    // Cast INT/LONG/FLOAT/DOUBLE to BIG_DECIMAL
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10.0 END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10' END", predicate, INT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10.0' END", predicate, INT_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? BigDecimal.valueOf(_intSVValues[i]) : BigDecimal.TEN;
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10 END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10.0 END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10' END", predicate, LONG_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10.0' END", predicate, LONG_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? BigDecimal.valueOf(_longSVValues[i]) : BigDecimal.TEN;
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10 END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10.0 END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10' END", predicate, FLOAT_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10.0' END", predicate, FLOAT_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? BigDecimal.valueOf(_floatSVValues[i]) : BigDecimal.TEN;
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10 END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE 10.0 END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10' END", predicate, DOUBLE_SV_COLUMN),
              String.format("CASE WHEN %s THEN CAST(%s AS DECIMAL) ELSE '10.0' END", predicate, DOUBLE_SV_COLUMN));
      BigDecimal[] expectedValues = new BigDecimal[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? BigDecimal.valueOf(_doubleSVValues[i]) : BigDecimal.TEN;
      }
      testCaseQueryWithBigDecimalResults(expressions, expectedValues);
    }

    // STRING
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, STRING_SV_COLUMN));
      String[] expectedValues = new String[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _stringSVValues[i] : "10";
      }
      testCaseQueryWithStringResults(expressions, expectedValues);
    }
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN '100' ELSE %s END", predicate, STRING_SV_COLUMN));
      String[] expectedValues = new String[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? "100" : _stringSVValues[i];
      }
      testCaseQueryWithStringResults(expressions, expectedValues);
    }
    // Cast INT to STRING
    {
      List<String> expressions = Collections.singletonList(
          String.format("CASE WHEN %s THEN CAST(%s AS STRING) ELSE '10' END", predicate, INT_SV_COLUMN));
      String[] expectedValues = new String[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? Integer.toString(_intSVValues[i]) : "10";
      }
      testCaseQueryWithStringResults(expressions, expectedValues);
    }

    // BYTES
    {
      List<String> expressions =
          Collections.singletonList(String.format("CASE WHEN %s THEN %s ELSE '10' END", predicate, BYTES_SV_COLUMN));
      byte[][] expectedValues = new byte[NUM_ROWS][];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _bytesSVValues[i] : BytesUtils.toBytes("10");
      }
      testCaseQueryWithBytesResults(expressions, expectedValues);
    }

    // TIMESTAMP
    {
      long currentTimeMs = System.currentTimeMillis();
      String timestamp = new Timestamp(currentTimeMs).toString();
      List<String> expressions =
          Arrays.asList(String.format("CASE WHEN %s THEN %s ELSE '%s' END", predicate, TIMESTAMP_COLUMN, timestamp),
              String.format("CASE WHEN %s THEN %s ELSE '%d' END", predicate, TIMESTAMP_COLUMN, currentTimeMs));
      long[] expectedValues = new long[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _timeValues[i] : currentTimeMs;
      }
      testCaseQueryWithTimestampResults(expressions, expectedValues);
    }
    // Cast LONG to TIMESTAMP
    {
      long currentTimeMs = System.currentTimeMillis();
      String timestamp = new Timestamp(currentTimeMs).toString();
      List<String> expressions = Arrays.asList(
          String.format("CASE WHEN %s THEN CAST(%s AS TIMESTAMP) ELSE '%s' END", predicate, LONG_SV_COLUMN, timestamp),
          String.format("CASE WHEN %s THEN CAST(%s AS TIMESTAMP) ELSE '%d' END", predicate, LONG_SV_COLUMN,
              currentTimeMs));
      long[] expectedValues = new long[NUM_ROWS];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = predicateResults[i] ? _longSVValues[i] : currentTimeMs;
      }
      testCaseQueryWithTimestampResults(expressions, expectedValues);
    }
  }

  private void testCaseQueryWithIntResults(List<String> expressions, int[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithLongResults(List<String> expressions, long[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.LONG);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithFloatResults(List<String> expressions, float[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.FLOAT);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithDoubleResults(List<String> expressions, double[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.DOUBLE);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithBigDecimalResults(List<String> expressions, BigDecimal[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.BIG_DECIMAL);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithStringResults(List<String> expressions, String[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithBytesResults(List<String> expressions, byte[][] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.BYTES);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private void testCaseQueryWithTimestampResults(List<String> expressions, long[] expectedValues) {
    for (String expression : expressions) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
      TransformFunction transformFunction = TransformFunctionFactory.get(expressionContext, _dataSourceMap);
      Assert.assertTrue(transformFunction instanceof CaseTransformFunction);
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.TIMESTAMP);
      testTransformFunction(transformFunction, expectedValues);
    }
  }

  private boolean[] getPredicateResults(String column, TransformFunctionType type) {
    boolean[] results = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      switch (column) {
        case INT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              results[i] = _intSVValues[i] == _intSVValues[INDEX_TO_COMPARE];
              break;
            case NOT_EQUALS:
              results[i] = _intSVValues[i] != _intSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN:
              results[i] = _intSVValues[i] > _intSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN_OR_EQUAL:
              results[i] = _intSVValues[i] >= _intSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN:
              results[i] = _intSVValues[i] < _intSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN_OR_EQUAL:
              results[i] = _intSVValues[i] <= _intSVValues[INDEX_TO_COMPARE];
              break;
            default:
              throw new IllegalStateException();
          }
          break;
        case LONG_SV_COLUMN:
          switch (type) {
            case EQUALS:
              results[i] = _longSVValues[i] == _longSVValues[INDEX_TO_COMPARE];
              break;
            case NOT_EQUALS:
              results[i] = _longSVValues[i] != _longSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN:
              results[i] = _longSVValues[i] > _longSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN_OR_EQUAL:
              results[i] = _longSVValues[i] >= _longSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN:
              results[i] = _longSVValues[i] < _longSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN_OR_EQUAL:
              results[i] = _longSVValues[i] <= _longSVValues[INDEX_TO_COMPARE];
              break;
            default:
              throw new IllegalStateException();
          }
          break;
        case FLOAT_SV_COLUMN:
          switch (type) {
            case EQUALS:
              results[i] = _floatSVValues[i] == _floatSVValues[INDEX_TO_COMPARE];
              break;
            case NOT_EQUALS:
              results[i] = _floatSVValues[i] != _floatSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN:
              results[i] = _floatSVValues[i] > _floatSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN_OR_EQUAL:
              results[i] = _floatSVValues[i] >= _floatSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN:
              results[i] = _floatSVValues[i] < _floatSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN_OR_EQUAL:
              results[i] = _floatSVValues[i] <= _floatSVValues[INDEX_TO_COMPARE];
              break;
            default:
              throw new IllegalStateException();
          }
          break;
        case DOUBLE_SV_COLUMN:
          switch (type) {
            case EQUALS:
              results[i] = _doubleSVValues[i] == _doubleSVValues[INDEX_TO_COMPARE];
              break;
            case NOT_EQUALS:
              results[i] = _doubleSVValues[i] != _doubleSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN:
              results[i] = _doubleSVValues[i] > _doubleSVValues[INDEX_TO_COMPARE];
              break;
            case GREATER_THAN_OR_EQUAL:
              results[i] = _doubleSVValues[i] >= _doubleSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN:
              results[i] = _doubleSVValues[i] < _doubleSVValues[INDEX_TO_COMPARE];
              break;
            case LESS_THAN_OR_EQUAL:
              results[i] = _doubleSVValues[i] <= _doubleSVValues[INDEX_TO_COMPARE];
              break;
            default:
              throw new IllegalStateException();
          }
          break;
        case STRING_SV_COLUMN:
          switch (type) {
            case EQUALS:
              results[i] = _stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) == 0;
              break;
            case NOT_EQUALS:
              results[i] = _stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) != 0;
              break;
            case GREATER_THAN:
              results[i] = _stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) > 0;
              break;
            case GREATER_THAN_OR_EQUAL:
              results[i] = _stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) >= 0;
              break;
            case LESS_THAN:
              results[i] = _stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) < 0;
              break;
            case LESS_THAN_OR_EQUAL:
              results[i] = _stringSVValues[i].compareTo(_stringSVValues[INDEX_TO_COMPARE]) <= 0;
              break;
            default:
              throw new IllegalStateException();
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return results;
  }
}
