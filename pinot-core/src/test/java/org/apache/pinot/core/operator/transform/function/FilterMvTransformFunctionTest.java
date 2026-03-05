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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.function.IntPredicate;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.function.scalar.FilterMvScalarFunction;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FilterMvTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testFilterMvTransformFunctionInt() {
    assertFilterMvTransformFunctionInt("v > 5", value -> value > 5);
  }

  @Test(dataProvider = "filterMvIntPredicates")
  public void testFilterMvTransformFunctionIntPredicates(String predicate, IntPredicate matcher) {
    assertFilterMvTransformFunctionInt(predicate, matcher);
  }

  @DataProvider(name = "filterMvIntPredicates")
  public Object[][] provideFilterMvIntPredicates() {
    return new Object[][]{
        new Object[]{"v != 5", (IntPredicate) value -> value != 5},
        new Object[]{"v > 5", (IntPredicate) value -> value > 5},
        new Object[]{"v >= 5", (IntPredicate) value -> value >= 5},
        new Object[]{"v < 5", (IntPredicate) value -> value < 5},
        new Object[]{"v <= 5", (IntPredicate) value -> value <= 5},
        new Object[]{"v IN (1, 3, 5)", (IntPredicate) value -> value == 1 || value == 3 || value == 5},
        new Object[]{"v NOT IN (1, 3, 5)", (IntPredicate) value -> value != 1 && value != 3 && value != 5},
        new Object[]{"v BETWEEN 3 AND 7", (IntPredicate) value -> value >= 3 && value <= 7},
        new Object[]{"v > 3 AND v < 7", (IntPredicate) value -> value > 3 && value < 7},
        new Object[]{"v < 2 OR v > 9", (IntPredicate) value -> value < 2 || value > 9},
        new Object[]{"NOT (v = 1)", (IntPredicate) value -> value != 1}
    };
  }

  private void assertFilterMvTransformFunctionInt(String predicate, IntPredicate matcher) {
    String escaped = predicate.replace("'", "''");
    String expressionStr = String.format("filterMv(%s, '%s')", INT_MV_COLUMN, escaped);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof FilterMvTransformFunction);
    assertEquals(transformFunction.getName(), FilterMvTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.INT);
    assertFalse(resultMetadata.isSingleValue());
    assertTrue(resultMetadata.hasDictionary());

    int[][] dictIdsMV = transformFunction.transformToDictIdsMV(_projectionBlock);
    int[][] intValuesMV = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValuesMV = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValuesMV = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValuesMV = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);

    Dictionary dictionary = transformFunction.getDictionary();
    for (int i = 0; i < NUM_ROWS; i++) {
      IntList expectedList = new IntArrayList();
      for (int value : _intMVValues[i]) {
        if (matcher.test(value)) {
          expectedList.add(value);
        }
      }
      int[] expectedValues = expectedList.toIntArray();

      int numValues = expectedValues.length;
      assertEquals(dictIdsMV[i].length, numValues);
      assertEquals(intValuesMV[i].length, numValues);
      assertEquals(longValuesMV[i].length, numValues);
      assertEquals(floatValuesMV[i].length, numValues);
      assertEquals(doubleValuesMV[i].length, numValues);
      assertEquals(stringValuesMV[i].length, numValues);
      for (int j = 0; j < numValues; j++) {
        int expected = expectedValues[j];
        assertEquals(dictIdsMV[i][j], dictionary.indexOf(expected));
        assertEquals(intValuesMV[i][j], expected);
        assertEquals(longValuesMV[i][j], expected);
        assertEquals(floatValuesMV[i][j], (float) expected);
        assertEquals(doubleValuesMV[i][j], (double) expected);
        assertEquals(stringValuesMV[i][j], Integer.toString(expected));
      }
    }
  }

  @Test(dataProvider = "filterMvStringPredicates")
  public void testFilterMvTransformFunctionString(String predicate, boolean expectMatch) {
    String escaped = predicate.replace("'", "''");
    String expressionStr = String.format("filterMv(%s, '%s')", STRING_ALPHANUM_MV_COLUMN_2, escaped);
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof FilterMvTransformFunction);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.STRING);
    assertFalse(resultMetadata.isSingleValue());
    assertTrue(resultMetadata.hasDictionary());

    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);
    Dictionary dictionary = transformFunction.getDictionary();
    for (int i = 0; i < NUM_ROWS; i++) {
      int expectedLength = expectMatch ? _stringAlphaNumericMV2Values[i].length : 0;
      assertEquals(stringValuesMV[i].length, expectedLength);
      for (int j = 0; j < expectedLength; j++) {
        assertEquals(stringValuesMV[i][j], "a");
        assertEquals(dictionary.indexOf(stringValuesMV[i][j]), dictionary.indexOf("a"));
      }
    }
  }

  @DataProvider(name = "filterMvStringPredicates")
  public Object[][] provideFilterMvStringPredicates() {
    return new Object[][]{
        new Object[]{"v = 'a'", true},
        new Object[]{"v = 'b'", false},
        new Object[]{"REGEXP_LIKE(v, '^a$')", true},
        new Object[]{"REGEXP_LIKE(v, '^b$')", false}
    };
  }

  @Test
  public void testFilterMvScalarFunctionBytes() {
    // Test the scalar function BYTES overload directly since the base test class has no BYTES MV column
    byte[] val1 = BytesUtils.toBytes("aabb");
    byte[] val2 = BytesUtils.toBytes("ccdd");
    byte[] val3 = BytesUtils.toBytes("eeff");
    byte[][] values = new byte[][]{val1, val2, val3};

    // Positive match: filter to values equal to 'ccdd'
    byte[][] filtered = FilterMvScalarFunction.filterMv(values, "v = 'ccdd'");
    assertEquals(filtered.length, 1);
    assertEquals(filtered[0], val2);

    // Negative match: filter to values equal to non-existent value
    byte[][] filteredNone = FilterMvScalarFunction.filterMv(values, "v = '0000'");
    assertEquals(filteredNone.length, 0);

    // All match: always-true predicate returns original array
    byte[][] filteredAll = FilterMvScalarFunction.filterMv(values, "v != '0000'");
    assertEquals(filteredAll.length, 3);
  }

  @Test(dataProvider = "illegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "illegalArguments")
  public Object[][] provideIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("filterMv(%s)", INT_MV_COLUMN)},
        new Object[]{String.format("filterMv(%s, 'v > 0')", INT_SV_COLUMN)},
        new Object[]{String.format("filterMv(%s, %s)", INT_MV_COLUMN, LONG_MV_COLUMN)},
        new Object[]{String.format("filterMv(%s, '%s > 0')", INT_MV_COLUMN, LONG_MV_COLUMN)},
        new Object[]{String.format("filterMv(%s, 'abs(v) > 0')", INT_MV_COLUMN)}
    };
  }
}
