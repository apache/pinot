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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.ArrayCopyUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ScalarTransformFunctionWrapperTest extends BaseTransformFunctionTest {

  @Test
  public void testStringLowerTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("lower(%s)", STRING_ALPHANUM_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "lower");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "upper");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "reverse");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "substr");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringAlphaNumericSVValues[i].substring(0, 2);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression =
        QueryContextConverterUtils.getExpression(String.format("substr(%s, '2', '-1')", STRING_ALPHANUM_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "substr");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "concat");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "replace");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "lpad");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = StringUtils.leftPad(_stringAlphaNumericSVValues[i], padLength, padString);
    }
    testTransformFunction(transformFunction, expectedValues);

    expression = QueryContextConverterUtils
        .getExpression(String.format("rpad(%s, %d, '%s')", STRING_ALPHANUM_SV_COLUMN, padLength, padString));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "rpad");
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
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "ltrim");
    testTransformFunction(transformFunction, _stringAlphaNumericSVValues);

    expression =
        QueryContextConverterUtils.getExpression(String.format("rtrim(rpad(%s, 50, ' '))", STRING_ALPHANUM_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "rtrim");
    testTransformFunction(transformFunction, _stringAlphaNumericSVValues);

    expression = QueryContextConverterUtils
        .getExpression(String.format("trim(rpad(lpad(%s, 50, ' '), 100, ' '))", STRING_ALPHANUM_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "trim");
    testTransformFunction(transformFunction, _stringAlphaNumericSVValues);
  }

  @Test
  public void testShaTransformFunction() {
    ExpressionContext expression = QueryContextConverterUtils.getExpression(String.format("sha(%s)", BYTES_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "sha");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = DigestUtils.shaHex(_bytesSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testSha256TransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("sha256(%s)", BYTES_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "sha256");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = DigestUtils.sha256Hex(_bytesSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testSha512TransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("sha512(%s)", BYTES_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "sha512");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = DigestUtils.sha512Hex(_bytesSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testMd5TransformFunction() {
    ExpressionContext expression = QueryContextConverterUtils.getExpression(String.format("md5(%s)", BYTES_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "md5");
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = DigestUtils.md5Hex(_bytesSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testArrayReverseIntTransformFunction() {
    {
      ExpressionContext expression =
          QueryContextConverterUtils.getExpression(String.format("array_reverse_int(%s)", INT_MV_COLUMN));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
      assertEquals(transformFunction.getName(), "arrayReverseInt");
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
      assertFalse(transformFunction.getResultMetadata().isSingleValue());
      int[][] expectedValues = new int[NUM_ROWS][];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = _intMVValues[i].clone();
        ArrayUtils.reverse(expectedValues[i]);
      }
      testTransformFunctionMV(transformFunction, expectedValues);
    }
    {
      ExpressionContext expression =
          QueryContextConverterUtils.getExpression(String.format("array_reverse_int(%s)", LONG_MV_COLUMN));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
      assertEquals(transformFunction.getName(), "arrayReverseInt");
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
      assertFalse(transformFunction.getResultMetadata().isSingleValue());
      int[][] expectedValues = new int[NUM_ROWS][];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = new int[_longMVValues[i].length];
        ArrayCopyUtils.copy(_longMVValues[i], expectedValues[i], _longMVValues[i].length);
        ArrayUtils.reverse(expectedValues[i]);
      }
      testTransformFunctionMV(transformFunction, expectedValues);
    }
  }

  @Test
  public void testArrayReverseStringTransformFunction() {
    {
      ExpressionContext expression =
          QueryContextConverterUtils.getExpression(String.format("array_reverse_string(%s)", STRING_MV_COLUMN));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
      assertEquals(transformFunction.getName(), "arrayReverseString");
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
      assertFalse(transformFunction.getResultMetadata().isSingleValue());
      String[][] expectedValues = new String[NUM_ROWS][];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = _stringMVValues[i].clone();
        ArrayUtils.reverse(expectedValues[i]);
      }
      testTransformFunctionMV(transformFunction, expectedValues);
    }
    {
      ExpressionContext expression =
          QueryContextConverterUtils.getExpression(String.format("array_reverse_string(%s)", INT_MV_COLUMN));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
      assertEquals(transformFunction.getName(), "arrayReverseString");
      assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
      assertFalse(transformFunction.getResultMetadata().isSingleValue());
      String[][] expectedValues = new String[NUM_ROWS][];
      for (int i = 0; i < NUM_ROWS; i++) {
        expectedValues[i] = new String[_intMVValues[i].length];
        ArrayCopyUtils.copy(_intMVValues[i], expectedValues[i], _longMVValues[i].length);
        ArrayUtils.reverse(expectedValues[i]);
      }
      testTransformFunctionMV(transformFunction, expectedValues);
    }
  }

  @Test
  public void testArraySortIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_sort_int(%s)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arraySortInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    int[][] expectedValues = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intMVValues[i].clone();
      Arrays.sort(expectedValues[i]);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArraySortStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_sort_string(%s)", STRING_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arraySortString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringMVValues[i].clone();
      Arrays.sort(expectedValues[i]);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayIndexOfIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_index_of_int(%s, 2)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayIndexOfInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertTrue(transformFunction.getResultMetadata().isSingleValue());
    int[] expectedValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = ArrayUtils.indexOf(_intMVValues[i], 2);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testArrayIndexOfStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_index_of_string(%s, 'a')", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayIndexOfString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertTrue(transformFunction.getResultMetadata().isSingleValue());
    int[] expectedValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = ArrayUtils.indexOf(_intMVValues[i], 'a');
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testArrayContainsIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_contains_int(%s, 2)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayContainsInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertTrue(transformFunction.getResultMetadata().isSingleValue());
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Boolean.toString(ArrayUtils.contains(_intMVValues[i], 2));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testArrayContainsStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_contains_string(%s, 'a')", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayContainsString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertTrue(transformFunction.getResultMetadata().isSingleValue());
    String[] expectedValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = Boolean.toString(ArrayUtils.contains(_intMVValues[i], 'a'));
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testArraySliceIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_slice_int(%s, 1, 3)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arraySliceInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    int[][] expectedValues = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intMVValues[i].clone();
      expectedValues[i] = Arrays.copyOfRange(expectedValues[i], 1, 3);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArraySliceStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_slice_string(%s, 1, 2)", STRING_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arraySliceString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringMVValues[i].clone();
      expectedValues[i] = Arrays.copyOfRange(expectedValues[i], 1, 2);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayDistinctIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_distinct_int(%s)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayDistinctInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    int[][] expectedValues = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intMVValues[i].clone();
      expectedValues[i] = Arrays.stream(expectedValues[i]).distinct().toArray();
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayDistinctStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_distinct_string(%s)", STRING_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayDistinctString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringMVValues[i].clone();
      expectedValues[i] = Arrays.stream(expectedValues[i]).distinct().toArray(String[]::new);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayRemoveIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_remove_int(%s, 2)", INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayRemoveInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    int[][] expectedValues = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intMVValues[i].clone();
      expectedValues[i] = ArrayUtils.removeElement(expectedValues[i], 2);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayRemoveStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_remove_string(%s, 2)", STRING_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayRemoveString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringMVValues[i].clone();
      expectedValues[i] = ArrayUtils.removeElement(expectedValues[i], 2);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayUnionIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_union_int(%s, %s)", INT_MV_COLUMN, INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayUnionInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    int[][] expectedValues = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intMVValues[i].clone();
      Set<Integer> set = new HashSet<>(Arrays.asList(ArrayUtils.toObject(expectedValues[i])));
      set.addAll(Arrays.asList(ArrayUtils.toObject(expectedValues[i])));
      expectedValues[i] = set.stream().mapToInt(Integer::intValue).toArray();
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testUnionStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_union_string(%s, %s)", STRING_MV_COLUMN, STRING_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayUnionString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringMVValues[i].clone();
      Set<String> set = new HashSet<>(Arrays.asList(expectedValues[i]));
      set.addAll(Arrays.asList(expectedValues[i]));
      expectedValues[i] = set.stream().toArray(String[]::new);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testArrayConcatIntTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_concat_int(%s, %s)", INT_MV_COLUMN, INT_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayConcatInt");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    int[][] expectedValues = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intMVValues[i].clone();
      expectedValues[i] = ArrayUtils.addAll(expectedValues[i], expectedValues[i]);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testConcatStringTransformFunction() {
    ExpressionContext expression =
        QueryContextConverterUtils.getExpression(String.format("array_concat_string(%s, %s)", STRING_MV_COLUMN, STRING_MV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof ScalarTransformFunctionWrapper);
    assertEquals(transformFunction.getName(), "arrayConcatString");
    assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(transformFunction.getResultMetadata().isSingleValue());
    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _stringMVValues[i].clone();
      expectedValues[i] = ArrayUtils.addAll(expectedValues[i], expectedValues[i]);;
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

}
