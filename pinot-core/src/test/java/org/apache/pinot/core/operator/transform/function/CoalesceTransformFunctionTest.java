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
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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
    assertEquals(coalesceFunc.getName(), "coalesce");
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

  @Test
  public void testCoalesceIntMvWithLiteralFallback() {
    int intLiteral = 42;
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %d)", INT_MV_NULL_COLUMN, intLiteral)),
        _dataSourceMap);
    TransformResultMetadata metadata = coalesceFunc.getResultMetadata();
    assertEquals(metadata.getDataType(), DataType.INT);
    assertFalse(metadata.isSingleValue());
    assertFalse(metadata.hasDictionary());

    int[][] expectedResults = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResults[i] = isNullRow(i) ? new int[]{intLiteral} : _intMVValues[i];
    }
    testTransformFunctionMV(coalesceFunc, expectedResults);
    assertNull(coalesceFunc.getNullBitmap(_projectionBlock));

    // Literal fallback wrapper array should be reused across null rows (rows 1 and 3 are both null).
    int[][] actualResults = coalesceFunc.transformToIntValuesMV(_projectionBlock);
    assertSame(actualResults[1], actualResults[3]);

    // SV transform methods must reject MV COALESCE.
    expectThrows(UnsupportedOperationException.class, () -> coalesceFunc.transformToIntValuesSV(_projectionBlock));
  }

  @Test
  public void testCoalesceMvNumericWidening() {
    // INT MV + LONG MV -> LONG MV
    TransformFunction longFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_MV_NULL_COLUMN, LONG_MV_COLUMN)),
        _dataSourceMap);
    assertEquals(longFunc.getResultMetadata().getDataType(), DataType.LONG);
    assertFalse(longFunc.getResultMetadata().isSingleValue());
    long[][] expectedLong = new long[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedLong[i] = _longMVValues[i];
      } else {
        expectedLong[i] = new long[_intMVValues[i].length];
        for (int j = 0; j < _intMVValues[i].length; j++) {
          expectedLong[i][j] = _intMVValues[i][j];
        }
      }
    }
    testTransformFunctionMV(longFunc, expectedLong);

    // INT MV + FLOAT MV -> FLOAT MV
    TransformFunction floatFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_MV_NULL_COLUMN, FLOAT_MV_COLUMN)),
        _dataSourceMap);
    assertEquals(floatFunc.getResultMetadata().getDataType(), DataType.FLOAT);
    assertFalse(floatFunc.getResultMetadata().isSingleValue());
    float[][] expectedFloat = new float[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedFloat[i] = _floatMVValues[i];
      } else {
        expectedFloat[i] = new float[_intMVValues[i].length];
        for (int j = 0; j < _intMVValues[i].length; j++) {
          expectedFloat[i][j] = _intMVValues[i][j];
        }
      }
    }
    testTransformFunctionMV(floatFunc, expectedFloat);

    // INT MV + DOUBLE MV -> DOUBLE MV
    TransformFunction doubleFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_MV_NULL_COLUMN, DOUBLE_MV_COLUMN)),
        _dataSourceMap);
    assertEquals(doubleFunc.getResultMetadata().getDataType(), DataType.DOUBLE);
    assertFalse(doubleFunc.getResultMetadata().isSingleValue());
    double[][] expectedDouble = new double[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedDouble[i] = _doubleMVValues[i];
      } else {
        expectedDouble[i] = new double[_intMVValues[i].length];
        for (int j = 0; j < _intMVValues[i].length; j++) {
          expectedDouble[i][j] = _intMVValues[i][j];
        }
      }
    }
    testTransformFunctionMV(doubleFunc, expectedDouble);

    // INT MV + BIG_DECIMAL SV -> BIG_DECIMAL MV
    TransformFunction bigDecimalFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_MV_NULL_COLUMN, BIG_DECIMAL_SV_COLUMN)),
        _dataSourceMap);
    assertEquals(bigDecimalFunc.getResultMetadata().getDataType(), DataType.BIG_DECIMAL);
    assertFalse(bigDecimalFunc.getResultMetadata().isSingleValue());
    BigDecimal[][] actualBigDecimal = bigDecimalFunc.transformToBigDecimalValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        assertEquals(actualBigDecimal[i].length, 1);
        assertEquals(actualBigDecimal[i][0].compareTo(_bigDecimalSVValues[i]), 0);
      } else {
        assertEquals(actualBigDecimal[i].length, _intMVValues[i].length);
        for (int j = 0; j < _intMVValues[i].length; j++) {
          assertEquals(actualBigDecimal[i][j].compareTo(BigDecimal.valueOf(_intMVValues[i][j])), 0);
        }
      }
    }
    assertNull(bigDecimalFunc.getNullBitmap(_projectionBlock));
  }

  @Test
  public void testCoalesceMvStringAndSvMix() {
    // MV string expression + SV string literal fallback
    TransformFunction literalFallbackFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(
            String.format("COALESCE(CAST(%s AS STRING), 'fallback')", INT_MV_NULL_COLUMN)),
        _dataSourceMap);
    assertEquals(literalFallbackFunc.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(literalFallbackFunc.getResultMetadata().isSingleValue());
    String[][] expectedLiteralFallback = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedLiteralFallback[i] = new String[]{"fallback"};
      } else {
        expectedLiteralFallback[i] = new String[_intMVValues[i].length];
        for (int j = 0; j < _intMVValues[i].length; j++) {
          expectedLiteralFallback[i][j] = Integer.toString(_intMVValues[i][j]);
        }
      }
    }
    testTransformFunctionMV(literalFallbackFunc, expectedLiteralFallback);
    String[][] actualLiteralFallback = literalFallbackFunc.transformToStringValuesMV(_projectionBlock);
    assertSame(actualLiteralFallback[1], actualLiteralFallback[3]);

    // SV nullable first argument + MV second argument
    TransformFunction svFirstFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", STRING_SV_NULL_COLUMN, STRING_MV_COLUMN)),
        _dataSourceMap);
    assertEquals(svFirstFunc.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(svFirstFunc.getResultMetadata().isSingleValue());
    String[][] expectedSvFirst = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedSvFirst[i] = isNullRow(i) ? _stringMVValues[i] : new String[]{_stringSVValues[i]};
    }
    testTransformFunctionMV(svFirstFunc, expectedSvFirst);
  }

  @Test
  public void testCastAroundCoalesceMv() {
    TransformFunction castFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("CAST(COALESCE(%s, 0) AS VARCHAR)", INT_MV_NULL_COLUMN)),
        _dataSourceMap);
    assertEquals(castFunc.getResultMetadata().getDataType(), DataType.STRING);
    assertFalse(castFunc.getResultMetadata().isSingleValue());
    String[][] expected = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expected[i] = new String[]{"0"};
      } else {
        expected[i] = new String[_intMVValues[i].length];
        for (int j = 0; j < _intMVValues[i].length; j++) {
          expected[i][j] = Integer.toString(_intMVValues[i][j]);
        }
      }
    }
    testTransformFunctionMV(castFunc, expected);
  }

  @Test
  public void testCoalesceMvAllNullRows() {
    TransformFunction intFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_MV_NULL_COLUMN, INT_SV_NULL_COLUMN)),
        _dataSourceMap);
    RoaringBitmap expectedNull = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNull.add(i);
      }
    }
    testTransformFunctionMVWithNull(intFunc, _intMVValues, expectedNull);
    int[][] intResults = intFunc.transformToIntValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        assertEquals(intResults[i], NullValuePlaceHolder.INT_ARRAY);
      }
    }

    TransformFunction strFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(
            String.format("COALESCE(CAST(%s AS STRING), %s)", INT_MV_NULL_COLUMN, STRING_SV_NULL_COLUMN)),
        _dataSourceMap);
    String[][] strResults = strFunc.transformToStringValuesMV(_projectionBlock);
    assertEquals(strFunc.getNullBitmap(_projectionBlock), expectedNull);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        assertEquals(strResults[i], NullValuePlaceHolder.STRING_ARRAY);
      }
    }
  }

  @Test
  public void testCoalesceMvPreservesNonNullEmptyArray() {
    // _intMVValues entries are in [1, MAX_MULTI_VALUE], so filterMv(..., 'v > 1000') yields [] on every non-null row
    // and SQL NULL on odd rows. COALESCE must replace only SQL NULL rows, keeping [] on non-null rows.
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(
            String.format("COALESCE(filterMv(%s, 'v > 1000'), 99)", INT_MV_NULL_COLUMN)),
        _dataSourceMap);
    int[][] expected = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expected[i] = isNullRow(i) ? new int[]{99} : new int[0];
    }
    testTransformFunctionMV(coalesceFunc, expected);
    assertNull(coalesceFunc.getNullBitmap(_projectionBlock));
  }

  @Test
  public void testCoalesceMvNonNullFastPath() {
    TransformFunction coalesceFunc = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, 42)", INT_MV_COLUMN)),
        _dataSourceMap);
    testTransformFunctionMV(coalesceFunc, _intMVValues);
    assertNull(coalesceFunc.getNullBitmap(_projectionBlock));
  }

  @Test
  public void testCoalesceNonNullSvFirstWithMvSecond() {
    // Non-null SV column first + MV column second: must NOT take the MV pass-through fast path on arg 0.
    TransformFunction svColFirst = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_SV_COLUMN, INT_MV_COLUMN)),
        _dataSourceMap);
    assertEquals(svColFirst.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(svColFirst.getResultMetadata().isSingleValue());
    assertFalse(svColFirst.getResultMetadata().hasDictionary());
    int[][] expectedSvCol = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedSvCol[i] = new int[]{_intSVValues[i]};
    }
    testTransformFunctionMV(svColFirst, expectedSvCol);
    assertNull(svColFirst.getNullBitmap(_projectionBlock));

    // Non-null SV literal first + MV column second: wraps literal in a single cached 1-element array.
    int intLiteral = 42;
    TransformFunction literalFirst = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%d, %s)", intLiteral, INT_MV_COLUMN)),
        _dataSourceMap);
    assertEquals(literalFirst.getResultMetadata().getDataType(), DataType.INT);
    assertFalse(literalFirst.getResultMetadata().isSingleValue());
    int[][] expectedLiteral = new int[NUM_ROWS][];
    int[] expectedSingle = new int[]{intLiteral};
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedLiteral[i] = expectedSingle;
    }
    testTransformFunctionMV(literalFirst, expectedLiteral);
    assertNull(literalFirst.getNullBitmap(_projectionBlock));
    int[][] actualLiteral = literalFirst.transformToIntValuesMV(_projectionBlock);
    assertSame(actualLiteral[0], actualLiteral[1]);
  }

  @Test
  public void testCoalesceDoesNotMutateCachedNullBitmap() {
    RoaringBitmap cachedNullBitmap = _projectionBlock.getBlockValueSet(INT_MV_NULL_COLUMN).getNullBitmap();
    assertNotNull(cachedNullBitmap);
    RoaringBitmap expectedCached = cachedNullBitmap.clone();

    // Case 1: second arg has no nulls -> COALESCE null bitmap is null, cached bitmap must remain intact.
    TransformFunction nonNullFallback = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s, %s)", INT_MV_NULL_COLUMN, INT_SV_COLUMN)),
        _dataSourceMap);
    assertNull(nonNullFallback.getNullBitmap(_projectionBlock));
    assertEquals(_projectionBlock.getBlockValueSet(INT_MV_NULL_COLUMN).getNullBitmap(), expectedCached);

    // Case 2: second arg has a disjoint null bitmap (null on even rows, non-null on odd rows) -> intersection is empty.
    TransformFunction disjointNulls = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(
            String.format("COALESCE(%s, CASE WHEN %s IS NULL THEN 1 ELSE NULL END)", INT_MV_NULL_COLUMN,
                INT_SV_NULL_COLUMN)),
        _dataSourceMap);
    assertNull(disjointNulls.getNullBitmap(_projectionBlock));
    assertEquals(_projectionBlock.getBlockValueSet(INT_MV_NULL_COLUMN).getNullBitmap(), expectedCached);

    // Case 3: second arg has a subset of nulls (null on row 1, which is odd) -> intersection contains 1.
    TransformFunction subsetNulls = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(
            String.format("COALESCE(%s, CASE WHEN %s = %d THEN NULL ELSE 1 END)", INT_MV_NULL_COLUMN, INT_SV_COLUMN,
                _intSVValues[1])),
        _dataSourceMap);
    RoaringBitmap subsetResult = subsetNulls.getNullBitmap(_projectionBlock);
    assertNotNull(subsetResult);
    assertTrue(subsetResult.contains(1));
    assertEquals(_projectionBlock.getBlockValueSet(INT_MV_NULL_COLUMN).getNullBitmap(), expectedCached);

    // Case 4: single-arg COALESCE returns a clone so mutating the result never mutates the cached bitmap.
    TransformFunction singleArg = TransformFunctionFactory.get(
        RequestContextUtils.getExpression(String.format("COALESCE(%s)", INT_MV_NULL_COLUMN)), _dataSourceMap);
    RoaringBitmap singleResult = singleArg.getNullBitmap(_projectionBlock);
    assertEquals(singleResult, expectedCached);
    assertNotNull(singleResult);
    singleResult.clear();
    assertEquals(_projectionBlock.getBlockValueSet(INT_MV_NULL_COLUMN).getNullBitmap(), expectedCached);
  }
}
