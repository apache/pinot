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
package org.apache.pinot.query.runtime.operator.utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies identity conversion and numeric coercions at the single-stage to multi-stage boundary.
public class TypeUtilsTest {
  @DataProvider(name = "numericValues")
  public Object[][] numericValues() {
    return new Object[][]{
        {Integer.valueOf(1024)}, {Long.valueOf(9007199254740993L)}, {Float.valueOf(-0.0f)},
        {Double.valueOf(-0.0d)}, {Float.intBitsToFloat(0x7fc00001)},
        {Double.longBitsToDouble(0x7ff8000000000001L)}, {Double.POSITIVE_INFINITY},
        {Double.NEGATIVE_INFINITY}, {Double.MIN_VALUE}, {new BigDecimal("123456789.125")},
        {Byte.valueOf((byte) -1)}, {Short.valueOf((short) 1024)}
    };
  }

  @Test(dataProvider = "numericValues")
  public void testNumericConversions(Number value) {
    Object converted = TypeUtils.convert(value, ColumnDataType.INT);
    assertEquals(converted, Integer.valueOf(value.intValue()));
    if (value instanceof Integer) {
      assertSame(converted, value);
    }
    converted = TypeUtils.convert(value, ColumnDataType.LONG);
    assertEquals(converted, Long.valueOf(value.longValue()));
    if (value instanceof Long) {
      assertSame(converted, value);
    }
    converted = TypeUtils.convert(value, ColumnDataType.FLOAT);
    assertEquals(Float.floatToRawIntBits((Float) converted), Float.floatToRawIntBits(value.floatValue()));
    if (value instanceof Float) {
      assertSame(converted, value);
    }
    converted = TypeUtils.convert(value, ColumnDataType.DOUBLE);
    assertEquals(Double.doubleToRawLongBits((Double) converted), Double.doubleToRawLongBits(value.doubleValue()));
    if (value instanceof Double) {
      assertSame(converted, value);
    }
  }

  /// BOOLEAN is stored as INT. A BOOLEAN leaf column the planner types as VARCHAR (e.g. an OPEN_STRUCT key) must read
  /// as `true`/`false`, as `CAST(b AS VARCHAR)` and the single-stage engine give, not as the stored `1`/`0`.
  @Test
  public void testBooleanToStringUsesLogicalType() {
    assertEquals(TypeUtils.convert(1, ColumnDataType.BOOLEAN, ColumnDataType.STRING), "true");
    assertEquals(TypeUtils.convert(0, ColumnDataType.BOOLEAN, ColumnDataType.STRING), "false");
    // Other logical types and stored-type conversions are unchanged.
    assertEquals(TypeUtils.convert(1, ColumnDataType.INT, ColumnDataType.STRING), "1");
    assertEquals(TypeUtils.convert(1, ColumnDataType.BOOLEAN, ColumnDataType.LONG), 1L);
  }

  @Test
  public void testTimestampAndArrayToStringMatchTheSingleStageEngine() {
    assertEquals(TypeUtils.convert(0L, ColumnDataType.TIMESTAMP, ColumnDataType.STRING),
        new Timestamp(0L).toString());
    assertEquals(TypeUtils.convert(new long[]{1, 2}, ColumnDataType.LONG_ARRAY, ColumnDataType.STRING), "[1,2]");
    assertEquals(TypeUtils.convert(new String[]{"a", "b"}, ColumnDataType.STRING_ARRAY, ColumnDataType.STRING),
        "[\"a\",\"b\"]");
    assertEquals(TypeUtils.convert(new int[]{1, 0}, ColumnDataType.BOOLEAN_ARRAY, ColumnDataType.STRING),
        "[true,false]");
  }

  /// A segment can serve a key as STRING while the plan types it numeric from the table's declaration: text converts
  /// as ingestion converts it, and text that does not convert fails with both types named, never truncated, wrapped
  /// or read as null.
  @Test
  public void testNumericTextToNumberConvertsOrFails() {
    assertEquals(TypeUtils.convert("42", ColumnDataType.STRING, ColumnDataType.LONG), 42L);
    assertEquals(TypeUtils.convert("1.5", ColumnDataType.STRING, ColumnDataType.DOUBLE), 1.5);
    assertEquals(TypeUtils.convert("7", ColumnDataType.STRING, ColumnDataType.INT), 7);
    assertEquals(TypeUtils.convert("NaN", ColumnDataType.STRING, ColumnDataType.DOUBLE), Double.NaN);
    for (String text : List.of("abc", "1.9", "2.0", "99999999999999999999")) {
      IllegalStateException e = expectThrows(IllegalStateException.class,
          () -> TypeUtils.convert(text, ColumnDataType.STRING, ColumnDataType.LONG));
      assertTrue(e.getMessage().contains("STRING value '" + text + "' to LONG"), e.getMessage());
    }
    expectThrows(IllegalStateException.class,
        () -> TypeUtils.convert("2147483648", ColumnDataType.STRING, ColumnDataType.INT));
  }

  @Test
  public void testConvertRowWithNullAndMixedTypes() {
    Object[] row = {Integer.valueOf(1024), Double.valueOf(-0.0d), null, Long.valueOf(9007199254740993L),
        Double.valueOf(9.75d), Integer.valueOf(1024)};
    Object[] original = row.clone();
    TypeUtils.convertRow(row, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.FLOAT,
        ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.DOUBLE});
    assertSame(row[0], original[0]);
    assertSame(row[1], original[1]);
    assertNull(row[2]);
    assertSame(row[3], original[3]);
    assertEquals(row[4], Integer.valueOf(9));
    assertEquals(row[5], Double.valueOf(1024d));
    for (ColumnDataType type : new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE}) {
      assertThrows(NullPointerException.class, () -> TypeUtils.convert(null, type));
    }
  }

  @Test
  public void testNumericArrayKeepsItsType() {
    int[] intArray = {1, 2};
    assertSame(TypeUtils.convert(intArray, ColumnDataType.INT_ARRAY), intArray);
    double[] doubleArray = {1.5, 2.5};
    assertSame(TypeUtils.convert(doubleArray, ColumnDataType.DOUBLE_ARRAY), doubleArray);
  }

  /// The single-stage engine can compute another element type than the one the multi-stage planner inferred, for
  /// example a `double[]` from `generateArray(intCol, intCol + 1)`, whose `plus` returns DOUBLE.
  @Test
  public void testNumericArrayConvertsToTheStoredType() {
    assertEquals(TypeUtils.convert(new double[]{1, 2}, ColumnDataType.INT_ARRAY), new int[]{1, 2});
    assertEquals(TypeUtils.convert(new double[]{1, 2}, ColumnDataType.LONG_ARRAY), new long[]{1, 2});
    assertEquals(TypeUtils.convert(new int[]{1, 2}, ColumnDataType.LONG_ARRAY), new long[]{1, 2});
    assertEquals(TypeUtils.convert(new long[]{1, 2}, ColumnDataType.INT_ARRAY), new int[]{1, 2});
    assertEquals(TypeUtils.convert(new int[]{1, 2}, ColumnDataType.FLOAT_ARRAY), new float[]{1, 2});
    assertEquals(TypeUtils.convert(new float[]{1.5f}, ColumnDataType.DOUBLE_ARRAY), new double[]{1.5});
    assertEquals(TypeUtils.convert(new long[]{1L << 40}, ColumnDataType.DOUBLE_ARRAY), new double[]{1L << 40});
  }
}
