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
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


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
}
