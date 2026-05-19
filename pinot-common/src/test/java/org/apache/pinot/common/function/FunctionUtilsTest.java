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
package org.apache.pinot.common.function;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class FunctionUtilsTest {

  @Test
  public void testGetArgumentType() {
    // Single values delegated to PinotDataType.getSingleValueType
    assertEquals(FunctionUtils.getArgumentType(1), PinotDataType.INTEGER);
    assertEquals(FunctionUtils.getArgumentType(1L), PinotDataType.LONG);
    assertEquals(FunctionUtils.getArgumentType(1.0f), PinotDataType.FLOAT);
    assertEquals(FunctionUtils.getArgumentType(1.0d), PinotDataType.DOUBLE);
    assertEquals(FunctionUtils.getArgumentType(BigDecimal.ONE), PinotDataType.BIG_DECIMAL);
    assertEquals(FunctionUtils.getArgumentType(Boolean.TRUE), PinotDataType.BOOLEAN);
    assertEquals(FunctionUtils.getArgumentType(new Timestamp(0L)), PinotDataType.TIMESTAMP);
    assertEquals(FunctionUtils.getArgumentType("foo"), PinotDataType.STRING);
    assertEquals(FunctionUtils.getArgumentType(new byte[]{0}), PinotDataType.BYTES);
    assertEquals(FunctionUtils.getArgumentType(new HashMap<>()), PinotDataType.MAP);
    assertEquals(FunctionUtils.getArgumentType(LocalDate.EPOCH), PinotDataType.DATE);
    assertEquals(FunctionUtils.getArgumentType(LocalTime.NOON), PinotDataType.TIME);
    assertEquals(FunctionUtils.getArgumentType(UUID.randomUUID()), PinotDataType.UUID);
    assertEquals(FunctionUtils.getArgumentType((byte) 1), PinotDataType.BYTE);
    assertEquals(FunctionUtils.getArgumentType('a'), PinotDataType.CHARACTER);
    assertEquals(FunctionUtils.getArgumentType((short) 1), PinotDataType.SHORT);
  }

  @Test
  public void testGetArgumentTypeForVendorTimestampSubclass() {
    // Vendor JDBC drivers commonly return Timestamp subclasses (e.g. BigQuery Simba's TimestampTz).
    // Subclasses must resolve to TIMESTAMP via the instanceof dispatch.
    class VendorTimestamp extends Timestamp {
      VendorTimestamp(long time) {
        super(time);
      }
    }
    assertEquals(FunctionUtils.getArgumentType(new VendorTimestamp(0L)), PinotDataType.TIMESTAMP);
  }

  @Test
  public void testGetArgumentTypeForPrimitiveArrays() {
    assertEquals(FunctionUtils.getArgumentType(new int[]{1}), PinotDataType.PRIMITIVE_INT_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new long[]{1L}), PinotDataType.PRIMITIVE_LONG_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new float[]{1.0f}), PinotDataType.PRIMITIVE_FLOAT_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new double[]{1.0d}), PinotDataType.PRIMITIVE_DOUBLE_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new boolean[]{true}), PinotDataType.PRIMITIVE_BOOLEAN_ARRAY);
  }

  @Test
  public void testGetArgumentTypeForReferenceArrays() {
    // Reference arrays sample first non-null element via PinotDataType.getMultiValueType
    assertEquals(FunctionUtils.getArgumentType(new Integer[]{1}), PinotDataType.INTEGER_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new Long[]{1L}), PinotDataType.LONG_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new Float[]{1.0f}), PinotDataType.FLOAT_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new Double[]{1.0d}), PinotDataType.DOUBLE_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new BigDecimal[]{BigDecimal.ONE}), PinotDataType.BIG_DECIMAL_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new Boolean[]{true}), PinotDataType.BOOLEAN_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new Timestamp[]{new Timestamp(0L)}), PinotDataType.TIMESTAMP_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new String[]{"foo"}), PinotDataType.STRING_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new byte[][]{{0}}), PinotDataType.BYTES_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new LocalDate[]{LocalDate.EPOCH}), PinotDataType.DATE_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new LocalTime[]{LocalTime.NOON}), PinotDataType.TIME_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new UUID[]{UUID.randomUUID()}), PinotDataType.UUID_ARRAY);
  }

  @Test
  public void testGetArgumentTypeForEmptyOrAllNullReferenceArray() {
    // Empty / all-null reference arrays fall back to OBJECT_ARRAY (element type undeterminable).
    assertEquals(FunctionUtils.getArgumentType(new Object[0]), PinotDataType.OBJECT_ARRAY);
    assertEquals(FunctionUtils.getArgumentType(new Object[]{null, null}), PinotDataType.OBJECT_ARRAY);
    // Specific component type with no usable sample also falls back to OBJECT_ARRAY.
    assertEquals(FunctionUtils.getArgumentType(new Integer[0]), PinotDataType.OBJECT_ARRAY);
  }

  @Test
  public void testGetArgumentTypeForCollection() {
    assertEquals(FunctionUtils.getArgumentType(List.of(1, 2, 3)), PinotDataType.COLLECTION);
  }

  @Test
  public void testGetArgumentTypeForUnknown() {
    // Unrecognized non-array types fall back to OBJECT (best-effort coercion sentinel).
    assertEquals(FunctionUtils.getArgumentType(new Object()), PinotDataType.OBJECT);
  }

  @Test
  public void testGetParameterType() {
    // Scalars
    assertEquals(FunctionUtils.getParameterType(int.class), PinotDataType.INTEGER);
    assertEquals(FunctionUtils.getParameterType(Integer.class), PinotDataType.INTEGER);
    assertEquals(FunctionUtils.getParameterType(boolean.class), PinotDataType.BOOLEAN);
    assertEquals(FunctionUtils.getParameterType(Boolean.class), PinotDataType.BOOLEAN);
    assertEquals(FunctionUtils.getParameterType(Timestamp.class), PinotDataType.TIMESTAMP);
    assertEquals(FunctionUtils.getParameterType(String.class), PinotDataType.STRING);
    assertEquals(FunctionUtils.getParameterType(byte[].class), PinotDataType.BYTES);
    // Arrays
    assertEquals(FunctionUtils.getParameterType(int[].class), PinotDataType.PRIMITIVE_INT_ARRAY);
    assertEquals(FunctionUtils.getParameterType(boolean[].class), PinotDataType.PRIMITIVE_BOOLEAN_ARRAY);
    assertEquals(FunctionUtils.getParameterType(Timestamp[].class), PinotDataType.TIMESTAMP_ARRAY);
    assertEquals(FunctionUtils.getParameterType(String[].class), PinotDataType.STRING_ARRAY);
    assertEquals(FunctionUtils.getParameterType(byte[][].class), PinotDataType.BYTES_ARRAY);
    // Boxed array forms not allowed as scalar function parameters
    assertNull(FunctionUtils.getParameterType(Integer[].class));
    assertNull(FunctionUtils.getParameterType(Boolean[].class));
    // Unknown class
    assertNull(FunctionUtils.getParameterType(LocalDate.class));
  }

  @Test
  public void testGetColumnDataType() {
    // Scalars
    assertEquals(FunctionUtils.getColumnDataType(int.class), ColumnDataType.INT);
    assertEquals(FunctionUtils.getColumnDataType(Integer.class), ColumnDataType.INT);
    assertEquals(FunctionUtils.getColumnDataType(boolean.class), ColumnDataType.BOOLEAN);
    assertEquals(FunctionUtils.getColumnDataType(Timestamp.class), ColumnDataType.TIMESTAMP);
    assertEquals(FunctionUtils.getColumnDataType(byte[].class), ColumnDataType.BYTES);
    // Arrays
    assertEquals(FunctionUtils.getColumnDataType(int[].class), ColumnDataType.INT_ARRAY);
    assertEquals(FunctionUtils.getColumnDataType(boolean[].class), ColumnDataType.BOOLEAN_ARRAY);
    assertEquals(FunctionUtils.getColumnDataType(Timestamp[].class), ColumnDataType.TIMESTAMP_ARRAY);
    assertEquals(FunctionUtils.getColumnDataType(byte[][].class), ColumnDataType.BYTES_ARRAY);
    // Object
    assertEquals(FunctionUtils.getColumnDataType(Object.class), ColumnDataType.OBJECT);
    // Unknown class
    assertNull(FunctionUtils.getColumnDataType(LocalDate.class));
  }
}
