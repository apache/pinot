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
package org.apache.pinot.common.utils;

import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.PinotDataType.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class PinotDataTypeTest {
  private static final PinotDataType[] SOURCE_TYPES =
      {BYTE, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, STRING, BYTE_ARRAY, CHARACTER_ARRAY, SHORT_ARRAY, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY};
  private static final Object[] SOURCE_VALUES =
      {(byte) 123, (char) 123, (short) 123, 123, 123L, 123f, 123d, " 123", new Object[]{(byte) 123}, new Object[]{(char) 123}, new Object[]{(short) 123}, new Object[]{123}, new Object[]{123L}, new Object[]{123f}, new Object[]{123d}, new Object[]{" 123"}};
  private static final PinotDataType[] DEST_TYPES =
      {INTEGER, LONG, FLOAT, DOUBLE, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY};
  private static final Object[] EXPECTED_DEST_VALUES =
      {123, 123L, 123f, 123d, new Object[]{123}, new Object[]{123L}, new Object[]{123f}, new Object[]{123d}};
  private static final String[] EXPECTED_STRING_VALUES =
      {Byte.toString((byte) 123), Character.toString((char) 123), Short.toString((short) 123), Integer
          .toString(123), Long.toString(123L), Float.toString(123f), Double.toString(123d), " 123", Byte
              .toString((byte) 123), Character.toString((char) 123), Short.toString((short) 123), Integer
                  .toString(123), Long.toString(123L), Float.toString(123f), Double.toString(123d), " 123"};

  @Test
  public void testNumberConversion() {
    int numDestTypes = DEST_TYPES.length;
    for (int i = 0; i < numDestTypes; i++) {
      PinotDataType destType = DEST_TYPES[i];
      Object expectedDestValue = EXPECTED_DEST_VALUES[i];
      int numSourceTypes = SOURCE_TYPES.length;
      for (int j = 0; j < numSourceTypes; j++) {
        Object actualDestValue = destType.convert(SOURCE_VALUES[j], SOURCE_TYPES[j]);
        assertEquals(actualDestValue, expectedDestValue);
      }
    }
  }

  @Test
  public void testToString() {
    int numSourceTypes = SOURCE_TYPES.length;
    for (int i = 0; i < numSourceTypes; i++) {
      assertEquals(STRING.convert(SOURCE_VALUES[i], SOURCE_TYPES[i]), EXPECTED_STRING_VALUES[i]);
      assertEquals(STRING_ARRAY.convert(SOURCE_VALUES[i], SOURCE_TYPES[i]), new String[]{EXPECTED_STRING_VALUES[i]});
    }
  }

  @Test
  public void testBoolean() {
    assertEquals(INTEGER.convert(true, BOOLEAN), 1);
    assertEquals(INTEGER.convert(false, BOOLEAN), 0);
    assertEquals(LONG.convert(true, BOOLEAN), 1L);
    assertEquals(LONG.convert(false, BOOLEAN), 0L);
    assertEquals(FLOAT.convert(true, BOOLEAN), 1f);
    assertEquals(FLOAT.convert(false, BOOLEAN), 0f);
    assertEquals(DOUBLE.convert(true, BOOLEAN), 1d);
    assertEquals(DOUBLE.convert(false, BOOLEAN), 0d);
    assertEquals(STRING.convert(true, BOOLEAN), "true");
    assertEquals(STRING.convert(false, BOOLEAN), "false");
  }

  @Test
  public void testCharacter() {
    assertEquals(STRING.convert('a', CHARACTER), "a");
    assertEquals(CHARACTER_ARRAY.getSingleValueType(), CHARACTER);
  }

  @Test
  public void testBytes() {
    assertEquals(STRING.convert(new byte[]{0, 1}, BYTES), "0001");
    assertEquals(BYTES.convert("0001", STRING), new byte[]{0, 1});
    assertEquals(BYTES.convert(new byte[]{0, 1}, BYTES), new byte[]{0, 1});
    assertEquals(BYTES.convert(new Byte[]{0, 1}, BYTE_ARRAY), new byte[]{0, 1});
    assertEquals(BYTES.convert(new String[]{"0001"}, STRING_ARRAY), new byte[]{0, 1});
  }

  @Test
  public void testObject() {
    assertEquals(OBJECT.toInt(new NumberObject("123")), 123);
    assertEquals(OBJECT.toLong(new NumberObject("123")), 123L);
    assertEquals(OBJECT.toFloat(new NumberObject("123")), 123f);
    assertEquals(OBJECT.toDouble(new NumberObject("123")), 123.0);
    assertEquals(OBJECT.toString(new NumberObject("123")), "123");
    assertEquals(OBJECT_ARRAY.getSingleValueType(), OBJECT);
  }

  @Test
  public void testInvalidConversion() {
    for (PinotDataType sourceType : values()) {
      if (sourceType.isSingleValue() && sourceType != STRING && sourceType != BYTES) {
        assertInvalidConversion(sourceType, BYTES);
      }
    }

    assertInvalidConversion(BYTES, INTEGER);
    assertInvalidConversion(BYTES, LONG);
    assertInvalidConversion(BYTES, FLOAT);
    assertInvalidConversion(BYTES, DOUBLE);
    assertInvalidConversion(BYTES, INTEGER_ARRAY);
    assertInvalidConversion(BYTES, LONG_ARRAY);
    assertInvalidConversion(BYTES, FLOAT_ARRAY);
    assertInvalidConversion(BYTES, DOUBLE_ARRAY);

    for (PinotDataType sourceType : values()) {
      assertInvalidConversion(sourceType, BOOLEAN);
      assertInvalidConversion(sourceType, BYTE);
      assertInvalidConversion(sourceType, CHARACTER);
      assertInvalidConversion(sourceType, SHORT);
      assertInvalidConversion(sourceType, OBJECT);
      assertInvalidConversion(sourceType, BYTE_ARRAY);
      assertInvalidConversion(sourceType, CHARACTER_ARRAY);
      assertInvalidConversion(sourceType, SHORT_ARRAY);
      assertInvalidConversion(sourceType, OBJECT_ARRAY);
    }
  }

  private void assertInvalidConversion(PinotDataType sourceType, PinotDataType destType) {
    try {
      destType.convert(null, sourceType);
    } catch (UnsupportedOperationException e) {
      return;
    }
    fail();
  }

  private static class NumberObject extends Number {
    final String _value;

    NumberObject(String value) {
      _value = value;
    }

    @Override
    public int intValue() {
      return Integer.parseInt(_value);
    }

    @Override
    public long longValue() {
      return Long.parseLong(_value);
    }

    @Override
    public float floatValue() {
      return Float.parseFloat(_value);
    }

    @Override
    public double doubleValue() {
      return Double.parseDouble(_value);
    }

    @Override
    public String toString() {
      return _value;
    }
  }
}
