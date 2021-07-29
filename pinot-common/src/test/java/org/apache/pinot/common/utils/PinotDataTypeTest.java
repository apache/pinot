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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.PinotDataType.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotDataTypeTest {
  private static final PinotDataType[] SOURCE_TYPES =
      {BYTE, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, STRING, JSON, BYTE_ARRAY, CHARACTER_ARRAY, SHORT_ARRAY, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY};
  private static final Object[] SOURCE_VALUES =
      {(byte) 123, (char) 123, (short) 123, 123, 123L, 123f, 123d, " 123", "123 ", new Object[]{(byte) 123}, new Object[]{(char) 123}, new Object[]{(short) 123}, new Object[]{123}, new Object[]{123L}, new Object[]{123f}, new Object[]{123d}, new Object[]{" 123"}};
  private static final PinotDataType[] DEST_TYPES =
      {INTEGER, LONG, FLOAT, DOUBLE, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY};
  private static final Object[] EXPECTED_DEST_VALUES =
      {123, 123L, 123f, 123d, new Object[]{123}, new Object[]{123L}, new Object[]{123f}, new Object[]{123d}};
  private static final String[] EXPECTED_STRING_VALUES =
      {Byte.toString((byte) 123), Character.toString((char) 123), Short.toString((short) 123), Integer.toString(
          123), Long.toString(123L), Float.toString(123f), Double.toString(123d), " 123", "123 ", Byte.toString(
          (byte) 123), Character.toString((char) 123), Short.toString((short) 123), Integer.toString(
          123), Long.toString(123L), Float.toString(123f), Double.toString(123d), " 123"};

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

    assertEquals(BOOLEAN.convert("true", JSON), true);
    assertEquals(BOOLEAN.convert("false", JSON), false);
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
    assertEquals(BYTES.convert("AAE=", JSON), new byte[]{0,1});
    assertEquals(BYTES.convert(new Byte[]{0, 1}, BYTE_ARRAY), new byte[]{0, 1});
    assertEquals(BYTES.convert(new String[]{"0001"}, STRING_ARRAY), new byte[]{0, 1});
  }

  @Test
  public void testTimestamp() {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    assertEquals(TIMESTAMP.convert(timestamp.getTime(), LONG), timestamp);
    assertEquals(TIMESTAMP.convert(timestamp.toString(), STRING), timestamp);
    assertEquals(TIMESTAMP.convert(timestamp.getTime(), JSON), timestamp);
    assertEquals(TIMESTAMP.convert(timestamp.toString(), JSON), timestamp);
  }

  @Test
  public void testJSON() {
    assertEquals(JSON.convert(false, BOOLEAN), "false");
    assertEquals(JSON.convert(true, BOOLEAN), "true");
    assertEquals(JSON.convert(new byte[]{0, 1}, BYTES), "\"AAE=\""); // Base64 encoding.
    assertEquals(JSON.convert("{\"bytes\":\"AAE=\",\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]},\"timestamp\":1620324238610}", STRING), "{\"bytes\":\"AAE=\",\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]},\"timestamp\":1620324238610}");
    assertEquals(JSON.convert(new Timestamp(1620324238610l), TIMESTAMP), "1620324238610");
  }

  @Test
  public void testObject() {
    assertEquals(OBJECT.toInt(new NumberObject("123")), 123);
    assertEquals(OBJECT.toLong(new NumberObject("123")), 123L);
    assertEquals(OBJECT.toFloat(new NumberObject("123")), 123f);
    assertEquals(OBJECT.toDouble(new NumberObject("123")), 123.0);
    assertTrue(OBJECT.toBoolean(new NumberObject("123")));
    assertEquals(OBJECT.toTimestamp(new NumberObject("123")).getTime(), 123L);
    assertEquals(OBJECT.toString(new NumberObject("123")), "123");
    assertEquals(OBJECT.toJson(getGenericTestObject()), "{\"bytes\":\"AAE=\",\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]},\"timestamp\":1620324238610}");
    assertEquals(OBJECT_ARRAY.getSingleValueType(), OBJECT);
  }

  @Test
  public void testGetSingleValueType() {
    Map<Class<?>, PinotDataType> testCases = new HashMap<>();
    testCases.put(Boolean.class, BOOLEAN);
    testCases.put(Byte.class, BYTE);
    testCases.put(Character.class, CHARACTER);
    testCases.put(Short.class, SHORT);
    testCases.put(Integer.class, INTEGER);
    testCases.put(Long.class, LONG);
    testCases.put(Float.class, FLOAT);
    testCases.put(Double.class, DOUBLE);
    testCases.put(Timestamp.class, TIMESTAMP);
    testCases.put(String.class, STRING);
    testCases.put(byte[].class, BYTES);

    for (Map.Entry<Class<?>, PinotDataType> tc : testCases.entrySet()) {
      assertEquals(getSingleValueType(tc.getKey()), tc.getValue());
    }
    assertEquals(getSingleValueType(Object.class), OBJECT);
    assertEquals(getSingleValueType(null), OBJECT);
  }

  @Test
  public void testGetMultipleValueType() {
    Map<Class<?>, PinotDataType> testCases = new HashMap<>();
    testCases.put(Byte.class, BYTE_ARRAY);
    testCases.put(Character.class, CHARACTER_ARRAY);
    testCases.put(Short.class, SHORT_ARRAY);
    testCases.put(Integer.class, INTEGER_ARRAY);
    testCases.put(Long.class, LONG_ARRAY);
    testCases.put(Float.class, FLOAT_ARRAY);
    testCases.put(Double.class, DOUBLE_ARRAY);
    testCases.put(String.class, STRING_ARRAY);

    for (Map.Entry<Class<?>, PinotDataType> tc : testCases.entrySet()) {
      assertEquals(getMultipleValueType(tc.getKey()), tc.getValue());
    }
    assertEquals(getMultipleValueType(Object.class), OBJECT_ARRAY);
    assertEquals(getMultipleValueType(null), OBJECT_ARRAY);
  }

  private static Object getGenericTestObject() {
    Map<String, Object> map1 = new HashMap<>();
    map1.put("array", Arrays.asList(-5.4,4, "2"));
    map1.put("key1", "value");
    map1.put("key2", null);

    Map<String, Object> map2 = new HashMap<>();
    map2.put("map", map1);
    map2.put("bytes", new byte[]{0,1});
    map2.put("timestamp", new Timestamp(1620324238610l));

    return map2;
  }

  @Test
  public void testInvalidConversion() {
    for (PinotDataType sourceType : values()) {
      if (sourceType.isSingleValue() && sourceType != STRING && sourceType != BYTES && sourceType != JSON) {
        assertInvalidConversion(null, sourceType, BYTES, UnsupportedOperationException.class);
      }
    }

    assertInvalidConversion(null, BYTES, INTEGER, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, LONG, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, FLOAT, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, DOUBLE, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, INTEGER_ARRAY, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, LONG_ARRAY, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, FLOAT_ARRAY, UnsupportedOperationException.class);
    assertInvalidConversion(null, BYTES, DOUBLE_ARRAY, UnsupportedOperationException.class);

    for (PinotDataType sourceType : values()) {
      assertInvalidConversion(null, sourceType, BYTE, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, CHARACTER, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, SHORT, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, OBJECT, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, BYTE_ARRAY, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, CHARACTER_ARRAY, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, SHORT_ARRAY, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, OBJECT_ARRAY, UnsupportedOperationException.class);
    }

    assertInvalidConversion("xyz", STRING, JSON, RuntimeException.class);

  }

  private void assertInvalidConversion(Object value, PinotDataType sourceType, PinotDataType destType,
      Class expectedExceptionType) {
    try {
      destType.convert(value, sourceType);
    } catch (Exception e) {
      if (e.getClass().equals(expectedExceptionType)) {
        return;
      }
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
