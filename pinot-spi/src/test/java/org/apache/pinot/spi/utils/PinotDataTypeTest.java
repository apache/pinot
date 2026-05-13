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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.spi.utils.PinotDataType.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotDataTypeTest {
  private static final PinotDataType[] SOURCE_TYPES = {
      BYTE, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING, JSON,
      BYTE_ARRAY, CHARACTER_ARRAY, SHORT_ARRAY, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY
  };
  private static final Object[] SOURCE_VALUES = {
      (byte) 123, (char) 123, (short) 123, 123, 123L, 123f, 123d, BigDecimal.valueOf(123), " 123", "123 ",
      new Object[]{(byte) 123}, new Object[]{(char) 123}, new Object[]{(short) 123}, new Object[]{123},
      new Object[]{123L}, new Object[]{123f}, new Object[]{123d}, new Object[]{" 123"}
  };
  private static final PinotDataType[] DEST_TYPES =
      {INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY};
  private static final Object[] EXPECTED_DEST_VALUES =
      {123, 123L, 123f, 123d, BigDecimal.valueOf(123), new Object[]{123}, new Object[]{123L}, new Object[]{123f},
          new Object[]{123d}};
  private static final String[] EXPECTED_STRING_VALUES = {
      Byte.toString((byte) 123), Character.toString((char) 123), Short.toString((short) 123), Integer.toString(123),
      Long.toString(123L), Float.toString(123f), Double.toString(123d),
      (BigDecimal.valueOf(123)).toPlainString(), " 123", "123 ", Byte.toString((byte) 123),
      Character.toString((char) 123), Short.toString((short) 123), Integer.toString(123), Long.toString(123L),
      Float.toString(123f), Double.toString(123d), " 123"
  };

  // Test cases where array for MV column contains values of mixing types.
  private static final PinotDataType[] SOURCE_ARRAY_TYPES =
      {SHORT_ARRAY, INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY};
  private static final Object[] SOURCE_ARRAY_VALUES = new Object[]{(short) 123, 4, 5L, 6f, 7d, "8"};

  private static final PinotDataType[] DEST_ARRAY_TYPES = {INTEGER_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY};
  private static final Object[] EXPECTED_DEST_ARRAY_VALUES = {
      new Object[]{123, 4, 5, 6, 7, 8}, new Object[]{123L, 4L, 5L, 6L, 7L, 8L}, new Object[]{123f, 4f, 5f, 6f, 7f, 8f},
      new Object[]{123d, 4d, 5d, 6d, 7d, 8d}
  };

  private static final PinotDataType[] DEST_PRIMITIVE_ARRAY_TYPES =
      {PRIMITIVE_INT_ARRAY, PRIMITIVE_LONG_ARRAY, PRIMITIVE_FLOAT_ARRAY, PRIMITIVE_DOUBLE_ARRAY};
  private static final Object[] EXPECTED_DEST_PRIMITIVE_ARRAY_VALUES = {
      new int[]{123, 4, 5, 6, 7, 8}, new long[]{123L, 4L, 5L, 6L, 7L, 8L}, new float[]{123f, 4f, 5f, 6f, 7f, 8f},
      new double[]{
          123d, 4d, 5d, 6d, 7d, 8d
      }
  };

  @Test
  public void testNumberConversion() {
    int numDestTypes = DEST_TYPES.length;
    for (int i = 0; i < numDestTypes; i++) {
      PinotDataType destType = DEST_TYPES[i];
      Object expectedDestValue = EXPECTED_DEST_VALUES[i];
      int numSourceTypes = SOURCE_TYPES.length;
      for (int j = 0; j < numSourceTypes; j++) {
        Object actualDestValue = destType.convert(SOURCE_VALUES[j], SOURCE_TYPES[j]);
        if (expectedDestValue.getClass().equals(BigDecimal.class)
            || actualDestValue.getClass().equals(BigDecimal.class)) {
          // Note: Unlike compareTo() method, BigDecimal equals() method considers two BigDecimal objects equal only
          // if they are equal in value and scale, (thus 123.0 is not equal to 123 when compared by this method).
          assertTrue(actualDestValue.equals(expectedDestValue)
              || ((Comparable) expectedDestValue).compareTo(actualDestValue) == 0);
        } else {
          assertEquals(actualDestValue, expectedDestValue);
        }
      }
    }
  }

  @Test
  public void testConversionWithMixTypes() {
    for (int i = 0; i < DEST_ARRAY_TYPES.length; i++) {
      PinotDataType destType = DEST_ARRAY_TYPES[i];
      Object expectedDestValue = EXPECTED_DEST_ARRAY_VALUES[i];
      for (PinotDataType sourceArrayType : SOURCE_ARRAY_TYPES) {
        Object actualDestValue = destType.convert(SOURCE_ARRAY_VALUES, sourceArrayType);
        assertEquals(actualDestValue, expectedDestValue);
      }
    }

    for (int i = 0; i < DEST_PRIMITIVE_ARRAY_TYPES.length; i++) {
      PinotDataType destType = DEST_PRIMITIVE_ARRAY_TYPES[i];
      Object expectedDestValue = EXPECTED_DEST_PRIMITIVE_ARRAY_VALUES[i];
      for (PinotDataType sourceArrayType : SOURCE_ARRAY_TYPES) {
        Object actualDestValue = destType.convert(SOURCE_ARRAY_VALUES, sourceArrayType);
        assertEquals(actualDestValue, expectedDestValue);
      }
    }
  }

  @DataProvider
  public Object[][] numberFormatConversionErrors() {
    return new Object[][] {
        {INTEGER_ARRAY, LONG_ARRAY, new Object[]{"abc"}},
        {INTEGER_ARRAY, INTEGER_ARRAY, new Object[]{"abc"}},
        {INTEGER_ARRAY, PRIMITIVE_BOOLEAN_ARRAY, new Object[]{"abc"}}
    };
  }

  @Test(dataProvider = "numberFormatConversionErrors", expectedExceptions = NumberFormatException.class)
  public void testNumberFormatConversionErrors(PinotDataType sourceType, PinotDataType destType, Object value) {
    sourceType.convert(value, destType);
  }

  @DataProvider
  public Object[][] conversions() {
    return new Object[][]{
        {STRING_ARRAY, PRIMITIVE_BOOLEAN_ARRAY, new String[] {"true", "false"}, new boolean[] { true, false }},
        {PRIMITIVE_BOOLEAN_ARRAY, PRIMITIVE_BOOLEAN_ARRAY, new boolean[] { true, false },
            new boolean[] { true, false }},
        {STRING_ARRAY, BOOLEAN_ARRAY, new String[] {"true", "false"}, new Boolean[] { true, false }},
        {BOOLEAN_ARRAY, BOOLEAN_ARRAY, new Boolean[] { true, false }, new Boolean[] { true, false }},
        // Cross-form: PRIMITIVE_BOOLEAN_ARRAY -> BOOLEAN_ARRAY exercises the boolean[] path through
        // toObjectArray (which now handles primitive boolean[]).
        {PRIMITIVE_BOOLEAN_ARRAY, BOOLEAN_ARRAY, new boolean[] { true, false }, new Boolean[] { true, false }},
        {BOOLEAN_ARRAY, PRIMITIVE_BOOLEAN_ARRAY, new Boolean[] { true, false }, new boolean[] { true, false }},
        {LONG_ARRAY, TIMESTAMP_ARRAY, new long[] {1000000L, 2000000L},
            new Timestamp[] { new Timestamp(1000000L), new Timestamp(2000000L) }},
        {TIMESTAMP_ARRAY, TIMESTAMP_ARRAY, new Timestamp[] { new Timestamp(1000000L), new Timestamp(2000000L) },
        new Timestamp[] { new Timestamp(1000000L), new Timestamp(2000000L) }},
        {BYTES_ARRAY, BYTES_ARRAY, new byte[][] { "foo".getBytes(UTF_8), "bar".getBytes(UTF_8) },
            new byte[][] { "foo".getBytes(UTF_8), "bar".getBytes(UTF_8) }},
        {COLLECTION, STRING_ARRAY, Arrays.asList("test1", "test2"), new String[] {"test1", "test2"}},
        {COLLECTION, FLOAT_ARRAY, Arrays.asList(1.0f, 2.0f), new Float[] {1.0f, 2.0f}},
        {OBJECT_ARRAY, STRING_ARRAY, new Object[] {"test1", "test2"}, new String[] {"test1", "test2"}},
        {OBJECT_ARRAY, FLOAT_ARRAY, new Object[] {1.0f, 2.0f}, new Float[] {1.0f, 2.0f}},
    };
  }

  @Test(dataProvider = "conversions")
  public void testConversions(PinotDataType sourceType, PinotDataType destType, Object value, Object expectedValue) {
    assertEquals(destType.convert(value, sourceType), expectedValue);
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
    assertEquals(BYTES.convert("AAE=", JSON), new byte[]{0, 1});
    assertEquals(BYTES.convert(new Byte[]{0, 1}, BYTE_ARRAY), new byte[]{0, 1});
    assertEquals(BYTES.convert(new String[]{"0001"}, STRING_ARRAY), new byte[]{0, 1});
  }

  @Test
  public void testTimestamp() {
    Timestamp timestamp = new Timestamp(1620324238610L);
    assertEquals(TIMESTAMP.convert(timestamp.getTime(), LONG), timestamp);
    assertEquals(TIMESTAMP.convert(timestamp.toString(), STRING), timestamp);
    // JSON: numeric (epoch millis) and quoted ISO-8601 forms — both round-trip via Jackson.
    assertEquals(TIMESTAMP.convert(Long.toString(timestamp.getTime()), JSON), timestamp);
    assertEquals(TIMESTAMP.convert("\"" + Instant.ofEpochMilli(timestamp.getTime()) + "\"", JSON), timestamp);
  }

  @Test
  public void testDate() {
    // 2022-04-14 = 19_096 days since epoch. `LocalDate` is TZ-independent — round-trips identically in any
    // JVM timezone.
    LocalDate date = LocalDate.parse("2022-04-14");
    assertEquals(DATE.convert(19_096, INTEGER), date);
    assertEquals(DATE.convert(19_096L, LONG), date);
    assertEquals(DATE.convert("2022-04-14", STRING), date);
    // JSON: quoted ISO-8601 string parsed via Jackson.
    assertEquals(DATE.convert("\"2022-04-14\"", JSON), date);
    assertEquals(DATE.convert(new Timestamp(19_096L * 86_400_000L), TIMESTAMP), date);
    assertEquals(DATE.toInt(date), 19_096);
    assertEquals(DATE.toLong(date), 19_096L);
    assertEquals(DATE.toInternal(date), 19_096);
    assertEquals(DATE.toString(date), "2022-04-14");
  }

  @Test
  public void testDateArray() {
    LocalDate[] dates = {LocalDate.parse("2022-04-14"), LocalDate.parse("2024-01-01")};
    // Identity round-trip from MV DATE.
    assertEquals(DATE_ARRAY.convert(dates, DATE_ARRAY), dates);
    // STRING_ARRAY → DATE_ARRAY: per-element ISO-8601 parsing via DATE.convert.
    assertEquals(DATE_ARRAY.convert(new String[]{"2022-04-14", "2024-01-01"}, STRING_ARRAY), dates);
    // INTEGER_ARRAY → DATE_ARRAY: per-element epoch-day decoding.
    assertEquals(DATE_ARRAY.convert(new Integer[]{19_096, 19_723}, INTEGER_ARRAY), dates);
    // DATE_ARRAY → STRING_ARRAY: each element via DATE.toString.
    assertEquals(STRING_ARRAY.convert(dates, DATE_ARRAY), new String[]{"2022-04-14", "2024-01-01"});
    // Lookup: Object[] of LocalDate routes to DATE_ARRAY.
    assertEquals(getMultiValueType(dates[0]), DATE_ARRAY);
    // toInternal: Integer[] of epoch-days.
    assertEquals(DATE_ARRAY.toInternal(dates), new Integer[]{19_096, 19_723});
  }

  @Test
  public void testTime() {
    // 08:51:32 = 31_892_000 ms since midnight. `LocalTime` is TZ-independent, so this round-trips
    // identically in any JVM timezone.
    LocalTime time = LocalTime.parse("08:51:32");
    assertEquals(TIME.convert(31_892_000L, LONG), time);
    assertEquals(TIME.convert(31_892_000, INTEGER), time);
    assertEquals(TIME.convert("08:51:32", STRING), time);
    // JSON: quoted ISO-8601 string parsed via Jackson.
    assertEquals(TIME.convert("\"08:51:32\"", JSON), time);
    assertEquals(TIME.toLong(time), 31_892_000L);
    assertEquals(TIME.toInt(time), 31_892_000);
    assertEquals(TIME.toInternal(time), 31_892_000L);
    assertEquals(TIME.toString(time), "08:51:32");
  }

  @Test
  public void testTimeArray() {
    LocalTime[] times = {LocalTime.parse("08:51:32"), LocalTime.parse("12:00:00")};
    assertEquals(TIME_ARRAY.convert(times, TIME_ARRAY), times);
    assertEquals(TIME_ARRAY.convert(new String[]{"08:51:32", "12:00:00"}, STRING_ARRAY), times);
    // LONG_ARRAY → TIME_ARRAY: per-element millis-since-midnight decoding.
    assertEquals(TIME_ARRAY.convert(new Long[]{31_892_000L, 43_200_000L}, LONG_ARRAY), times);
    assertEquals(STRING_ARRAY.convert(times, TIME_ARRAY), new String[]{"08:51:32", "12:00"});
    assertEquals(getMultiValueType(times[0]), TIME_ARRAY);
    // toInternal: Long[] of millis-since-midnight.
    assertEquals(TIME_ARRAY.toInternal(times), new Long[]{31_892_000L, 43_200_000L});
  }

  @Test
  public void testUuid() {
    UUID uuid = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    String canonical = "550e8400-e29b-41d4-a716-446655440000";
    assertEquals(UUID.convert(uuid, UUID), uuid);
    assertEquals(UUID.convert(canonical, STRING), uuid);
    // JSON: quoted canonical UUID string — Jackson's UUIDDeserializer handles it.
    assertEquals(UUID.convert("\"" + canonical + "\"", JSON), uuid);
    // BYTES: 16-byte big-endian round-trip via UuidUtils.
    byte[] bytes = UUID.toBytes(uuid);
    assertEquals(bytes.length, 16);
    assertEquals(UUID.convert(bytes, BYTES), uuid);
    // toString / toInternal both return the canonical form (no FQN needed in canonical 8-4-4-4-12 layout).
    assertEquals(UUID.toString(uuid), canonical);
    assertEquals(UUID.toInternal(uuid), canonical);
  }

  @Test
  public void testUuidArray() {
    UUID u1 = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    UUID u2 = java.util.UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID[] uuids = {u1, u2};
    assertEquals(UUID_ARRAY.convert(uuids, UUID_ARRAY), uuids);
    // STRING_ARRAY → UUID_ARRAY: per-element parse.
    assertEquals(UUID_ARRAY.convert(
        new String[]{"550e8400-e29b-41d4-a716-446655440000", "00000000-0000-0000-0000-000000000001"}, STRING_ARRAY),
        uuids);
    // STRING_ARRAY destination: canonical strings.
    assertEquals(STRING_ARRAY.convert(uuids, UUID_ARRAY),
        new String[]{"550e8400-e29b-41d4-a716-446655440000", "00000000-0000-0000-0000-000000000001"});
    // BYTES_ARRAY destination: 16-byte big-endian per element. This is the path the bot flagged as broken
    // pre-fix (Object[] of UUID → BYTES_ARRAY went through OBJECT.toBytes which threw).
    byte[][] bytesArray = (byte[][]) BYTES_ARRAY.convert(uuids, UUID_ARRAY);
    assertEquals(bytesArray.length, 2);
    assertEquals(bytesArray[0].length, 16);
    assertEquals(UUID.convert(bytesArray[0], BYTES), u1);
    assertEquals(UUID.convert(bytesArray[1], BYTES), u2);
    // Lookup: Object[] of UUID routes to UUID_ARRAY.
    assertEquals(getMultiValueType(u1), UUID_ARRAY);
    // toInternal: String[] of canonical form.
    assertEquals(UUID_ARRAY.toInternal(uuids),
        new String[]{"550e8400-e29b-41d4-a716-446655440000", "00000000-0000-0000-0000-000000000001"});
  }

  @Test
  public void testJSON() {
    assertEquals(JSON.convert(false, BOOLEAN), "false");
    assertEquals(JSON.convert(true, BOOLEAN), "true");
    assertEquals(JSON.convert(new byte[]{0, 1}, BYTES), "\"AAE=\""); // Base64 encoding.
    assertEquals(JSON.convert("{\"bytes\":\"AAE=\",\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]},"
            + "\"timestamp\":1620324238610}", STRING),
        "{\"bytes\":\"AAE=\",\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]},"
            + "\"timestamp\":1620324238610}");
    assertEquals(JSON.convert(new Timestamp(1620324238610L), TIMESTAMP), "1620324238610");
    assertEquals(JSON.convert(LocalDate.of(2022, 2, 8), DATE), "\"2022-02-08\"");
    assertEquals(JSON.convert(LocalTime.of(12, 34, 56), TIME), "\"12:34:56\"");
    assertEquals(JSON.convert(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), UUID),
        "\"550e8400-e29b-41d4-a716-446655440000\"");
    // Nested case: a Map produced by ResultSetRecordExtractor.extractStruct can wrap LocalDate values.
    assertEquals(JSON.convert(Map.of("d", LocalDate.of(2022, 2, 8)), MAP), "{\"d\":\"2022-02-08\"}");
  }

  @Test
  public void testJSONArray()
      throws JsonProcessingException {
    assertEquals(JSON.convert(new Object[]{false}, BOOLEAN), "[false]");
    assertEquals(JSON.convert(new Object[]{true}, BOOLEAN), "[true]"); // Base64 encoding.
    assertEquals(JSON.convert(new Object[]{
        JsonUtils.stringToObject("{\"bytes\":\"AAE=\"}", Map.class),
            JsonUtils.stringToObject("{\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]}}",
                Map.class),
            JsonUtils.stringToObject("{\"timestamp\":1620324238610}", Map.class)}, JSON),
        "[{\"bytes\":\"AAE=\"},{\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]}},"
            + "{\"timestamp\":1620324238610}]");
    assertEquals(JSON.convert(new Object[]{}, JSON), "[]");
    assertEquals(JSON.convert(new Object[]{new Timestamp(1620324238610L)}, TIMESTAMP), "[1620324238610]");
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

    // check if a well formed JSON string string can be converted to JSON.
    assertEquals(OBJECT.toJson(getGenericTestObject()),
        "{\"bytes\":\"AAE=\",\"map\":{\"key1\":\"value\",\"key2\":null,\"array\":[-5.4,4,\"2\"]},"
            + "\"timestamp\":1620324238610}");

    // check if a Java string (which does not represent JSON) can be converted into JSON.
    assertEquals(OBJECT.toJson("test"), "\"test\"");
    assertEquals(OBJECT_ARRAY.getSingleValueType(), OBJECT);
    // Non-zero value is treated as true.
    assertTrue(OBJECT.toBoolean(1.1d));
    assertTrue(OBJECT.toBoolean(0.1d));
    assertFalse(OBJECT.toBoolean(0d));
    assertTrue(OBJECT.toBoolean(-0.1d));
  }

  @Test
  public void testGetSingleValueType() {
    assertEquals(getSingleValueType(1), INTEGER);
    assertEquals(getSingleValueType(1L), LONG);
    assertEquals(getSingleValueType(1.0f), FLOAT);
    assertEquals(getSingleValueType(1.0d), DOUBLE);
    assertEquals(getSingleValueType(BigDecimal.ONE), BIG_DECIMAL);
    assertEquals(getSingleValueType(Boolean.TRUE), BOOLEAN);
    assertEquals(getSingleValueType(new Timestamp(0L)), TIMESTAMP);
    assertEquals(getSingleValueType("foo"), STRING);
    assertEquals(getSingleValueType(new byte[]{0}), BYTES);
    assertEquals(getSingleValueType(new HashMap<>()), MAP);
    assertEquals(getSingleValueType(LocalDate.EPOCH), DATE);
    assertEquals(getSingleValueType(LocalTime.NOON), TIME);
    assertEquals(getSingleValueType(java.util.UUID.randomUUID()), UUID);
    assertEquals(getSingleValueType((byte) 1), BYTE);
    assertEquals(getSingleValueType('a'), CHARACTER);
    assertEquals(getSingleValueType((short) 1), SHORT);
    assertEquals(getSingleValueType(new Object()), OBJECT);

    // Vendor JDBC drivers commonly return Timestamp subclasses (e.g. BigQuery Simba's TimestampTz).
    // Subclasses must resolve to TIMESTAMP, not OBJECT.
    class VendorTimestamp extends Timestamp {
      VendorTimestamp(long time) {
        super(time);
      }
    }
    assertEquals(getSingleValueType(new VendorTimestamp(0L)), TIMESTAMP);
  }

  @Test
  public void testGetMultipleValueType() {
    assertEquals(getMultiValueType(1), INTEGER_ARRAY);
    assertEquals(getMultiValueType(1L), LONG_ARRAY);
    assertEquals(getMultiValueType(1.0f), FLOAT_ARRAY);
    assertEquals(getMultiValueType(1.0d), DOUBLE_ARRAY);
    assertEquals(getMultiValueType(BigDecimal.ONE), BIG_DECIMAL_ARRAY);
    assertEquals(getMultiValueType(Boolean.TRUE), BOOLEAN_ARRAY);
    assertEquals(getMultiValueType(new Timestamp(0L)), TIMESTAMP_ARRAY);
    assertEquals(getMultiValueType("foo"), STRING_ARRAY);
    assertEquals(getMultiValueType(new byte[]{0}), BYTES_ARRAY);
    assertEquals(getMultiValueType(LocalDate.EPOCH), DATE_ARRAY);
    assertEquals(getMultiValueType(LocalTime.NOON), TIME_ARRAY);
    assertEquals(getMultiValueType(java.util.UUID.randomUUID()), UUID_ARRAY);
    assertEquals(getMultiValueType((byte) 1), BYTE_ARRAY);
    assertEquals(getMultiValueType('a'), CHARACTER_ARRAY);
    assertEquals(getMultiValueType((short) 1), SHORT_ARRAY);
    assertEquals(getMultiValueType(new Object()), OBJECT_ARRAY);

    // Vendor JDBC drivers commonly return Timestamp subclasses (e.g. BigQuery Simba's TimestampTz).
    // Subclasses must resolve to TIMESTAMP_ARRAY, not OBJECT_ARRAY.
    class VendorTimestamp extends Timestamp {
      VendorTimestamp(long time) {
        super(time);
      }
    }
    assertEquals(getMultiValueType(new VendorTimestamp(0L)), TIMESTAMP_ARRAY);
  }

  private static Object getGenericTestObject() {
    Map<String, Object> map1 = new HashMap<>();
    map1.put("array", Arrays.asList(-5.4, 4, "2"));
    map1.put("key1", "value");
    map1.put("key2", null);

    Map<String, Object> map2 = new HashMap<>();
    map2.put("map", map1);
    map2.put("bytes", new byte[]{0, 1});
    map2.put("timestamp", new Timestamp(1620324238610L));

    return map2;
  }

  @Test
  public void testInvalidConversion() {
    // Single-value types that do NOT support BYTES conversion; STRING / JSON / BYTES / BIG_DECIMAL / UUID
    // each have their own valid byte-form encoding and are tested elsewhere.
    PinotDataType[] noBytesConversion = {
        BOOLEAN, BYTE, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, TIMESTAMP, DATE, TIME, OBJECT
    };
    for (PinotDataType sourceType : noBytesConversion) {
      assertInvalidConversion(null, sourceType, BYTES, UnsupportedOperationException.class);
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
      assertInvalidConversion(null, sourceType, BYTE_ARRAY, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, CHARACTER_ARRAY, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, SHORT_ARRAY, UnsupportedOperationException.class);
      assertInvalidConversion(null, sourceType, OBJECT_ARRAY, UnsupportedOperationException.class);
    }
  }

  private void assertInvalidConversion(Object value, PinotDataType sourceType, PinotDataType destType,
      Class<?> expectedExceptionType) {
    Object result;
    try {
      result = destType.convert(value, sourceType);
    } catch (Exception e) {
      if (e.getClass().equals(expectedExceptionType)) {
        return;
      }
      fail(String.format("Converting %s from %s to %s: expected %s but got %s",
          value, sourceType, destType, expectedExceptionType.getSimpleName(), e.getClass().getSimpleName()), e);
      return;
    }
    fail(String.format("Converting %s from %s to %s: expected %s but completed with %s",
        value, sourceType, destType, expectedExceptionType.getSimpleName(), result));
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
