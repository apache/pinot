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
package org.apache.pinot.plugin.inputformat.parquet;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [ParquetNativeRecordExtractor] — see its class Javadoc for the Parquet primitive + logical-type
/// annotation → Java output type matrix.
public class ParquetNativeRecordExtractorTest {

  private static final String COLUMN = "col";

  // === Primitives — order follows the parquet type list in the class Javadoc ===

  @Test
  public void testBooleanPreserved() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required boolean col; }");
    assertEquals(extract(schema, g -> g.add(0, true)), true);
  }

  @Test
  public void testIntegerPreserved() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int32 col; }");
    assertEquals(extract(schema, g -> g.add(0, 42)), 42);
  }

  @Test
  public void testLongPreserved() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int64 col; }");
    assertEquals(extract(schema, g -> g.add(0, 1_588_469_340_000L)), 1_588_469_340_000L);
  }

  @Test
  public void testInt96ExtractedAsTimestamp() {
    long epochMillis = 1_649_924_302_123L;
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int96 col; }");
    assertEquals(extract(schema, g -> g.add(0, Binary.fromConstantByteArray(int96Bytes(epochMillis)))),
        new Timestamp(epochMillis));
  }

  @Test
  public void testFloatPreserved() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required float col; }");
    assertEquals(extract(schema, g -> g.add(0, 1.5f)), 1.5f);
  }

  @Test
  public void testDoublePreserved() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required double col; }");
    assertEquals(extract(schema, g -> g.add(0, 2.5d)), 2.5d);
  }

  // === Binary with logical-type annotations ===

  @Test
  public void testStringExtractedAsString() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required binary col (STRING); }");
    assertEquals(extract(schema, g -> g.add(0, Binary.fromString("hello"))), "hello");
  }

  @Test
  public void testEnumExtractedAsString() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required binary col (ENUM); }");
    assertEquals(extract(schema, g -> g.add(0, Binary.fromString("RED"))), "RED");
  }

  @Test
  public void testBinaryExtractedAsByteArray() {
    byte[] bytes = {0, 1, 2, 3};
    MessageType schema = MessageTypeParser.parseMessageType("message R { required binary col; }");
    assertEquals((byte[]) extract(schema, g -> g.add(0, Binary.fromConstantByteArray(bytes))), bytes);
  }

  @Test
  public void testFixedLenByteArrayExtractedAsByteArray() {
    byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7};
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required fixed_len_byte_array(8) col; }");
    assertEquals((byte[]) extract(schema, g -> g.add(0, Binary.fromConstantByteArray(bytes))), bytes);
  }

  // === Decimal logical type ===

  @Test
  public void testInt32DecimalExtractedAsBigDecimal() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required int32 col (DECIMAL(9, 2)); }");
    // 12345 with scale 2 → 123.45
    assertEquals(extract(schema, g -> g.add(0, 12345)), new BigDecimal("123.45"));
  }

  @Test
  public void testInt64DecimalExtractedAsBigDecimal() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required int64 col (DECIMAL(18, 3)); }");
    // 123456 with scale 3 → 123.456
    assertEquals(extract(schema, g -> g.add(0, 123456L)), new BigDecimal("123.456"));
  }

  @Test
  public void testBinaryDecimalExtractedAsBigDecimal() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required binary col (DECIMAL(38, 5)); }");
    // Two's-complement big-endian bytes for 12345000.
    byte[] unscaled = new BigDecimal("123.45000").unscaledValue().toByteArray();
    assertEquals(extract(schema, g -> g.add(0, Binary.fromConstantByteArray(unscaled))),
        new BigDecimal("123.45000"));
  }

  // === Date / Time / Timestamp logical types ===

  @Test
  public void testDateExtractedAsLocalDate() {
    // `DATE` (int32 days-since-epoch) → [LocalDate] (TZ-independent).
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int32 col (DATE); }");
    assertEquals(extract(schema, g -> g.add(0, 19_096)), LocalDate.of(2022, 4, 14));
  }

  @Test
  public void testTimeMillisExtractedAsLocalTime() {
    // `TIME_MILLIS` (int32 millis-since-midnight) → [LocalTime] at millis precision.
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required int32 col (TIME_MILLIS); }");
    assertEquals(extract(schema, g -> g.add(0, 31_892_123)), LocalTime.parse("08:51:32.123"));
  }

  @Test
  public void testTimeMicrosExtractedAsLocalTime() {
    // `TIME_MICROS` (int64 micros-since-midnight) → [LocalTime] preserving full microsecond precision.
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required int64 col (TIME_MICROS); }");
    assertEquals(extract(schema, g -> g.add(0, 31_892_123_987L)), LocalTime.parse("08:51:32.123987"));
  }

  @Test
  public void testTimestampMillisExtractedAsTimestamp() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int64 col (TIMESTAMP_MILLIS); }");
    assertEquals(extract(schema, g -> g.add(0, 1_649_924_302_123L)), new Timestamp(1_649_924_302_123L));
  }

  @Test
  public void testTimestampMicrosExtractedAsTimestampPreservingSubMillis() {
    // `TIMESTAMP_MICROS` (int64) → [Timestamp]. Sub-millisecond precision (123_987 micros) is preserved via
    // setNanos.
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int64 col (TIMESTAMP_MICROS); }");
    Timestamp expected = new Timestamp(1_649_924_302_123L);
    expected.setNanos(123_987_000);
    assertEquals(extract(schema, g -> g.add(0, 1_649_924_302_123_987L)), expected);
  }

  @Test
  public void testTimestampMicrosNegativeRoundsConsistently() {
    // Pre-epoch micros must use floor semantics so the divide doesn't shift the result by a full second.
    // -2_000_500 micros = -2.0005 seconds → -3 seconds + 999_500_000 nanos. Timestamp uses ms epoch (-3000)
    // plus the leftover sub-second nanos (999_500_000).
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int64 col (TIMESTAMP_MICROS); }");
    Timestamp expected = new Timestamp(-3_000L);
    expected.setNanos(999_500_000);
    assertEquals(extract(schema, g -> g.add(0, -2_000_500L)), expected);
  }

  @Test
  public void testTimestampNanosNegativeRoundsConsistently() {
    // Same floor-semantics concern for `TIMESTAMP_NANOS`. -2_000_500_000 nanos = -2.0005 seconds → -3 seconds
    // + 999_500_000 nanos. `MessageTypeParser` only understands the legacy `OriginalType` set (which tops
    // out at `TIMESTAMP_MICROS`); construct the schema via the `Types` builder to attach the modern
    // `TimestampLogicalTypeAnnotation` with `NANOS` precision.
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.INT64)
        .as(LogicalTypeAnnotation.timestampType(/* isAdjustedToUTC = */ true, TimeUnit.NANOS))
        .named(COLUMN)
        .named("R");
    Timestamp expected = new Timestamp(-3_000L);
    expected.setNanos(999_500_000);
    assertEquals(extract(schema, g -> g.add(0, -2_000_500_000L)), expected);
  }

  // === UUID logical type ===

  @Test
  public void testUuidExtractedAsUuid() {
    UUID uuid = UUID.fromString("af68efc2-818a-42ac-96c3-ced5ca6585a2");
    byte[] uuidBytes = UuidUtils.toBytes(uuid);
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(16)
        .as(LogicalTypeAnnotation.uuidType())
        .named(COLUMN)
        .named("R");
    assertEquals(extract(schema, g -> g.add(0, Binary.fromConstantByteArray(uuidBytes))), uuid);
  }

  // === extractRawTimeValues = true ===

  @Test
  public void testDateExtractedAsRawIntDaysWhenRaw() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int32 col (DATE); }");
    assertEquals(extract(schema, g -> g.add(0, 19_096), rawConfig()), 19_096);
  }

  @Test
  public void testTimeMillisExtractedAsRawIntMillisWhenRaw() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required int32 col (TIME_MILLIS); }");
    assertEquals(extract(schema, g -> g.add(0, 31_892_123), rawConfig()), 31_892_123);
  }

  @Test
  public void testTimeMicrosExtractedAsRawLongMicrosWhenRaw() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message R { required int64 col (TIME_MICROS); }");
    assertEquals(extract(schema, g -> g.add(0, 31_892_123_987L), rawConfig()), 31_892_123_987L);
  }

  @Test
  public void testTimestampMillisExtractedAsRawLongMillisWhenRaw() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int64 col (TIMESTAMP_MILLIS); }");
    assertEquals(extract(schema, g -> g.add(0, 1_649_924_302_123L), rawConfig()), 1_649_924_302_123L);
  }

  @Test
  public void testTimestampMicrosExtractedAsRawLongMicrosWhenRaw() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int64 col (TIMESTAMP_MICROS); }");
    assertEquals(extract(schema, g -> g.add(0, 1_649_924_302_123_987L), rawConfig()), 1_649_924_302_123_987L);
  }

  @Test
  public void testInt96ExtractedAsRawLongEpochNanosWhenRaw() {
    // INT96 has no Parquet logical-type spec, but its physical encoding (nanos-of-day + Julian day) carries
    // nanosecond precision, so nanos is the natural raw unit.
    long epochMillis = 1_649_924_302_123L;
    MessageType schema = MessageTypeParser.parseMessageType("message R { required int96 col; }");
    assertEquals(extract(schema, g -> g.add(0, Binary.fromConstantByteArray(int96Bytes(epochMillis))), rawConfig()),
        epochMillis * 1_000_000L);
  }

  // === LIST — standard 3-level wrapper and legacy forms ===

  @Test
  public void testStandardListExtractedAsArray() {
    // Standard 3-level LIST encoding (parquet-avro / Spark / Arrow):
    //   group col (LIST) { repeated group list { required int32 element; } }
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col (LIST) { repeated group list { required int32 element; } } }");
    Object[] result = (Object[]) extract(schema, g -> {
      Group list = g.addGroup(0);
      list.addGroup(0).add(0, 10);
      list.addGroup(0).add(0, 20);
      list.addGroup(0).add(0, 30);
    });
    assertEquals(result, new Object[]{10, 20, 30});
  }

  @Test
  public void testLegacyRepeatedPrimitiveExtractedAsArray() {
    // Legacy rule 1: repeated primitive — the primitive IS the element, no wrapper.
    MessageType schema = MessageTypeParser.parseMessageType("message R { repeated int32 col; }");
    Object[] result = (Object[]) extract(schema, g -> {
      g.add(0, 10);
      g.add(0, 20);
    });
    assertEquals(result, new Object[]{10, 20});
  }

  @Test
  public void testLegacyRepeatedPrimitiveWithSingleElementExtractedAsArray() {
    // Cardinality-1 case: REPEATED still surfaces as `Object[]`, never as a bare scalar.
    MessageType schema = MessageTypeParser.parseMessageType("message R { repeated int32 col; }");
    Object[] result = (Object[]) extract(schema, g -> g.add(0, 10));
    assertEquals(result, new Object[]{10});
  }

  @Test
  public void testLegacyRepeatedPrimitiveEmptyExtractedAsEmptyArray() {
    // Cardinality-0 case: empty REPEATED is an empty `Object[]`, not `null`.
    MessageType schema = MessageTypeParser.parseMessageType("message R { repeated int32 col; }");
    Object[] result = (Object[]) extract(schema, g -> { });
    assertEquals(result, new Object[]{});
  }

  @Test
  public void testListPreservesNullElements() {
    // Standard 3-level LIST with `optional` element — null elements stay null in `Object[]`.
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col (LIST) { repeated group list { optional int32 element; } } }");
    Object[] result = (Object[]) extract(schema, g -> {
      Group list = g.addGroup(0);
      list.addGroup(0).add(0, 10);
      list.addGroup(0); // element absent → null
      list.addGroup(0).add(0, 30);
    });
    assertEquals(result, new Object[]{10, null, 30});
  }

  @Test
  public void testEmptyListExtractedAsEmptyArray() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col (LIST) { repeated group list { required int32 element; } } }");
    Object[] result = (Object[]) extract(schema, g -> g.addGroup(0));
    assertEquals(result, new Object[]{});
  }

  // === MAP ===

  @Test
  public void testMapStringToInt() {
    // Standard MAP encoding: a group annotated MAP with one repeated key_value child carrying key + value.
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col (MAP) { "
        + "  repeated group key_value { "
        + "    required binary key (STRING); "
        + "    required int32 value; "
        + "  } "
        + "} }");
    Object result = extract(schema, g -> {
      Group map = g.addGroup(0);
      Group entry1 = map.addGroup(0);
      entry1.add(0, Binary.fromString("a"));
      entry1.add(1, 1);
      Group entry2 = map.addGroup(0);
      entry2.add(0, Binary.fromString("b"));
      entry2.add(1, 2);
    });
    Map<?, ?> resultMap = (Map<?, ?>) result;
    assertEquals(resultMap.size(), 2);
    assertEquals(resultMap.get("a"), 1);
    assertEquals(resultMap.get("b"), 2);
  }

  @Test
  public void testEmptyMapExtractedAsEmptyMap() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col (MAP) { "
        + "  repeated group key_value { "
        + "    required binary key (STRING); "
        + "    required int32 value; "
        + "  } "
        + "} }");
    Object result = extract(schema, g -> g.addGroup(0));
    assertEquals(((Map<?, ?>) result).size(), 0);
  }

  @Test
  public void testMapBinaryKeyBase64Encoded() {
    // Unannotated BINARY keys surface as `byte[]`; we base64-encode rather than letting `byte[].toString()`
    // produce `[B@<hash>` garbage.
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col (MAP) { "
        + "  repeated group key_value { "
        + "    required binary key; "
        + "    required int32 value; "
        + "  } "
        + "} }");
    Object result = extract(schema, g -> {
      Group map = g.addGroup(0);
      Group entry = map.addGroup(0);
      entry.add(0, Binary.fromConstantByteArray(new byte[]{0, 1, 2, 3}));
      entry.add(1, 42);
    });
    Map<?, ?> resultMap = (Map<?, ?>) result;
    assertEquals(resultMap.size(), 1);
    assertEquals(resultMap.get("AAECAw=="), 42);  // base64 of {0, 1, 2, 3}
  }

  // === Struct (un-annotated group) ===

  @Test
  public void testStructExtractedAsMap() {
    MessageType schema = MessageTypeParser.parseMessageType("message R { "
        + "required group col { required binary s (STRING); required int32 i; } }");
    Object result = extract(schema, g -> {
      Group struct = g.addGroup(0);
      struct.add(0, Binary.fromString("hello"));
      struct.add(1, 42);
    });
    Map<?, ?> resultMap = (Map<?, ?>) result;
    assertEquals(resultMap.get("s"), "hello");
    assertEquals(resultMap.get("i"), 42);
  }

  // === Null / unset ===

  @Test
  public void testUnsetOptionalFieldReturnsNull() {
    // Optional field left unset → repetition count is 0 → extractor surfaces null.
    MessageType schema = MessageTypeParser.parseMessageType("message R { optional int32 col; }");
    assertNull(extract(schema, g -> { /* don't add */ }));
  }

  // === Helpers ===

  private static Object extract(MessageType schema, Consumer<SimpleGroup> populator) {
    return extract(schema, populator, null);
  }

  private static Object extract(MessageType schema, Consumer<SimpleGroup> populator,
      ParquetNativeRecordExtractorConfig config) {
    SimpleGroup group = new SimpleGroup(schema);
    populator.accept(group);
    ParquetNativeRecordExtractor extractor = new ParquetNativeRecordExtractor();
    extractor.init(null, config);
    GenericRow row = new GenericRow();
    extractor.extract(group, row);
    return row.getValue(COLUMN);
  }

  private static ParquetNativeRecordExtractorConfig rawConfig() {
    ParquetNativeRecordExtractorConfig config = new ParquetNativeRecordExtractorConfig();
    config.setExtractRawTimeValues(true);
    return config;
  }

  /// Encodes `epochMillis` as an INT96 byte[12]: bytes 0..7 = nanos-within-day (long, little-endian),
  /// bytes 8..11 = Julian day number (int, little-endian).
  private static byte[] int96Bytes(long epochMillis) {
    long millisPerDay = 86_400_000L;
    long dayNumber = epochMillis / millisPerDay;
    long nanosWithinDay = (epochMillis - dayNumber * millisPerDay) * 1_000_000L;
    int julianDay = (int) (dayNumber + ParquetUtils.JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH);
    return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
        .putLong(nanosWithinDay).putInt(julianDay).array();
  }
}
