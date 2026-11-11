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
package org.apache.pinot.plugin.inputformat.avro;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [AvroRecordExtractor] — see its class Javadoc for the full Avro source type → Java output type
/// matrix. Each test exercises one row of that matrix end-to-end against `GenericDatumReader` semantics.
/// Test order follows the Avro type list in the extractor's class javadoc: primitives → bytes → fixed →
/// enum → array → map → record → nullable union → logical types (default mode, then raw mode).
public class AvroRecordExtractorTest {

  private static final String COLUMN = "col";

  // === boolean ===

  @Test
  public void testBooleanPreserved() {
    Object result = extract(singleField(Schema.create(Type.BOOLEAN), true));
    assertEquals(result, true);
  }

  // === int ===

  @Test
  public void testIntegerPreserved() {
    Object result = extract(singleField(Schema.create(Type.INT), 42));
    assertEquals(result, 42);
  }

  // === long ===

  @Test
  public void testLongPreserved() {
    Object result = extract(singleField(Schema.create(Type.LONG), 1_588_469_340_000L));
    assertEquals(result, 1_588_469_340_000L);
  }

  // === float ===

  @Test
  public void testFloatPreserved() {
    Object result = extract(singleField(Schema.create(Type.FLOAT), 1.5f));
    assertEquals(result, 1.5f);
  }

  // === double ===

  @Test
  public void testDoublePreserved() {
    Object result = extract(singleField(Schema.create(Type.DOUBLE), 1.5d));
    assertEquals(result, 1.5d);
  }

  // === string (Utf8 / String → String) ===

  @Test
  public void testStringPreserved() {
    Object result = extract(singleField(Schema.create(Type.STRING), "hello"));
    assertEquals(result, "hello");
  }

  @Test
  public void testUtf8ConvertedToString() {
    // Avro's GenericDatumReader produces `Utf8` for `string` fields by default; `Utf8.toString()` decodes
    // it.
    Object result = extract(singleField(Schema.create(Type.STRING), new Utf8("hello")));
    assertEquals(result, "hello");
  }

  // === bytes (ByteBuffer → byte[]) ===

  @Test
  public void testByteBufferConvertedToByteArray() {
    ByteBuffer buf = ByteBuffer.wrap(new byte[]{1, 2, 3});
    Object result = extract(singleField(Schema.create(Type.BYTES), buf));
    assertEquals((byte[]) result, new byte[]{1, 2, 3});
  }

  @Test
  public void testReusedByteBufferIsRereadable() {
    // The extractor must not consume the source buffer's position — repeated extract calls on the same
    // buffer should keep producing the same byte[].
    byte[] content = new byte[100];
    ThreadLocalRandom.current().nextBytes(content);
    ByteBuffer buffer = ByteBuffer.wrap(content);
    for (int i = 0; i < 10; i++) {
      assertEquals((byte[]) extract(singleField(Schema.create(Type.BYTES), buffer)), content);
    }
  }

  // === fixed (GenericFixed → byte[]) ===

  @Test
  public void testGenericFixedConvertedToByteArray() {
    Schema fixed = Schema.createFixed("FixedSchema", "", "", 4);
    Object result = extract(singleField(fixed, new GenericData.Fixed(fixed, new byte[]{0, 1, 2, 3})));
    assertEquals((byte[]) result, new byte[]{0, 1, 2, 3});
  }

  // === enum (EnumSymbol → String) ===

  @Test
  public void testEnumExtractedAsString() {
    Schema enumSchema = Schema.createEnum("Color", null, null, List.of("RED", "GREEN", "BLUE"));
    Object result = extract(singleField(enumSchema, new GenericData.EnumSymbol(enumSchema, "GREEN")));
    assertEquals(result, "GREEN");
  }

  // === array<T> → Object[] ===

  @Test
  public void testArrayOfIntsExtractedAsArray() {
    Schema arraySchema = Schema.createArray(Schema.create(Type.INT));
    Object[] result = (Object[]) extract(singleField(arraySchema, List.of(1, 2, 3)));
    assertEquals(result, new Object[]{1, 2, 3});
  }

  @Test
  public void testArrayOfStringsExtractedAsArray() {
    Schema arraySchema = Schema.createArray(Schema.create(Type.STRING));
    Object[] result = (Object[]) extract(singleField(arraySchema, List.of("a", "b")));
    assertEquals(result, new Object[]{"a", "b"});
  }

  @Test
  public void testArrayOfRecordsExtractedAsArrayOfMaps() {
    Schema inner = Schema.createRecord("Inner", null, null, false);
    inner.setFields(List.of(
        new Field("s", Schema.create(Type.STRING), null, null),
        new Field("i", Schema.create(Type.INT), null, null)
    ));
    Schema arraySchema = Schema.createArray(inner);
    GenericRecord r1 = new GenericData.Record(inner);
    r1.put("s", "hello");
    r1.put("i", 1);
    GenericRecord r2 = new GenericData.Record(inner);
    r2.put("s", "world");
    r2.put("i", 2);
    Object[] result = (Object[]) extract(singleField(arraySchema, List.of(r1, r2)));
    assertEquals(result.length, 2);
    Map<?, ?> m0 = (Map<?, ?>) result[0];
    assertEquals(m0.get("s"), "hello");
    assertEquals(m0.get("i"), 1);
    Map<?, ?> m1 = (Map<?, ?>) result[1];
    assertEquals(m1.get("s"), "world");
    assertEquals(m1.get("i"), 2);
  }

  @Test
  public void testArrayPreservesNullElements() {
    // Avro arrays of `union[null, X]` can carry null elements. `Arrays.asList` (not `List.of`) since
    // `List.of` rejects null elements.
    Schema arraySchema = Schema.createArray(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.INT)));
    Object[] result = (Object[]) extract(singleField(arraySchema, Arrays.asList(1, null, 3)));
    assertEquals(result, new Object[]{1, null, 3});
  }

  @Test
  public void testEmptyArrayExtractedAsEmptyArray() {
    Schema arraySchema = Schema.createArray(Schema.create(Type.INT));
    Object[] result = (Object[]) extract(singleField(arraySchema, List.of()));
    assertEquals(result, new Object[]{});
  }

  // === map<string, T> → Map<String, Object> ===

  @Test
  public void testMapOfStringToInt() {
    Schema mapSchema = Schema.createMap(Schema.create(Type.INT));
    Map<?, ?> result = (Map<?, ?>) extract(singleField(mapSchema, Map.of("a", 1, "b", 2)));
    assertEquals(result.size(), 2);
    assertEquals(result.get("a"), 1);
    assertEquals(result.get("b"), 2);
  }

  @Test
  public void testMapPreservesNullValues() {
    // Avro map<union[null, int]> can carry null values per the spec; the extractor must preserve the entry
    // (key kept, value null) rather than dropping it.
    Schema valueSchema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.INT));
    Schema mapSchema = Schema.createMap(valueSchema);
    Map<String, Integer> input = new HashMap<>();
    input.put("a", 1);
    input.put("b", null);
    Map<?, ?> result = (Map<?, ?>) extract(singleField(mapSchema, input));
    assertEquals(result.size(), 2);
    assertEquals(result.get("a"), 1);
    assertNull(result.get("b"));
  }

  @Test
  public void testMapOfStringToRecord() {
    Schema inner = Schema.createRecord("Inner", null, null, false);
    inner.setFields(List.of(
        new Field("s", Schema.create(Type.STRING), null, null),
        new Field("i", Schema.create(Type.INT), null, null)
    ));
    Schema mapSchema = Schema.createMap(inner);
    GenericRecord apple = new GenericData.Record(inner);
    apple.put("s", "apple");
    apple.put("i", 1);
    GenericRecord orange = new GenericData.Record(inner);
    orange.put("s", "orange");
    orange.put("i", 2);
    Map<?, ?> result = (Map<?, ?>) extract(singleField(mapSchema, Map.of("fruit1", apple, "fruit2", orange)));
    assertEquals(result.size(), 2);
    Map<?, ?> r1 = (Map<?, ?>) result.get("fruit1");
    assertEquals(r1.get("s"), "apple");
    assertEquals(r1.get("i"), 1);
    Map<?, ?> r2 = (Map<?, ?>) result.get("fruit2");
    assertEquals(r2.get("s"), "orange");
    assertEquals(r2.get("i"), 2);
  }

  // === nested record → Map<String, Object> ===

  @Test
  public void testNestedRecordExtractedAsMap() {
    Schema inner = Schema.createRecord("Inner", null, null, false);
    inner.setFields(List.of(
        new Field("s", Schema.create(Type.STRING), null, null),
        new Field("i", Schema.create(Type.INT), null, null),
        new Field("doubles", Schema.createArray(Schema.create(Type.DOUBLE)), null, null)
    ));
    GenericRecord nested = new GenericData.Record(inner);
    nested.put("s", "hello");
    nested.put("i", 42);
    nested.put("doubles", List.of(1.1, 2.2));
    Map<?, ?> result = (Map<?, ?>) extract(singleField(inner, nested));
    assertEquals(result.get("s"), "hello");
    assertEquals(result.get("i"), 42);
    assertEquals((Object[]) result.get("doubles"), new Object[]{1.1, 2.2});
  }

  @Test
  public void testDeeplyNestedRecord() {
    Schema innermost = Schema.createRecord("Innermost", null, null, false);
    innermost.setFields(List.of(
        new Field("a", Schema.create(Type.INT), null, null),
        new Field("b", Schema.create(Type.LONG), null, null)
    ));
    Schema outer = Schema.createRecord("Outer", null, null, false);
    outer.setFields(List.of(
        new Field("scalar", Schema.create(Type.STRING), null, null),
        new Field("inner", innermost, null, null)
    ));
    GenericRecord innerRec = new GenericData.Record(innermost);
    innerRec.put("a", 100);
    innerRec.put("b", 1_588_469_340_000L);
    GenericRecord outerRec = new GenericData.Record(outer);
    outerRec.put("scalar", "hello");
    outerRec.put("inner", innerRec);
    Map<?, ?> result = (Map<?, ?>) extract(singleField(outer, outerRec));
    assertEquals(result.get("scalar"), "hello");
    Map<?, ?> innerMap = (Map<?, ?>) result.get("inner");
    assertEquals(innerMap.get("a"), 100);
    assertEquals(innerMap.get("b"), 1_588_469_340_000L);
  }

  // === union[null, X] ===

  @Test
  public void testNullInNullableUnion() {
    Schema nullable = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING));
    assertNull(extract(singleField(nullable, null)));
  }

  @Test
  public void testStringInNullableUnion() {
    Schema nullable = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING));
    Object result = extract(singleField(nullable, "hello"));
    assertEquals(result, "hello");
  }

  @Test
  public void testMultiBranchUnionDispatchesByRuntimeType() {
    // union[int, string] — branch is picked by the runtime Java type via Avro's standard
    // GenericData.resolveUnion, not by branch order.
    Schema union = Schema.createUnion(Schema.create(Type.INT), Schema.create(Type.STRING));
    assertEquals(extract(singleField(union, 42)), 42);
    assertEquals(extract(singleField(union, "hello")), "hello");
  }

  @Test
  public void testNullableMultiBranchUnionDispatchesByRuntimeType() {
    // union[null, int, string] — null short-circuits; non-null values dispatch by runtime type.
    Schema union = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.INT), Schema.create(Type.STRING));
    assertNull(extract(singleField(union, null)));
    assertEquals(extract(singleField(union, 42)), 42);
    assertEquals(extract(singleField(union, "hello")), "hello");
  }

  // === Logical types — default behavior (extractRawTimeValues = false) ===
  // Order follows the logical-type list in AvroRecordExtractor's class javadoc.

  @Test
  public void testDecimalLogicalTypePreservedAsBigDecimal() {
    BigDecimal value = new BigDecimal(1.999999999d, MathContext.DECIMAL64).setScale(10);
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "bytes", "logicalType": "decimal", "precision": 64, "scale": 10}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(value, schema, new Conversions.DecimalConversion()));
    assertEquals(extract(record), value);
  }

  @Test
  public void testTimestampMillisLogicalTypeAsTimestamp() {
    Instant instant = Instant.ofEpochMilli(1649924302123L);
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "timestamp-millis"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampMillisConversion()));
    assertEquals(extract(record), Timestamp.from(instant));
  }

  @Test
  public void testTimestampMicrosLogicalTypeAsTimestampPreservingSubMillis() {
    // Sub-millisecond precision (the trailing 987 micros) is preserved via Timestamp.setNanos.
    Instant instant = Instant.ofEpochMilli(1649924302123L).plus(987, ChronoUnit.MICROS);
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "timestamp-micros"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampMicrosConversion()));
    assertEquals(extract(record), Timestamp.from(instant));
  }

  @Test
  public void testTimestampNanosLogicalTypeAsTimestampPreservingNanos() {
    // Full nanosecond precision (the trailing 789 nanos beyond the micros boundary) is preserved via
    // Timestamp.setNanos.
    Instant instant = Instant.ofEpochSecond(1649924302L, 123_456_789L);
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "timestamp-nanos"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampNanosConversion()));
    assertEquals(extract(record), Timestamp.from(instant));
  }

  @Test
  public void testDateLogicalTypeAsLocalDate() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "int", "logicalType": "date"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(LocalDate.of(2022, 4, 14), schema, new TimeConversions.DateConversion()));
    assertEquals(extract(record), LocalDate.of(2022, 4, 14));
  }

  @Test
  public void testTimeMillisLogicalTypeAsLocalTime() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "int", "logicalType": "time-millis"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN,
        encodeLogical(LocalTime.parse("08:51:32.123"), schema, new TimeConversions.TimeMillisConversion()));
    assertEquals(extract(record), LocalTime.parse("08:51:32.123"));
  }

  @Test
  public void testTimeMicrosLogicalTypeAsLocalTime() {
    // Sub-millisecond `.987` tail of 08:51:32.123987 is preserved.
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "time-micros"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN,
        encodeLogical(LocalTime.parse("08:51:32.123987"), schema, new TimeConversions.TimeMicrosConversion()));
    assertEquals(extract(record), LocalTime.parse("08:51:32.123987"));
  }

  @Test
  public void testUuidStringLogicalTypeAsUuid() {
    UUID uuid = UUID.fromString("af68efc2-818a-42ac-96c3-ced5ca6585a2");
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "string", "logicalType": "uuid"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(uuid, schema, new Conversions.UUIDConversion()));
    assertEquals(extract(record), uuid);
  }

  @Test
  public void testUuidFixedLogicalTypeAsUuid() {
    UUID uuid = UUID.fromString("af68efc2-818a-42ac-96c3-ced5ca6585a2");
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "fixed", "name": "UUIDFixed", "size": 16, "logicalType": "uuid"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(uuid, schema, new Conversions.UUIDConversion()));
    assertEquals(extract(record), uuid);
  }

  @Test
  public void testUnknownLogicalTypeReturnsSameValue() {
    // A schema with a custom (unregistered) `logicalType` name — no conversion runs, the raw value passes
    // through and only the physical-type normalization (BYTES → byte[]) applies.
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "bytes", "logicalType": "custom-type"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, ByteBuffer.wrap(new byte[]{0, 1, 2, 3}));
    assertEquals((byte[]) extract(record), new byte[]{0, 1, 2, 3});
  }

  // === Logical types — extractRawTimeValues = true ===
  // Same logical-type order. Temporal types pass through as raw integers; decimal / uuid still convert.

  @Test
  public void testTimestampMillisLogicalTypeAsRawLongMillisWhenRaw() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "timestamp-millis"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    Instant instant = Instant.ofEpochMilli(1649924302123L);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampMillisConversion()));
    assertEquals(extractRaw(record), 1649924302123L);
  }

  @Test
  public void testTimestampMicrosLogicalTypeAsRawLongMicrosWhenRaw() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "timestamp-micros"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    Instant instant = Instant.ofEpochMilli(1649924302123L).plus(987, ChronoUnit.MICROS);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampMicrosConversion()));
    assertEquals(extractRaw(record), 1649924302123L * 1_000L + 987L);
  }

  @Test
  public void testTimestampNanosLogicalTypeAsRawLongNanosWhenRaw() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "timestamp-nanos"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    Instant instant = Instant.ofEpochSecond(1649924302L, 123_456_789L);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampNanosConversion()));
    assertEquals(extractRaw(record), 1649924302L * 1_000_000_000L + 123_456_789L);
  }

  @Test
  public void testDateLogicalTypeAsRawIntDaysWhenRaw() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "int", "logicalType": "date"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    LocalDate date = LocalDate.of(2022, 4, 14);
    record.put(COLUMN, encodeLogical(date, schema, new TimeConversions.DateConversion()));
    assertEquals(extractRaw(record), (int) date.toEpochDay());
  }

  @Test
  public void testTimeMillisLogicalTypeAsRawIntMillisWhenRaw() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "int", "logicalType": "time-millis"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    LocalTime time = LocalTime.parse("08:51:32.123");
    record.put(COLUMN, encodeLogical(time, schema, new TimeConversions.TimeMillisConversion()));
    assertEquals(extractRaw(record), (int) (time.toNanoOfDay() / 1_000_000L));
  }

  @Test
  public void testTimeMicrosLogicalTypeAsRawLongMicrosWhenRaw() {
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "long", "logicalType": "time-micros"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    LocalTime time = LocalTime.parse("08:51:32.123987");
    record.put(COLUMN, encodeLogical(time, schema, new TimeConversions.TimeMicrosConversion()));
    assertEquals(extractRaw(record), time.toNanoOfDay() / 1_000L);
  }

  @Test
  public void testDecimalLogicalTypeStillConvertsWhenRaw() {
    // `extractRawTimeValues` only affects temporal logical types — `decimal` is always converted.
    BigDecimal value = new BigDecimal(1.999999999d, MathContext.DECIMAL64).setScale(10);
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "bytes", "logicalType": "decimal", "precision": 64, "scale": 10}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(value, schema, new Conversions.DecimalConversion()));
    assertEquals(extractRaw(record), value);
  }

  @Test
  public void testUuidLogicalTypeStillConvertsWhenRaw() {
    // `extractRawTimeValues` only affects temporal logical types — `uuid` is always converted.
    UUID uuid = UUID.fromString("af68efc2-818a-42ac-96c3-ced5ca6585a2");
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "string", "logicalType": "uuid"}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(uuid, schema, new Conversions.UUIDConversion()));
    assertEquals(extractRaw(record), uuid);
  }

  @Test
  public void testArrayOfTimestampsConvertedRecursivelyWhenRaw() {
    // Verifies that the walker recurses into ARRAY in raw mode — each inner timestamp-millis returns the
    // raw `Long` epoch millis (not a `Timestamp`).
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "array", "items": {"type": "long", "logicalType": "timestamp-millis"}}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, List.of(1_649_924_302_123L, 1_672_531_200_000L));
    Object[] result = (Object[]) extractRaw(record);
    assertEquals(result, new Object[]{1_649_924_302_123L, 1_672_531_200_000L});
  }

  @Test
  public void testMapOfTimestampsConvertedRecursivelyWhenRaw() {
    // Verifies that the walker recurses into MAP in raw mode — values pass through as raw `Long` epoch millis.
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [{
            "name": "col",
            "type": {"type": "map", "values": {"type": "long", "logicalType": "timestamp-millis"}}
          }]
        }
        """);
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, Map.of(
        "a", 1_649_924_302_123L,
        "b", 1_672_531_200_000L
    ));
    Map<?, ?> result = (Map<?, ?>) extractRaw(record);
    assertEquals(result.size(), 2);
    assertEquals(result.get("a"), 1_649_924_302_123L);
    assertEquals(result.get("b"), 1_672_531_200_000L);
  }

  // === End-to-end nested schema ===

  @Test
  public void testComplexNestedSchema() {
    // End-to-end exercise of every recursive path: top-level uuid, array<record<timestamp, map<decimal>>>,
    // array<decimal>, map<record>. Catches any single-path regression that the per-component tests miss.
    Schema schema = parseSchema("""
        {
          "type": "record",
          "name": "R",
          "fields": [
            {
              "name": "uuid",
              "type": {"type": "string", "logicalType": "uuid"}
            },
            {
              "name": "points",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "Point",
                  "fields": [
                    {
                      "name": "timestamp",
                      "type": {"type": "long", "logicalType": "timestamp-millis"}
                    },
                    {
                      "name": "labels",
                      "type": {
                        "type": "map",
                        "values": {"type": "bytes", "logicalType": "decimal", "precision": 22, "scale": 3}
                      }
                    }
                  ]
                }
              }
            },
            {
              "name": "decimals",
              "type": {
                "type": "array",
                "items": {"type": "bytes", "logicalType": "decimal", "precision": 22, "scale": 3}
              }
            },
            {
              "name": "attrs",
              "type": {
                "type": "map",
                "values": {
                  "type": "record",
                  "name": "Attr",
                  "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "verified", "type": "boolean"}
                  ]
                }
              }
            }
          ]
        }
        """);
    Conversions.UUIDConversion uuidConv = new Conversions.UUIDConversion();
    Conversions.DecimalConversion decimalConv = new Conversions.DecimalConversion();
    TimeConversions.TimestampMillisConversion tsConv = new TimeConversions.TimestampMillisConversion();

    UUID uuid = UUID.fromString("1bca8360-894c-47b3-93b0-515e2c5877ce");
    BigDecimal decimal1 = new BigDecimal("125.243");
    BigDecimal decimal2 = new BigDecimal("125.531");
    Schema uuidSchema = schema.getField("uuid").schema();
    Schema pointSchema = schema.getField("points").schema().getElementType();
    Schema timestampSchema = pointSchema.getField("timestamp").schema();
    // Same decimal logical-type schema appears twice (labels map values + decimals array items); the
    // encoded bytes don't carry a schema reference, so one variable serves both encoding sites.
    Schema decimalSchema = schema.getField("decimals").schema().getElementType();
    Schema attrSchema = schema.getField("attrs").schema().getValueType();

    GenericRecord point = new GenericData.Record(pointSchema);
    point.put("timestamp", Conversions.convertToRawType(Instant.ofEpochMilli(1609459200000L), timestampSchema,
        timestampSchema.getLogicalType(), tsConv));
    point.put("labels", Map.of(
        "L1", Conversions.convertToRawType(decimal1, decimalSchema, decimalSchema.getLogicalType(), decimalConv),
        "L2", Conversions.convertToRawType(decimal2, decimalSchema, decimalSchema.getLogicalType(), decimalConv)
    ));

    GenericRecord attr = new GenericData.Record(attrSchema);
    attr.put("name", "size");
    attr.put("verified", true);

    GenericRecord record = new GenericData.Record(schema);
    record.put("uuid", Conversions.convertToRawType(uuid, uuidSchema, uuidSchema.getLogicalType(), uuidConv));
    record.put("points", List.of(point));
    record.put("decimals", List.of(
        Conversions.convertToRawType(decimal1, decimalSchema, decimalSchema.getLogicalType(), decimalConv),
        Conversions.convertToRawType(decimal2, decimalSchema, decimalSchema.getLogicalType(), decimalConv)));
    record.put("attrs", Map.of("size", attr));

    AvroRecordExtractor extractor = new AvroRecordExtractor();
    extractor.init(null, new AvroRecordExtractorConfig());
    GenericRow row = new GenericRow();
    extractor.extract(record, row);

    assertEquals(row.getValue("uuid"), uuid);

    Object[] points = (Object[]) row.getValue("points");
    assertEquals(points.length, 1);
    Map<?, ?> point0 = (Map<?, ?>) points[0];
    assertEquals(point0.get("timestamp"), Timestamp.from(Instant.ofEpochMilli(1609459200000L)));
    Map<?, ?> point0Labels = (Map<?, ?>) point0.get("labels");
    assertEquals(point0Labels.get("L1"), decimal1);
    assertEquals(point0Labels.get("L2"), decimal2);

    Object[] decimals = (Object[]) row.getValue("decimals");
    assertEquals(decimals, new Object[]{decimal1, decimal2});

    Map<?, ?> attrs = (Map<?, ?>) row.getValue("attrs");
    Map<?, ?> sizeAttr = (Map<?, ?>) attrs.get("size");
    assertEquals(sizeAttr.get("name"), "size");
    assertEquals(sizeAttr.get("verified"), true);
  }

  // === Helpers ===

  /// Builds a single-field [GenericRecord] with `value` set on `fieldSchema`.
  private static GenericRecord singleField(Schema fieldSchema, Object value) {
    Schema record = Schema.createRecord("R", null, null, false);
    record.setFields(List.of(new Field(COLUMN, fieldSchema, null, null)));
    GenericRecord r = new GenericData.Record(record);
    r.put(COLUMN, value);
    return r;
  }

  private static Object extract(GenericRecord record) {
    return extract(record, new AvroRecordExtractorConfig());
  }

  private static Object extractRaw(GenericRecord record) {
    AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
    config.setExtractRawTimeValues(true);
    return extract(record, config);
  }

  private static Object extract(GenericRecord record, AvroRecordExtractorConfig config) {
    AvroRecordExtractor extractor = new AvroRecordExtractor();
    extractor.init(null, config);
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row.getValue(COLUMN);
  }

  private static Schema parseSchema(String json) {
    return new Schema.Parser().parse(json);
  }

  /// Converts a logical-typed Java value into its raw avro storage form (e.g. `BigDecimal` → `ByteBuffer`).
  /// The extractor's [AvroRecordExtractor#convertSingleValue] step runs the inverse conversion; this helper sets up
  /// records as they would arrive from `GenericDatumReader` *before* the logical-type pass.
  private static <T> Object encodeLogical(T value, Schema recordSchema, Conversion<T> conversion) {
    Schema fieldSchema = recordSchema.getField(COLUMN).schema();
    return Conversions.convertToRawType(value, fieldSchema, fieldSchema.getLogicalType(), conversion);
  }
}
