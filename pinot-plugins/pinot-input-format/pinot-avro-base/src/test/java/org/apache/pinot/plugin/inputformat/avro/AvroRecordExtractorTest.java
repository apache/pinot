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
public class AvroRecordExtractorTest {

  private static final String COLUMN = "col";

  // === Single-value primitives — order follows the avro type list in the class Javadoc ===

  @Test
  public void testBooleanPreserved() {
    Object result = extract(singleField(Schema.create(Type.BOOLEAN), true));
    assertEquals(result, true);
  }

  @Test
  public void testIntegerPreserved() {
    Object result = extract(singleField(Schema.create(Type.INT), 42));
    assertEquals(result, 42);
  }

  @Test
  public void testLongPreserved() {
    Object result = extract(singleField(Schema.create(Type.LONG), 1_588_469_340_000L));
    assertEquals(result, 1_588_469_340_000L);
  }

  @Test
  public void testFloatPreserved() {
    Object result = extract(singleField(Schema.create(Type.FLOAT), 1.5f));
    assertEquals(result, 1.5f);
  }

  @Test
  public void testDoublePreserved() {
    Object result = extract(singleField(Schema.create(Type.DOUBLE), 1.5d));
    assertEquals(result, 1.5d);
  }

  @Test
  public void testStringPreserved() {
    Object result = extract(singleField(Schema.create(Type.STRING), "hello"));
    assertEquals(result, "hello");
  }

  @Test
  public void testUtf8ConvertedToString() {
    // Avro's GenericDatumReader produces `Utf8` for `string` fields by default. The extractor's
    // `convertSingleValue` falls back to `toString()` for unknown types, which materializes Utf8 → String.
    Object result = extract(singleField(Schema.create(Type.STRING), new Utf8("hello")));
    assertEquals(result, "hello");
  }

  // === Bytes / Fixed → byte[] ===

  @Test
  public void testByteBufferConvertedToByteArray() {
    ByteBuffer buf = ByteBuffer.wrap(new byte[]{1, 2, 3});
    Object result = extract(singleField(Schema.create(Type.BYTES), buf));
    assertEquals((byte[]) result, new byte[]{1, 2, 3});
  }

  @Test
  public void testReusedByteBufferIsRereadable() {
    // The extractor must not consume the source buffer's position — repeated calls should keep producing
    // the same byte[]. Tests that {@link AvroRecordExtractor#convertSingleValue} slices the buffer.
    byte[] content = new byte[100];
    ThreadLocalRandom.current().nextBytes(content);
    ByteBuffer buffer = ByteBuffer.wrap(content);
    AvroRecordExtractor extractor = new AvroRecordExtractor();
    for (int i = 0; i < 10; i++) {
      assertEquals((byte[]) extractor.convertSingleValue(buffer), content);
    }
  }

  @Test
  public void testGenericFixedConvertedToByteArray() {
    Schema fixed = Schema.createFixed("FixedSchema", "", "", 4);
    Object result = extract(singleField(fixed, new GenericData.Fixed(fixed, new byte[]{0, 1, 2, 3})));
    assertEquals((byte[]) result, new byte[]{0, 1, 2, 3});
  }

  // === Enum → String ===

  @Test
  public void testEnumExtractedAsString() {
    Schema enumSchema = Schema.createEnum("Color", null, null, List.of("RED", "GREEN", "BLUE"));
    Object result = extract(singleField(enumSchema, new GenericData.EnumSymbol(enumSchema, "GREEN")));
    assertEquals(result, "GREEN");
  }

  // === Array (avro list) → Object[] ===

  @Test
  public void testArrayOfIntsExtractedAsArray() {
    Schema arraySchema = Schema.createArray(Schema.create(Type.INT));
    Object[] result = (Object[]) extract(singleField(arraySchema, Arrays.asList(1, 2, 3)));
    assertEquals(result, new Object[]{1, 2, 3});
  }

  @Test
  public void testArrayOfStringsExtractedAsArray() {
    Schema arraySchema = Schema.createArray(Schema.create(Type.STRING));
    Object[] result = (Object[]) extract(singleField(arraySchema, Arrays.asList("a", "b")));
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
    Object[] result = (Object[]) extract(singleField(arraySchema, Arrays.asList(r1, r2)));
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
    // Avro arrays of `union[null, X]` can carry null elements; the extractor must preserve them in `Object[]`.
    Schema arraySchema = Schema.createArray(Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.INT)));
    List<Integer> input = Arrays.asList(1, null, 3);
    Object[] result = (Object[]) extract(singleField(arraySchema, input));
    assertEquals(result, new Object[]{1, null, 3});
  }

  @Test
  public void testEmptyArrayExtractedAsEmptyArray() {
    Schema arraySchema = Schema.createArray(Schema.create(Type.INT));
    Object[] result = (Object[]) extract(singleField(arraySchema, Arrays.asList()));
    assertEquals(result, new Object[]{});
  }

  // === Map (avro map<string, X>) ===

  @Test
  public void testMapOfStringToInt() {
    Schema mapSchema = Schema.createMap(Schema.create(Type.INT));
    Map<String, Integer> input = new HashMap<>();
    input.put("a", 1);
    input.put("b", 2);
    Map<?, ?> result = (Map<?, ?>) extract(singleField(mapSchema, input));
    assertEquals(result.size(), 2);
    assertEquals(result.get("a"), 1);
    assertEquals(result.get("b"), 2);
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
    Map<String, GenericRecord> input = new HashMap<>();
    input.put("fruit1", apple);
    input.put("fruit2", orange);
    Map<?, ?> result = (Map<?, ?>) extract(singleField(mapSchema, input));
    assertEquals(result.size(), 2);
    Map<?, ?> r1 = (Map<?, ?>) result.get("fruit1");
    assertEquals(r1.get("s"), "apple");
    assertEquals(r1.get("i"), 1);
    Map<?, ?> r2 = (Map<?, ?>) result.get("fruit2");
    assertEquals(r2.get("s"), "orange");
    assertEquals(r2.get("i"), 2);
  }

  // === Nested record → Map ===

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
    nested.put("doubles", Arrays.asList(1.1, 2.2));
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

  // === Null in `union[null, X]` ===

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

  // === Logical types (enableLogicalTypes is the config default = true) ===

  @Test
  public void testDecimalLogicalTypePreservedAsBigDecimal() {
    BigDecimal value = new BigDecimal(1.999999999d, MathContext.DECIMAL64).setScale(10);
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":64,\"scale\":10}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(value, schema, new Conversions.DecimalConversion()));
    Object result = extract(record);
    assertEquals(result, value);
  }

  @Test
  public void testUuidLogicalTypeAsString() {
    String uuidString = "af68efc2-818a-42ac-96c3-ced5ca6585a2";
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN,
        encodeLogical(UUID.fromString(uuidString), schema, new Conversions.UUIDConversion()));
    Object result = extract(record);
    assertEquals(result, uuidString);
  }

  @Test
  public void testDateLogicalTypeAsLocalDate() {
    // `date` → [LocalDate] (passes through unchanged; TZ-independent).
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN,
        encodeLogical(LocalDate.of(2022, 4, 14), schema, new TimeConversions.DateConversion()));
    Object result = extract(record);
    assertEquals(result, LocalDate.of(2022, 4, 14));
  }

  @Test
  public void testTimeMillisLogicalTypeAsLocalTime() {
    // `time-millis` → [LocalTime] passed through with millisecond precision.
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN,
        encodeLogical(LocalTime.parse("08:51:32.123"), schema, new TimeConversions.TimeMillisConversion()));
    Object result = extract(record);
    assertEquals(result, LocalTime.parse("08:51:32.123"));
  }

  @Test
  public void testTimeMicrosLogicalTypeAsLocalTime() {
    // `time-micros` → [LocalTime] passed through with full microsecond precision (sub-millisecond `.987`
    // tail of 08:51:32.123987 is preserved).
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN,
        encodeLogical(LocalTime.parse("08:51:32.123987"), schema, new TimeConversions.TimeMicrosConversion()));
    Object result = extract(record);
    assertEquals(result, LocalTime.parse("08:51:32.123987"));
  }

  @Test
  public void testTimestampMillisLogicalTypeAsTimestamp() {
    Instant instant = Instant.ofEpochMilli(1649924302123L);
    Timestamp expected = new Timestamp(1649924302000L);
    expected.setNanos(123000000);
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampMillisConversion()));
    Object result = extract(record);
    assertEquals(result, expected);
  }

  @Test
  public void testTimestampMicrosLogicalTypeAsTimestamp() {
    // Microsecond precision survives via `Timestamp.nanos`.
    Instant instant = Instant.ofEpochMilli(1649924302123L).plus(987, ChronoUnit.MICROS);
    Timestamp expected = new Timestamp(1649924302000L);
    expected.setNanos(123987000);
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put(COLUMN, encodeLogical(instant, schema, new TimeConversions.TimestampMicrosConversion()));
    Object result = extract(record);
    assertEquals(result, expected);
  }

  @Test
  public void testLogicalTypesIgnoredWhenDisabled() {
    // With `enableLogicalTypes = false`, the extractor leaves raw avro storage values alone — a `decimal`
    // surfaces as the stored `ByteBuffer` (materialized to `byte[]`), not as `BigDecimal`.
    BigDecimal value = new BigDecimal(1.5d, MathContext.DECIMAL64).setScale(10);
    Schema schema = parseSchema(""
        + "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{"
        + "\"name\":\"" + COLUMN + "\","
        + "\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":64,\"scale\":10}"
        + "}]}");
    GenericRecord record = new GenericData.Record(schema);
    ByteBuffer rawBytes = (ByteBuffer) encodeLogical(value, schema, new Conversions.DecimalConversion());
    record.put(COLUMN, rawBytes);
    Object result = extract(record, /* enableLogicalTypes = */ false);
    // ByteBuffer is materialized to `byte[]` by the base extractor regardless of logical-type config.
    byte[] expected = new byte[rawBytes.remaining()];
    rawBytes.duplicate().get(expected);
    assertEquals((byte[]) result, expected);
  }

  // === Helpers ===

  /// Build a single-field [GenericRecord] with `value` set on `fieldSchema`. Array and map field values must be
  /// mutable (e.g. `Arrays.asList(...)` / `new HashMap<>()` populated with `put`) — [AvroSchemaUtil#applyLogicalType]
  /// walks them in-place even when no logical type is present, so immutable `List.of` / `Map.of` would throw.
  private static GenericRecord singleField(Schema fieldSchema, Object value) {
    Schema record = Schema.createRecord("R", null, null, false);
    record.setFields(List.of(new Field(COLUMN, fieldSchema, null, null)));
    GenericRecord r = new GenericData.Record(record);
    r.put(COLUMN, value);
    return r;
  }

  private static Object extract(GenericRecord record) {
    return extract(record, /* enableLogicalTypes = */ true);
  }

  private static Object extract(GenericRecord record, boolean enableLogicalTypes) {
    AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
    config.setEnableLogicalTypes(enableLogicalTypes);
    AvroRecordExtractor extractor = new AvroRecordExtractor();
    extractor.init(null, config);
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row.getValue(COLUMN);
  }

  private static Schema parseSchema(String json) {
    return new Schema.Parser().parse(json);
  }

  /// Convert a logical-typed Java value into its raw avro storage form (e.g. `BigDecimal` → `ByteBuffer`).
  /// The extractor's `applyLogicalType` step runs the inverse conversion; this helper sets up records as
  /// they would arrive from `GenericDatumReader` *before* the logical-type pass.
  private static <T> Object encodeLogical(T value, Schema recordSchema, Conversion<T> conversion) {
    Schema fieldSchema = recordSchema.getField(COLUMN).schema();
    return Conversions.convertToRawType(value, fieldSchema, fieldSchema.getLogicalType(), conversion);
  }
}
