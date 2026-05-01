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
package org.apache.pinot.spi.data.readers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests [BaseRecordExtractor]'s default conversion paths against the type contract on [RecordExtractor].
/// One test per matrix row:
/// - **Single-value scalars**: `Boolean`, `Byte` / `Short` → `Integer`, `Integer`, `Long`, `Float`, `Double`,
///   `BigDecimal`, `String`, `byte[]`, `ByteBuffer` → `byte[]` (with slice-safety guarantee)
/// - **Temporal**: `LocalDate` / `LocalTime` / `Timestamp` pass through; `Instant` / `OffsetDateTime` /
///   `ZonedDateTime` bridge to `Timestamp`; `LocalDateTime` bridges via UTC interpretation
/// - **Multi-value**: `Collection`, `Object[]`, primitive `int[]` / `long[]` / `float[]` / `double[]` →
///   `Object[]`; unsupported primitive arrays (`boolean[]` / `short[]` / `char[]`) throw
/// - **Map**: keys flow through `convertSingleValue`; values recursively converted
/// - **Nested record**: default `convertRecord` throws unless the subclass overrides it
/// - **Fallback**: any other type → `value.toString()`
public class BaseRecordExtractorTest {

  // === Single-value — order follows the type list in the class Javadoc ===

  @Test
  void testBooleanPreserved() {
    Object trueResult = extract(true);
    assertEquals(trueResult, true);
    Object falseResult = extract(false);
    assertEquals(falseResult, false);
  }

  @Test
  void testByteWidenedToInteger() {
    Object result = extract((byte) 42);
    assertEquals(result, 42);
  }

  @Test
  void testShortWidenedToInteger() {
    Object result = extract((short) 42);
    assertEquals(result, 42);
  }

  @Test
  void testIntegerPreserved() {
    Object result = extract(42);
    assertEquals(result, 42);
  }

  @Test
  void testLongPreserved() {
    Object result = extract(42L);
    assertEquals(result, 42L);
  }

  @Test
  void testFloatPreserved() {
    Object result = extract(1.5f);
    assertEquals(result, 1.5f);
  }

  @Test
  void testDoublePreserved() {
    Object result = extract(1.5d);
    assertEquals(result, 1.5d);
  }

  @Test
  void testBigDecimalPreserved() {
    Object result = extract(new BigDecimal("123.45"));
    assertEquals(result, new BigDecimal("123.45"));
  }

  @Test
  void testBigIntegerWidenedToBigDecimal() {
    // Pinot has no `BigInteger` type; the base widens to `BigDecimal` so downstream transforms can handle it.
    Object result = extract(new BigInteger("12345678901234567890123456789"));
    assertEquals(result, new BigDecimal("12345678901234567890123456789"));
  }

  @Test
  void testStringPreserved() {
    Object result = extract("hello");
    assertEquals(result, "hello");
  }

  // === byte[] / ByteBuffer ===

  @Test
  void testByteArrayPreserved() {
    Object result = extract(new byte[]{1, 2, 3});
    assertEquals((byte[]) result, new byte[]{1, 2, 3});
  }

  @Test
  void testByteBufferConvertedToByteArray() {
    Object result = extract(ByteBuffer.wrap(new byte[]{1, 2, 3}));
    assertEquals((byte[]) result, new byte[]{1, 2, 3});
  }

  @Test
  void testByteBufferSliceDoesNotMutateOriginal() {
    // The extractor must read only the remaining bytes and leave the source buffer's position untouched.
    ByteBuffer buf = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4});
    buf.position(2);
    Object result = extract(buf);
    assertEquals((byte[]) result, new byte[]{2, 3, 4});
    assertEquals(buf.position(), 2);
  }

  // === java.time / java.sql temporal types ===

  @Test
  void testLocalDatePreserved() {
    LocalDate date = LocalDate.of(2022, 4, 14);
    assertEquals(extract(date), date);
  }

  @Test
  void testLocalTimePreserved() {
    LocalTime time = LocalTime.of(8, 51, 32, 123_456_789);
    assertEquals(extract(time), time);
  }

  @Test
  void testTimestampPreserved() {
    Timestamp ts = Timestamp.from(Instant.parse("2022-04-14T08:51:32.123Z"));
    assertEquals(extract(ts), ts);
  }

  @Test
  void testInstantBridgedToTimestamp() {
    Instant instant = Instant.parse("2022-04-14T08:51:32.123456789Z");
    assertEquals(extract(instant), Timestamp.from(instant));
  }

  @Test
  void testOffsetDateTimeBridgedToTimestamp() {
    OffsetDateTime odt = OffsetDateTime.parse("2022-04-14T08:51:32.123-05:00");
    assertEquals(extract(odt), Timestamp.from(odt.toInstant()));
  }

  @Test
  void testZonedDateTimeBridgedToTimestamp() {
    ZonedDateTime zdt = ZonedDateTime.parse("2022-04-14T08:51:32.123+09:00[Asia/Tokyo]");
    assertEquals(extract(zdt), Timestamp.from(zdt.toInstant()));
  }

  @Test
  void testLocalDateTimeBridgedToTimestampViaUtc() {
    // `LocalDateTime` has no zone — interpreted as UTC per the analytics convention.
    LocalDateTime ldt = LocalDateTime.parse("2022-04-14T08:51:32.123");
    assertEquals(extract(ldt), Timestamp.from(ldt.toInstant(ZoneOffset.UTC)));
  }

  // === Fallback / null ===

  @Test
  void testNonStandardObjectFallsBackToToString() {
    Object result = extract(new StringBuilder("hello"));
    assertEquals(result, "hello");
  }

  // === Multi-value (Collection / Object[] / primitive array) → Object[] ===

  @Test
  void testListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of(1, 2, 3));
    assertEquals(result, new Object[]{1, 2, 3});
  }

  @Test
  void testObjectArrayExtractedAsArray() {
    Object[] result = (Object[]) extract(new Object[]{1, "a", true});
    assertEquals(result, new Object[]{1, "a", true});
  }

  @Test
  void testIntPrimitiveArrayExtractedAsArray() {
    Object[] result = (Object[]) extract(new int[]{10, 20, 30});
    assertEquals(result, new Object[]{10, 20, 30});
  }

  @Test
  void testLongPrimitiveArrayExtractedAsArray() {
    Object[] result = (Object[]) extract(new long[]{10L, 20L, 30L});
    assertEquals(result, new Object[]{10L, 20L, 30L});
  }

  @Test
  void testFloatPrimitiveArrayExtractedAsArray() {
    Object[] result = (Object[]) extract(new float[]{1.5f, 2.5f});
    assertEquals(result, new Object[]{1.5f, 2.5f});
  }

  @Test
  void testDoublePrimitiveArrayExtractedAsArray() {
    Object[] result = (Object[]) extract(new double[]{1.5d, 2.5d});
    assertEquals(result, new Object[]{1.5d, 2.5d});
  }

  @Test
  void testUnsupportedPrimitiveArrayThrows() {
    expectThrows(IllegalArgumentException.class, () -> extract(new char[]{'a', 'b'}));
  }

  @Test
  void testListPreservesNullElements() {
    // Shape preservation: null elements stay null in `Object[]`.
    Object[] result = (Object[]) extract(Arrays.asList(1, null, 3));
    assertEquals(result, new Object[]{1, null, 3});
  }

  @Test
  void testObjectArrayPreservesNullElements() {
    Object[] result = (Object[]) extract(new Object[]{1, null, 3});
    assertEquals(result, new Object[]{1, null, 3});
  }

  @Test
  void testEmptyListExtractedAsEmptyArray() {
    Object[] result = (Object[]) extract(List.of());
    assertEquals(result, new Object[]{});
  }

  @Test
  void testEmptyObjectArrayExtractedAsEmptyArray() {
    Object[] result = (Object[]) extract(new Object[]{});
    assertEquals(result, new Object[]{});
  }

  // === Map (recursively converted) ===

  @Test
  void testMapWithStringKeys() {
    Map<?, ?> result = (Map<?, ?>) extract(Map.of(
        "k1", 1,
        "k2", "foo"
    ));
    assertEquals(result.size(), 2);
    assertEquals(result.get("k1"), 1);
    assertEquals(result.get("k2"), "foo");
  }

  @Test
  void testMapValuesRecursivelyConverted() {
    // Inner List values become Object[]; inner Short values widen to Integer.
    Map<?, ?> result = (Map<?, ?>) extract(Map.of(
        "list", List.of(1, 2),
        "short", (short) 7
    ));
    assertEquals((Object[]) result.get("list"), new Object[]{1, 2});
    assertEquals(result.get("short"), 7);
  }

  @Test
  void testMapPreservesNullValues() {
    // Shape preservation: null values stay null (only null *keys* drop the entry).
    HashMap<String, Object> input = new HashMap<>();
    input.put("k1", 1);
    input.put("k2", null);
    Map<?, ?> result = (Map<?, ?>) extract(input);
    assertEquals(result.size(), 2);
    assertEquals(result.get("k1"), 1);
    assertNull(result.get("k2"));
    assertTrue(result.containsKey("k2"));
  }

  @Test
  void testMapDropsNullKeyEntry() {
    HashMap<Object, Object> input = new HashMap<>();
    input.put("k1", 1);
    input.put(null, "ignored");
    Map<?, ?> result = (Map<?, ?>) extract(input);
    assertEquals(result, Map.of("k1", 1));
  }

  @Test
  void testMapDropsEntryWhenConvertSingleValueReturnsNull() {
    // Format-specific overrides may translate a non-null Java key to `null` (a format-native null sentinel).
    // The entry must be dropped — converted-key check, not just input-key check.
    Map<?, ?> result = (Map<?, ?>) new SentinelKeyExtractor().convert(Map.of(
        "k1", 1,
        "__SENTINEL__", 2
    ));
    assertEquals(result, Map.of("k1", 1));
  }

  @Test
  void testEmptyMapExtractedAsEmptyMap() {
    Map<?, ?> result = (Map<?, ?>) extract(Map.of());
    assertEquals(result.size(), 0);
  }

  // === Nested record — base throws unless overridden ===

  @Test
  void testNestedRecordThrowsByDefault() {
    UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class,
        () -> new RecordCapableExtractor().convert(new Object()));
    assertEquals(ex.getMessage(),
        "RecordCapableExtractor does not support nested records; override convertRecord() to enable.");
  }

  // === shouldExtract — extract-all vs explicit include list ===

  @Test
  void testShouldExtractDefaultIsExtractAll() {
    // Pre-`init` state: `_extractAll = true`, `_fields = Set.of()`. Every name should extract.
    TestExtractor extractor = new TestExtractor();
    assertTrue(extractor.shouldExtract("anything"));
    assertTrue(extractor.shouldExtract(""));
  }

  @Test
  void testShouldExtractWithNullFieldsIsExtractAll() {
    TestExtractor extractor = new TestExtractor();
    extractor.init(null, null);
    assertTrue(extractor.shouldExtract("a"));
    assertTrue(extractor.shouldExtract("b"));
  }

  @Test
  void testShouldExtractWithEmptyFieldsIsExtractAll() {
    TestExtractor extractor = new TestExtractor();
    extractor.init(Set.of(), null);
    assertTrue(extractor.shouldExtract("a"));
    assertTrue(extractor.shouldExtract("b"));
  }

  @Test
  void testShouldExtractFiltersToIncludeList() {
    TestExtractor extractor = new TestExtractor();
    extractor.init(Set.of("a", "b"), null);
    assertTrue(extractor.shouldExtract("a"));
    assertTrue(extractor.shouldExtract("b"));
    assertFalse(extractor.shouldExtract("c"));
    assertFalse(extractor.shouldExtract(""));
  }

  @Test
  void testShouldExtractResetsOnReinit() {
    // Start filtered, then re-init with extract-all — `shouldExtract` should flip from per-name to all.
    TestExtractor extractor = new TestExtractor();
    extractor.init(Set.of("a"), null);
    assertFalse(extractor.shouldExtract("b"));
    extractor.init(null, null);
    assertTrue(extractor.shouldExtract("b"));
    // And back the other way.
    extractor.init(Set.of("a"), null);
    assertFalse(extractor.shouldExtract("b"));
  }

  // === Helpers ===

  private static Object extract(Object value) {
    return new TestExtractor().convert(value);
  }

  /// Minimal subclass that doesn't override anything — exercises the default conversion paths in
  /// [BaseRecordExtractor].
  private static final class TestExtractor extends BaseRecordExtractor<Object> {
    @Override
    public GenericRow extract(Object from, GenericRow to) {
      throw new UnsupportedOperationException();
    }
  }

  /// Subclass whose `convertSingleValue` translates a sentinel `String` to `null`, mimicking a format-specific
  /// null-sentinel translation. Used to verify that `convertMap` drops entries whose post-conversion key is
  /// `null`, not just entries whose input key is `null`.
  private static final class SentinelKeyExtractor extends BaseRecordExtractor<Object> {
    @Override
    public GenericRow extract(Object from, GenericRow to) {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    protected Object convertSingleValue(Object value) {
      return "__SENTINEL__".equals(value) ? null : super.convertSingleValue(value);
    }
  }

  /// Subclass that returns `true` from [#isRecord] for any input but does NOT override `convertRecord` —
  /// triggers the default `UnsupportedOperationException`.
  private static final class RecordCapableExtractor extends BaseRecordExtractor<Object> {
    @Override
    public GenericRow extract(Object from, GenericRow to) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isRecord(Object value) {
      return true;
    }
  }
}
