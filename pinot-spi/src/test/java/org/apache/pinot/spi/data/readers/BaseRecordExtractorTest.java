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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests what [BaseRecordExtractor] still owns after format-specific extractors took over their own type
/// conversions:
/// - **Universal normalizations**: `Byte` / `Short` → `Integer`, [BigInteger] → [BigDecimal], `ByteBuffer` →
///   `byte[]` (with slice-safety guarantee); passthrough for other `Number` / `Boolean` / `byte[]` / `String`.
/// - **Recursive walker** for nested complex inputs that format extractors with `Collection` / `Map` / nested
///   record shapes (Avro, Thrift, JSON) lean on:
///   - `Collection` / `Object[]` / primitive `int[]` / `long[]` / `float[]` / `double[]` → `Object[]`;
///     unsupported primitive arrays (`boolean[]` / `short[]` / `char[]`) throw.
///   - `Map`: keys flow through `convertSingleValue`; values recursively converted.
///   - Nested record: default `convertRecord` throws unless the subclass overrides it.
/// - **Fallback**: any other type → `value.toString()`.
///
/// Date / time types are NOT exercised here — DATE / TIME passthrough and TIMESTAMP narrowing are per-format
/// (see e.g. `AvroRecordExtractorTest`).
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
  void testMapKeysRunThroughConvertSingleValue() {
    // Keys flow through `convertSingleValue` before `stringifyMapKey`. ByteBuffer materializes to byte[] then
    // base64-encodes; Byte/Short/BigInteger widen before `toString()`.
    HashMap<Object, Object> input = new HashMap<>();
    input.put(ByteBuffer.wrap(new byte[]{1, 2, 3}), "bytes");
    input.put((byte) 7, "byte");
    input.put((short) 8, "short");
    input.put(new BigInteger("12345678901234567890"), "bigint");
    Map<?, ?> result = (Map<?, ?>) extract(input);
    assertEquals(result, Map.of(
        "AQID", "bytes",
        "7", "byte",
        "8", "short",
        "12345678901234567890", "bigint"
    ));
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

  // === stringifyMapKey — shared helper for the Map<String, Object> contract ===

  @Test
  void testStringifyMapKeyByteArrayBase64Encoded() {
    assertEquals(BaseRecordExtractor.stringifyMapKey(new byte[]{0, 1, 2, 3}), "AAECAw==");
  }

  @Test
  void testStringifyMapKeyTimestampUsesIsoUtcWithNanos() {
    java.sql.Timestamp ts = new java.sql.Timestamp(1700000000123L);
    ts.setNanos(123_456_789);
    // ISO-8601 UTC, JVM-TZ-stable, full nanosecond precision preserved.
    assertEquals(BaseRecordExtractor.stringifyMapKey(ts), "2023-11-14T22:13:20.123456789Z");
  }

  @Test
  void testStringifyMapKeyOtherTypesUseToString() {
    assertEquals(BaseRecordExtractor.stringifyMapKey("k"), "k");
    assertEquals(BaseRecordExtractor.stringifyMapKey(42), "42");
    assertEquals(BaseRecordExtractor.stringifyMapKey(123L), "123");
    assertEquals(BaseRecordExtractor.stringifyMapKey(true), "true");
    assertEquals(BaseRecordExtractor.stringifyMapKey(new BigDecimal("1.50")), "1.50");
    assertEquals(BaseRecordExtractor.stringifyMapKey(java.time.LocalDate.of(2024, 1, 1)), "2024-01-01");
    assertEquals(BaseRecordExtractor.stringifyMapKey(java.time.LocalTime.of(8, 51, 32)), "08:51:32");
  }

  // === Nested record — base throws unless overridden ===

  @Test
  void testNestedRecordThrowsByDefault() {
    UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class,
        () -> new RecordCapableExtractor().convert(new Object()));
    assertEquals(ex.getMessage(),
        "RecordCapableExtractor does not support nested records; override convertRecord() to enable.");
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

  /// Subclass that returns `null` from [#convertSingleValue] for the `"__SENTINEL__"` key — simulates a format's
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
}
