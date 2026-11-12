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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MapUtilsTest {

  @Test
  void testSerializeAndDeserializeEmptyMap() {
    Map<String, Object> emptyMap = Map.of();
    byte[] serialized = MapUtils.serializeMap(emptyMap);
    Map<String, Object> deserialized = MapUtils.deserializeMap(serialized);

    assertNotNull(serialized, "Serialized byte array should not be null");
    assertEquals(serialized.length, 4, "Serialized empty map should only have 4 bytes for size integer");
    assertNotNull(deserialized, "Deserialized map should not be null");
    assertTrue(deserialized.isEmpty(), "Deserialized map should be empty");
  }

  @Test
  void testSerializeAndDeserializeMapWithVariousDataTypes() {
    Map<String, Object> map = new HashMap<>();
    map.put("string", "value");
    map.put("int", 123);
    map.put("double", 456.78);
    map.put("boolean", true);
    map.put("nullValue", null);

    byte[] serialized = MapUtils.serializeMap(map);
    Map<String, Object> deserialized = MapUtils.deserializeMap(serialized);

    assertNotNull(deserialized, "Deserialized map should not be null");
    assertEquals(deserialized.size(), map.size(), "Deserialized map should have the same size as the original");
    assertEquals(deserialized.get("string"), map.get("string"), "String value should match");
    assertEquals(deserialized.get("int"), map.get("int"), "Integer value should match");
    assertEquals(deserialized.get("double"), map.get("double"), "Double value should match");
    assertEquals(deserialized.get("boolean"), map.get("boolean"), "Boolean value should match");
    assertNull(deserialized.get("nullValue"), "Null value should be preserved");
  }

  @Test
  void testDeserializeMapEntryValue() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("first", Map.of("nested", List.of(1, 2, 3)));
    map.put("k8s.workload.name", "pinot-server");
    map.put("nullValue", null);
    byte[] serialized = MapUtils.serializeMap(map);

    assertEquals(MapUtils.deserializeMapEntryValue(serialized, "k8s.workload.name"), "pinot-server");
    assertEquals(MapUtils.deserializeMapEntryValue(ByteBuffer.wrap(serialized), "first"),
        Map.of("nested", List.of(1, 2, 3)));
    assertNull(MapUtils.deserializeMapEntryValue(serialized, "missing"));
    assertNull(MapUtils.deserializeMapEntryValue(serialized, "nullValue"));
    assertNull(MapUtils.deserializeMapEntryValue(MapUtils.serializeMap(Map.of()), "any"));
  }

  /// Keys that are the same length and differ only in their trailing bytes are the case the scanning extractor can
  /// get wrong: the length check passes for every entry, so the match has to come from the byte comparison alone.
  @Test
  void testDeserializeMapEntryValueWithCollidingKeyShapes() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("k8s.workload.name", "workload");
    map.put("k8s.workload.kind", "kind");
    map.put("k8s.namespace.nam", "namespace");
    map.put("k8s.workload", "prefix-of-another-key");
    map.put("k8s.workload.name.suffixed", "longer-than-another-key");
    byte[] serialized = MapUtils.serializeMap(map, false);

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      assertEquals(MapUtils.deserializeMapEntryValue(serialized, entry.getKey()), entry.getValue(),
          "Value should match for key: " + entry.getKey());
    }
    assertNull(MapUtils.deserializeMapEntryValue(serialized, "k8s.workload.nam"));
    assertNull(MapUtils.deserializeMapEntryValue(serialized, "k8s.workload.names"));
  }

  /// The extractor matches on encoded UTF-8 bytes rather than decoding each key, so multi-byte keys - and keys whose
  /// character count differs from their byte count - have to resolve correctly.
  @Test
  void testDeserializeMapEntryValueWithNonAsciiKeys() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("hôte", "host");
    map.put("hote", "ascii-host");
    map.put("命名空间", "namespace");
    byte[] serialized = MapUtils.serializeMap(map, false);

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      assertEquals(MapUtils.deserializeMapEntryValue(serialized, entry.getKey()), entry.getValue(),
          "Value should match for key: " + entry.getKey());
    }
    assertNull(MapUtils.deserializeMapEntryValue(serialized, "命名"));
  }

  /// The string accessor must agree with `deserializeMapValue(...).toString()` for every value shape, since the only
  /// difference is meant to be whether Jackson was involved. Escaped and multi-byte strings take the Jackson
  /// fallback and must come out identical to the plain ones.
  @Test
  void testDeserializeMapValueAsStringMatchesToString() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("plain", "pinot-server");
    map.put("empty", "");
    map.put("quoted", "has \"quotes\" inside");
    map.put("backslash", "has \\ backslash");
    map.put("newline", "has \n newline");
    map.put("unicode", "çöğüşÇÖĞÜŞéÉ");
    map.put("int", 42);
    map.put("long", 9999999999L);
    map.put("double", 1.5);
    map.put("bool", true);
    map.put("list", List.of(1, 2));
    map.put("nested", Map.of("a", 1));
    byte[] serialized = MapUtils.serializeMap(map, false);

    for (String key : map.keySet()) {
      Object asObject = MapUtils.deserializeMapEntryValue(serialized, key);
      assertEquals(MapUtils.deserializeMapEntryValueAsString(ByteBuffer.wrap(serialized), key), asObject.toString(),
          "String rendering should match toString() for key: " + key);
    }
  }

  /// Mixed-type MAP columns are a supported shape - a STRING-valued MAP can carry numeric entries, as
  /// `MapFieldTypeMixedValueIngestingIntegrationTest` ingests - so the non-string shapes have to render exactly as
  /// `toString()` on the parsed value, whether or not they take a Jackson-free path.
  @Test
  void testDeserializeMapValueAsStringMatchesToStringForNonStringShapes() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("zero", 0);
    map.put("negative", -42);
    map.put("intMax", Integer.MAX_VALUE);
    map.put("longMin", Long.MIN_VALUE);
    map.put("bigInteger", new BigInteger("123456789012345678901234567890"));
    map.put("boolTrue", true);
    map.put("boolFalse", false);
    // Renders as "1.50" in the frame but binds to a Double, so it must come back re-normalized as "1.5".
    map.put("trailingZeroDecimal", new BigDecimal("1.50"));
    map.put("exponent", 1.0E300);
    map.put("negativeZeroDouble", -0.0d);
    byte[] serialized = MapUtils.serializeMap(map, false);

    for (String key : map.keySet()) {
      Object asObject = MapUtils.deserializeMapEntryValue(serialized, key);
      assertEquals(MapUtils.deserializeMapEntryValueAsString(ByteBuffer.wrap(serialized), key), asObject.toString(),
          "String rendering should match toString() for key: " + key);
    }
    assertEquals(MapUtils.deserializeMapEntryValueAsString(ByteBuffer.wrap(serialized), "trailingZeroDecimal"), "1.5");
  }

  @Test
  void testDeserializeMapValueAsStringHandlesMissingAndNull() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("present", "value");
    map.put("nullValue", null);
    byte[] serialized = MapUtils.serializeMap(map, false);

    assertNull(MapUtils.deserializeMapEntryValueAsString(ByteBuffer.wrap(serialized), "missing"));
    assertNull(MapUtils.deserializeMapEntryValueAsString(ByteBuffer.wrap(serialized), "nullValue"));
    assertEquals(MapUtils.deserializeMapEntryValueAsString(ByteBuffer.wrap(serialized), "present"), "value");
  }

  /// An off-heap forward-index view inherits the platform's native byte order, while the frame is always written
  /// big-endian. The extractor has to force the order rather than trust the incoming buffer.
  @Test
  void testDeserializeMapEntryValueForcesBigEndian() {
    byte[] serialized = MapUtils.serializeMap(Map.of("k8s.workload.name", "pinot-server"));
    ByteBuffer littleEndian = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

    assertEquals(MapUtils.deserializeMapEntryValue(littleEndian, "k8s.workload.name"), "pinot-server");
  }

  @Test
  void testDeserializeMapEntryValueRejectsInvalidValueLength() {
    String key = "key";
    byte[] serialized = MapUtils.serializeMap(Map.of(key, "value"));
    ByteBuffer frame = ByteBuffer.wrap(serialized);
    frame.position(Integer.BYTES);
    int keyLength = frame.getInt();
    int valueLengthOffset = frame.position() + keyLength;

    byte[] negativeLength = serialized.clone();
    ByteBuffer.wrap(negativeLength).putInt(valueLengthOffset, -1);
    assertThrows(BufferUnderflowException.class, () -> MapUtils.deserializeMapEntryValue(negativeLength, key));
    assertThrows(BufferUnderflowException.class, () -> MapUtils.deserializeMapEntryValue(negativeLength, "missing"));

    byte[] truncatedValue = serialized.clone();
    ByteBuffer.wrap(truncatedValue).putInt(valueLengthOffset, serialized.length);
    assertThrows(BufferUnderflowException.class, () -> MapUtils.deserializeMapEntryValue(truncatedValue, key));
  }

  @Test
  void testDeserializeMapEntryValueRejectsNegativeMapSize() {
    byte[] negativeSize = ByteBuffer.allocate(Integer.BYTES).putInt(-1).array();

    assertThrows(BufferUnderflowException.class, () -> MapUtils.deserializeMapEntryValue(negativeSize, "key"));
  }

  @Test
  void testSerializeAndDeserializeWithSpecialCharacters() {
    Map<String, Object> map = new HashMap<>();
    map.put("specialChars", "çöğüşÇÖĞÜŞéÉ");

    byte[] serialized = MapUtils.serializeMap(map);
    Map<String, Object> deserialized = MapUtils.deserializeMap(serialized);

    assertNotNull(deserialized, "Deserialized map should not be null");
    assertEquals(deserialized.get("specialChars"), map.get("specialChars"), "Special character value should match");
  }

  @Test
  void testSerializeSortedIsDeterministic() {
    // Same logical content in different insertion orders must produce identical canonical bytes.
    Map<String, Object> map1 = new LinkedHashMap<>();
    map1.put("a", 1);
    map1.put("b", 2);
    map1.put("c", 3);

    Map<String, Object> map2 = new LinkedHashMap<>();
    map2.put("c", 3);
    map2.put("a", 1);
    map2.put("b", 2);

    assertEquals(MapUtils.serializeMap(map1), MapUtils.serializeMap(map2),
        "Sorted serialization should be deterministic regardless of input iteration order");
  }

  @Test
  void testSerializeUnsortedPreservesIterationOrder() {
    // sortByKey=false should write entries in the input map's iteration order.
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("z", 1);
    map.put("a", 2);
    map.put("m", 3);

    byte[] sorted = MapUtils.serializeMap(map, true);
    byte[] unsorted = MapUtils.serializeMap(map, false);

    assertNotEquals(sorted, unsorted,
        "Sorted and unsorted serialization should differ for non-alphabetical insertion order");
    assertEquals(MapUtils.deserializeMap(sorted), map, "Sorted bytes should round-trip");
    assertEquals(MapUtils.deserializeMap(unsorted), map, "Unsorted bytes should round-trip");
  }

  @Test
  void testSerializeSortedMapShortCircuitsSort() {
    // SortedMap is already in sorted order, so no resort is needed; bytes must still match the canonical form.
    Map<String, Object> treeMap = new TreeMap<>();
    treeMap.put("c", 3);
    treeMap.put("a", 1);
    treeMap.put("b", 2);
    Map<String, Object> hashMap = new HashMap<>(treeMap);

    assertEquals(MapUtils.serializeMap(treeMap), MapUtils.serializeMap(hashMap),
        "TreeMap and HashMap with the same content should produce identical canonical bytes");
  }

  @Test
  void testSerializeNestedMapsAreCanonical() {
    // Nested maps inside values must also be key-sorted in canonical mode (ORDER_MAP_ENTRIES_BY_KEYS).
    Map<String, Object> nested1 = new LinkedHashMap<>();
    nested1.put("z", 1);
    nested1.put("a", 2);

    Map<String, Object> nested2 = new LinkedHashMap<>();
    nested2.put("a", 2);
    nested2.put("z", 1);

    Map<String, Object> outer1 = new HashMap<>();
    outer1.put("nested", nested1);
    Map<String, Object> outer2 = new HashMap<>();
    outer2.put("nested", nested2);

    assertEquals(MapUtils.serializeMap(outer1), MapUtils.serializeMap(outer2),
        "Nested-map canonicalization should produce identical bytes");
  }

  @Test
  void testSerializedSizeMatchesSerializedBytes() {
    Map<String, Object> map = new HashMap<>();
    map.put("string", "value");
    map.put("int", 123);
    map.put("double", 4.5);
    map.put("boolean", true);
    map.put("nested", Map.of("a", 1, "b", 2));
    map.put("list", List.of("x", "y", "z"));

    int reportedSize = MapUtils.serializedSize(map);
    byte[] actualBytes = MapUtils.serializeMap(map);

    assertEquals(reportedSize, actualBytes.length, "serializedSize should match serializeMap(...).length");
  }

  @Test
  void testSerializedSizeEmpty() {
    assertEquals(MapUtils.serializedSize(Map.of()), 4, "Empty map should report 4 bytes (size header only)");
  }

  @Test
  void testSerializedSizeWithSpecialCharacters() {
    // UTF-8 multi-byte chars must be counted correctly on both key and value sides.
    Map<String, Object> map = new HashMap<>();
    map.put("çöğüş", "ÇÖĞÜŞéÉ");

    assertEquals(MapUtils.serializedSize(map), MapUtils.serializeMap(map).length,
        "serializedSize should match for maps containing UTF-8 multi-byte characters");
  }

  @Test
  void testToString() {
    Map<String, Object> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", 123);

    String mapString = MapUtils.toString(map);
    assertNotNull(mapString, "Serialized string should not be null");
    assertTrue(mapString.contains("\"key1\":\"value1\""),
        "Serialized string should contain the correct key-value pairs");
    assertTrue(mapString.contains("\"key2\":123"), "Serialized string should contain the correct key-value pairs");
  }

  @Test
  void testToStringSortedByDefault() {
    // Default toString must produce canonical (key-sorted) JSON.
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("z", 1);
    map.put("a", 2);

    assertEquals(MapUtils.toString(map), "{\"a\":2,\"z\":1}", "Default toString should be key-sorted");
  }

  @Test
  void testToStringSortedAndUnsorted() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("z", 1);
    map.put("a", 2);

    assertEquals(MapUtils.toString(map, true), "{\"a\":2,\"z\":1}", "sortByKey=true should produce sorted JSON");
    assertEquals(MapUtils.toString(map, false), "{\"z\":1,\"a\":2}",
        "sortByKey=false should preserve insertion order");
  }

  @Test
  void testToStringNestedMapsAreSortedInCanonicalMode() {
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("z", 1);
    nested.put("a", 2);
    Map<String, Object> outer = new LinkedHashMap<>();
    outer.put("inner", nested);

    assertEquals(MapUtils.toString(outer, true), "{\"inner\":{\"a\":2,\"z\":1}}",
        "Canonical toString should sort nested map keys too");
    assertEquals(MapUtils.toString(outer, false), "{\"inner\":{\"z\":1,\"a\":2}}",
        "Non-canonical toString should preserve nested map iteration order");
  }

  @Test
  void testFromStringRoundTrip() {
    Map<String, Object> original = new HashMap<>();
    original.put("string", "value");
    original.put("int", 123);
    original.put("double", 4.5);
    original.put("boolean", true);
    original.put("nullValue", null);
    original.put("list", List.of(1, 2, 3));
    original.put("nested", Map.of("a", 1, "b", 2));

    String json = MapUtils.toString(original);
    Map<String, Object> roundTripped = MapUtils.fromString(json);

    assertEquals(roundTripped, original, "fromString(toString) should preserve all values");
  }

  @Test
  void testFromStringEmpty() {
    Map<String, Object> result = MapUtils.fromString("{}");
    assertNotNull(result, "fromString of empty object should return non-null map");
    assertTrue(result.isEmpty(), "fromString of empty object should return empty map");
  }

  @Test(expectedExceptions = BufferUnderflowException.class)
  void testDeserializeInvalidData() {
    // First 4 bytes parse as size = 66051, which the loop then tries to read past end-of-buffer.
    MapUtils.deserializeMap(new byte[]{0, 1, 2, 3});
  }

  // === JSR-310 — LocalDate / LocalTime values must reach Jackson via the configured ObjectMapper, NOT
  // crash. The MAP serialization path goes through MapUtils' own writers, separate from JsonUtils', so
  // this is its own coverage layer. ===

  @Test
  void testToStringSerializesLocalDateAsIsoString() {
    Map<String, Object> map = new HashMap<>();
    map.put("d", LocalDate.of(2022, 2, 8));
    assertEquals(MapUtils.toString(map), "{\"d\":\"2022-02-08\"}");
  }

  @Test
  void testToStringSerializesLocalTimeAsIsoString() {
    Map<String, Object> map = new HashMap<>();
    map.put("t", LocalTime.of(12, 34, 56));
    assertEquals(MapUtils.toString(map), "{\"t\":\"12:34:56\"}");
  }

  @Test
  void testSerializeMapWithLocalDateRoundTrips() {
    // LocalDate goes in, ISO string comes out — serialization is lossy for type info; the contract is
    // "values are JSON-encoded, downstream coerces by column type".
    Map<String, Object> map = new HashMap<>();
    map.put("d", LocalDate.of(2022, 2, 8));
    Map<String, Object> deserialized = MapUtils.deserializeMap(MapUtils.serializeMap(map));
    assertEquals(deserialized.get("d"), "2022-02-08");
  }

  @Test
  void testToStringSerializesNestedMapWithLocalDate() {
    Map<String, Object> nested = new HashMap<>();
    nested.put("d", LocalDate.of(2022, 2, 8));
    Map<String, Object> map = new HashMap<>();
    map.put("nested", nested);
    assertEquals(MapUtils.toString(map), "{\"nested\":{\"d\":\"2022-02-08\"}}");
  }

  /// The contract that lets the forward-index read path swap `toString(deserializeMap(frame))` for
  /// `frameToJsonString(frame)`: for any frame written through the key-sorting [MapUtils#serializeMap(Map)] - which
  /// is what both forward-index write paths use - the two must produce identical output.
  @Test
  void testFrameToJsonStringMatchesToString() {
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("z", 1);
    nested.put("a", List.of(1, 2, 3));

    Map<String, Object> map = new LinkedHashMap<>();
    map.put("k8s.workload.name", "pinot-server");
    map.put("int", 42);
    map.put("long", 9999999999L);
    map.put("double", 1.5);
    map.put("bool", true);
    map.put("nullValue", null);
    map.put("nested", nested);
    map.put("list", List.of("a", "b"));
    map.put("emptyString", "");
    map.put("unicodeValue", "çöğüşÇÖĞÜŞéÉ");
    map.put("命名空间", "namespace");
    map.put("date", LocalDate.of(2022, 2, 8));
    map.put("quote\"key", "quoted");
    map.put("back\\slash", "escaped");
    map.put("tab\tkey", "control");
    map.put("value with \"quotes\" and \\ backslash", "in value");

    assertEquals(MapUtils.frameToJsonString(MapUtils.serializeMap(map)), MapUtils.toString(map));
  }

  @Test
  void testFrameToJsonStringHandlesEmptyMap() {
    assertEquals(MapUtils.frameToJsonString(MapUtils.serializeMap(Map.of())), "{}");
    assertEquals(MapUtils.frameToJsonString(MapUtils.serializeMap(Map.of())), MapUtils.toString(Map.of()));
  }

  /// Rendering must round-trip back through the JSON reader to the same map, independent of the string comparison
  /// above - that guards against two implementations agreeing on malformed output.
  @Test
  void testFrameToJsonStringRoundTrips() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("a", "value");
    map.put("b", List.of(1, 2));
    map.put("çö", Map.of("inner", true));
    assertEquals(MapUtils.fromString(MapUtils.frameToJsonString(MapUtils.serializeMap(map))),
        MapUtils.deserializeMap(MapUtils.serializeMap(map)));
  }

  /// An off-heap forward-index view arrives in the platform's native byte order while the frame is written
  /// big-endian, so the renderer has to force the order rather than trust the buffer.
  @Test
  void testFrameToJsonStringForcesBigEndian() {
    byte[] serialized = MapUtils.serializeMap(Map.of("k8s.workload.name", "pinot-server"));
    ByteBuffer littleEndian = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

    assertEquals(MapUtils.frameToJsonString(littleEndian), "{\"k8s.workload.name\":\"pinot-server\"}");
  }
}
