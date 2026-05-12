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

import java.nio.BufferUnderflowException;
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
}
