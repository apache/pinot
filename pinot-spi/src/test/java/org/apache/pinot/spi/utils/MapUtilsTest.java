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

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MapUtilsTest {
  @Test
  void testSerializeAndDeserializeEmptyMap() {
    // Test for empty map
    Map<String, Object> emptyMap = Map.of();
    byte[] serialized = MapUtils.serializeMap(emptyMap);
    Map<String, Object> deserialized = MapUtils.deserializeMap(serialized);

    assertNotNull(serialized, "Serialized byte array should not be null");
    assertEquals(4, serialized.length, "Serialized empty map should only have 4 bytes for size integer");
    assertNotNull(deserialized, "Deserialized map should not be null");
    assertTrue(deserialized.isEmpty(), "Deserialized map should be empty");
  }

  @Test
  void testSerializeAndDeserializeMapWithVariousDataTypes() {
    // Test map with various data types
    Map<String, Object> map = new HashMap<>();
    map.put("string", "value");
    map.put("int", 123);
    map.put("double", 456.78);
    map.put("boolean", true);
    map.put("nullValue", null);

    byte[] serialized = MapUtils.serializeMap(map);
    Map<String, Object> deserialized = MapUtils.deserializeMap(serialized);

    assertNotNull(deserialized, "Deserialized map should not be null");
    assertEquals(map.size(), deserialized.size(), "Deserialized map should have the same size as the original");
    assertEquals(map.get("string"), deserialized.get("string"), "String value should match");
    assertEquals(map.get("int"), deserialized.get("int"), "Integer value should match");
    assertEquals(map.get("double"), deserialized.get("double"), "Double value should match");
    assertEquals(map.get("boolean"), deserialized.get("boolean"), "Boolean value should match");
    assertNull(deserialized.get("nullValue"), "Null value should be preserved");
  }

  @Test
  void testSerializeAndDeserializeWithSpecialCharacters() {
    // Test map with special characters
    Map<String, Object> map = new HashMap<>();
    map.put("specialChars", "çöğüşÇÖĞÜŞéÉ");

    byte[] serialized = MapUtils.serializeMap(map);
    Map<String, Object> deserialized = MapUtils.deserializeMap(serialized);

    assertNotNull(deserialized, "Deserialized map should not be null");
    assertEquals(map.get("specialChars"), deserialized.get("specialChars"), "Special character value should match");
  }

  @Test
  void testToString() {
    // Test the toString method
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
  void testDeserializeInvalidData() {
    // Test deserialization of invalid data
    byte[] invalidData = new byte[]{0, 1, 2, 3};

    try {
      MapUtils.deserializeMap(invalidData);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Insufficient bytes in buffer to read all keys and values"),
          "Exception message should contain the correct error message");
      return;
    }
    fail("Should have thrown an exception");
  }
}
