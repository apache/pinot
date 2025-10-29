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
package org.apache.pinot.common.function.scalar;

import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class StringToMapFunctionsTest {

  @Test
  public void testStringToMap() {
    // Test valid JSON string
    String json = "{\"name\":\"John\",\"age\":30,\"city\":\"New York\"}";
    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.size(), 3);
    assertEquals(result.get("name"), "John");
    assertEquals(result.get("age"), 30);
    assertEquals(result.get("city"), "New York");
  }

  @Test
  public void testStringToMapWithNestedObject() {
    String json = "{\"user\":{\"name\":\"Alice\"},\"count\":5}";
    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.get("user") instanceof Map);
    assertEquals(result.get("count"), 5);
  }

  @Test
  public void testStringToMapNullInput() {
    assertNull(StringToMapFunctions.stringToMap(null));
  }

  @Test
  public void testStringToMapEmptyInput() {
    assertNull(StringToMapFunctions.stringToMap(""));
    assertNull(StringToMapFunctions.stringToMap("   "));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStringToMapInvalidJson() {
    // Should throw exception for invalid JSON
    StringToMapFunctions.stringToMap("not json");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStringToMapInvalidJsonBraces() {
    // Should throw exception for invalid JSON
    StringToMapFunctions.stringToMap("{invalid}");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStringToMapJsonArray() {
    // Should throw exception for JSON array (not an object)
    StringToMapFunctions.stringToMap("[1,2,3]");
  }

  @Test
  public void testStringToMapWithDifferentTypes() {
    // Test that values are kept as OBJECT type (not converted to strings)
    String json = "{\"name\":\"Bob\",\"age\":25,\"active\":true,\"score\":98.5}";
    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.size(), 4);
    assertEquals(result.get("name"), "Bob");
    assertEquals(result.get("age"), 25); // Remains as Integer
    assertEquals(result.get("active"), true); // Remains as Boolean
    assertEquals(result.get("score"), 98.5); // Remains as Double
  }

  @Test
  public void testStringToMapNullValue() {
    String json = "{\"key\":null}";
    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertNull(result.get("key"));
  }

  @Test
  public void testStringExtractValue() {
    String json = "{\"event\":\"click\",\"user\":\"john\",\"timestamp\":1234567890}";

    assertEquals(StringToMapFunctions.stringExtractValue(json, "event"), "click");
    assertEquals(StringToMapFunctions.stringExtractValue(json, "user"), "john");
    assertEquals(StringToMapFunctions.stringExtractValue(json, "timestamp"), 1234567890);
    assertNull(StringToMapFunctions.stringExtractValue(json, "nonexistent"));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStringExtractValueInvalidJson() {
    // Should throw exception for invalid JSON
    StringToMapFunctions.stringExtractValue("invalid", "key");
  }

  @Test
  public void testStringExtractValueNullInput() {
    // Null input returns null
    assertNull(StringToMapFunctions.stringExtractValue(null, "key"));
  }

  @Test
  public void testEmptyJsonObject() {
    String json = "{}";
    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testJsonWithSpecialCharacters() {
    String json = "{\"key\":\"value with spaces\",\"special\":\"a\\\"b\\\"c\"}";
    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.get("key"), "value with spaces");
    assertTrue(result.get("special").toString().contains("\""));
  }

  /**
   * DataProvider for testing different value types in MAP.
   * MAP supports String keys with values of type: String, Integer, Long, Float, Double, Boolean.
   */
  @DataProvider(name = "mapValueTypes")
  public Object[][] provideMapValueTypes() {
    return new Object[][] {
        // JSON, key, expectedValue, expectedType
        {"{\"strKey\":\"hello\"}", "strKey", "hello", String.class},
        {"{\"intKey\":42}", "intKey", 42, Integer.class},
        {"{\"longKey\":9999999999}", "longKey", 9999999999L, Long.class},
        {"{\"floatKey\":3.14}", "floatKey", 3.14, Double.class}, // JSON numbers are parsed as Double
        {"{\"doubleKey\":123.456}", "doubleKey", 123.456, Double.class},
        {"{\"boolKey\":true}", "boolKey", true, Boolean.class},
        {"{\"boolFalse\":false}", "boolFalse", false, Boolean.class},
        {"{\"negativeInt\":-100}", "negativeInt", -100, Integer.class},
        {"{\"negativeDouble\":-99.99}", "negativeDouble", -99.99, Double.class},
        {"{\"zeroInt\":0}", "zeroInt", 0, Integer.class},
        {"{\"zeroDouble\":0.0}", "zeroDouble", 0.0, Double.class}
    };
  }

  @Test(dataProvider = "mapValueTypes")
  public void testStringToMapWithValueTypes(String jsonString, String key, Object expectedValue,
      Class<?> expectedType) {
    Map<String, Object> result = StringToMapFunctions.stringToMap(jsonString);

    assertNotNull(result, "Result should not be null");
    assertTrue(result.containsKey(key), "Result should contain key: " + key);

    Object actualValue = result.get(key);
    assertNotNull(actualValue, "Value for key '" + key + "' should not be null");

    // Check the type
    assertTrue(expectedType.isInstance(actualValue),
        String.format("Value for key '%s' should be of type %s but was %s",
            key, expectedType.getSimpleName(), actualValue.getClass().getSimpleName()));

    // Check the value
    assertEquals(actualValue, expectedValue,
        String.format("Value for key '%s' should be %s", key, expectedValue));
  }

  @Test(dataProvider = "mapValueTypes")
  public void testStringExtractValueWithTypes(String jsonString, String key, Object expectedValue,
      Class<?> expectedType) {
    Object actualValue = StringToMapFunctions.stringExtractValue(jsonString, key);

    assertNotNull(actualValue, "Extracted value should not be null");

    // Check the type
    assertTrue(expectedType.isInstance(actualValue),
        String.format("Extracted value for key '%s' should be of type %s but was %s",
            key, expectedType.getSimpleName(), actualValue.getClass().getSimpleName()));

    // Check the value
    assertEquals(actualValue, expectedValue,
        String.format("Extracted value for key '%s' should be %s", key, expectedValue));
  }

  @Test
  public void testStringToMapWithMixedTypes() {
    // Test a JSON object with all supported types
    String json = "{"
        + "\"name\":\"Alice\","
        + "\"age\":30,"
        + "\"height\":5.6,"
        + "\"weight\":65.5,"
        + "\"active\":true,"
        + "\"verified\":false,"
        + "\"score\":100,"
        + "\"rating\":4.8,"
        + "\"id\":1234567890"
        + "}";

    Map<String, Object> result = StringToMapFunctions.stringToMap(json);

    assertNotNull(result);
    assertEquals(result.size(), 9);

    // String
    assertEquals(result.get("name"), "Alice");
    assertTrue(result.get("name") instanceof String);

    // Integer
    assertEquals(result.get("age"), 30);
    assertTrue(result.get("age") instanceof Integer);

    // Double (from float-like values)
    assertEquals(result.get("height"), 5.6);
    assertTrue(result.get("height") instanceof Double);

    assertEquals(result.get("weight"), 65.5);
    assertTrue(result.get("weight") instanceof Double);

    // Boolean
    assertEquals(result.get("active"), true);
    assertTrue(result.get("active") instanceof Boolean);

    assertEquals(result.get("verified"), false);
    assertTrue(result.get("verified") instanceof Boolean);

    // Integer
    assertEquals(result.get("score"), 100);
    assertTrue(result.get("score") instanceof Integer);

    // Double
    assertEquals(result.get("rating"), 4.8);
    assertTrue(result.get("rating") instanceof Double);
  }
}
