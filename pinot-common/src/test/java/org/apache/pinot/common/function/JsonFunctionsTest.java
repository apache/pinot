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
package org.apache.pinot.common.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.InvalidJsonException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class JsonFunctionsTest {

  @Test
  public void testJsonFunction()
      throws JsonProcessingException {

    // CHECKSTYLE:OFF
    // @formatter:off
    String jsonString = "{" +
        "  \"id\": \"7044885078\"," +
        "  \"type\": \"CreateEvent\"," +
        "  \"actor\": {" +
        "    \"id\": 33500718," +
        "    \"login\": \"dipper-github-icn-bom-cdg\"," +
        "    \"display_login\": \"dipper-github-icn-bom-cdg\"," +
        "    \"gravatar_id\": \"\"," +
        "    \"url\": \"https://api.github.com/users/dipper-github-icn-bom-cdg\"," +
        "    \"avatar_url\": \"https://avatars.githubusercontent.com/u/33500718?\"" +
        "  }," +
        "  \"repo\": {" +
        "    \"id\": 112368043," +
        "    \"name\": \"dipper-github-icn-bom-cdg/test-ruby-sample\"," +
        "    \"url\": \"https://api.github.com/repos/dipper-github-icn-bom-cdg/test-ruby-sample\"" +
        "  }," +
        "  \"payload\": {" +
        "    \"ref\": \"canary-test-7f3af0db-3ffa-4259-894f-950d2c76594b\"," +
        "    \"ref_type\": \"branch\"," +
        "    \"master_branch\": \"master\"," +
        "    \"description\": null," +
        "    \"pusher_type\": \"user\"" +
        "  }," +
        "  \"public\": true," +
        "  \"created_at\": \"2018-01-01T11:12:53Z\"" +
        "}";
    // @formatter:on
    // CHECKSTYLE:ON
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.actor.id"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.id"), "33500718");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.id"), 33500718L);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.id"), 33500718.0);
    assertFalse(JsonFunctions.jsonPathExists(jsonString, "$.actor.aaa"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.aaa", "null"), "null");
    assertEquals(JsonFunctions.jsonPathString("not json", "$.actor.aaa", "null"), "null");
    assertEquals(JsonFunctions.jsonPathString(null, "$.actor.aaa", "null"), "null");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.aaa", 100L), 100L);
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.aaa"), Long.MIN_VALUE);
    assertEquals(JsonFunctions.jsonPathLong("not json", "$.actor.aaa", Long.MIN_VALUE), Long.MIN_VALUE);
    assertEquals(JsonFunctions.jsonPathLong(null, "$.actor.aaa", Long.MIN_VALUE), Long.MIN_VALUE);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.aaa", 53.2), 53.2);
    assertEquals(JsonFunctions.jsonPathDouble("not json", "$.actor.aaa", 53.2), 53.2);
    assertEquals(JsonFunctions.jsonPathDouble(null, "$.actor.aaa", 53.2), 53.2);
    assertTrue(Double.isNaN(JsonFunctions.jsonPathDouble(jsonString, "$.actor.aaa")));
  }

  @Test
  public void testJsonPathStringWithDefaultValue()
      throws JsonProcessingException {
    String jsonString = "{\"name\": \"Pete\", \"age\": 24}";
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.name"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.name", "default"), "Pete");
    assertFalse(JsonFunctions.jsonPathExists(jsonString, "$.missing"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.missing", "default"), "default");
    assertNull(JsonFunctions.jsonPathString(jsonString, "$.missing", null));
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.age"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.age", "default"), "24");
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.age"), "24");
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.age", null), "24");
  }

  @Test
  public void testJsonPathStringWithoutDefaultValue()
      throws JsonProcessingException {
    String jsonString = "{\"name\": \"Pete\", \"age\": 24}";
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.name"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.name"), "Pete");
    assertFalse(JsonFunctions.jsonPathExists(jsonString, "$.missing"));
    assertNull(JsonFunctions.jsonPathString(jsonString, "$.missing"));
    assertNull(JsonFunctions.jsonPathString(jsonString, "$.missing", null));
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.age"));
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.age"), "24");
  }

  @Test
  public void testJsonPathStringWithInvalidJson()
      throws JsonProcessingException {
    try {
      JsonFunctions.jsonPathString("not json", "$.anything");
      Assert.fail("Should have thrown InvalidJsonException");
    } catch (InvalidJsonException e) {
      // Expected
    }
    try {
      JsonFunctions.jsonPathString(null, "$.anything");
      Assert.fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    assertEquals(JsonFunctions.jsonPathString(null, "$.actor.aaa", "foo"), "foo");
  }

  @Test
  public void testJsonPathStringWithNullValue()
      throws JsonProcessingException {
    String result = JsonFunctions.jsonPathString("{\"foo\": null}", "$.foo");

    assertNull(result, "Expected null json value. Received instead "
        + (result == null ? "Java null value" : result + " of type " + result.getClass()));

    assertEquals(JsonFunctions.jsonPathString("{\"foo\": null}", "$.foo", "default"), "default");
  }

  @Test
  public void testJsonPathStringWithStringNull()
      throws JsonProcessingException {
    assertEquals(JsonFunctions.jsonPathString("{\"foo\": \"null\"}", "$.foo"), "null");
    assertEquals(JsonFunctions.jsonPathString("{\"foo\": \"null\"}", "$.foo", "default"), "null");
  }

  @Test
  public void testJsonFunctionExtractingArray()
      throws JsonProcessingException {
    // CHECKSTYLE:OFF
    // @formatter:off
    String jsonString = "{\n" +
        "    \"name\": \"Pete\",\n" +
        "    \"age\": 24,\n" +
        "    \"subjects\": [\n" +
        "        {\n" +
        "            \"name\": \"maths\",\n" +
        "            \"homework_grades\": [80, 85, 90, 95, 100],\n" +
        "            \"grade\": \"A\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\": \"english\",\n" +
        "            \"homework_grades\": [60, 65, 70, 85, 90],\n" +
        "            \"grade\": \"B\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
    // @formatter:on
    // CHECKSTYLE:ON
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.subjects[*].name"));
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertFalse(JsonFunctions.jsonPathExists(jsonString, null));
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, null), new Object[0]);
    assertFalse(JsonFunctions.jsonPathExists(jsonString, "not json"));
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "not json"), new Object[0]);
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.subjects[*].missing"));
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].missing"), new Object[0]);
  }

  @Test
  public void testJsonFunctionExtractingArrayWithMissingField()
      throws JsonProcessingException {
    String jsonString = "{\"name\": \"Pete\", \"age\": 24}";

    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].name"), new String[]{});
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].name"), new String[]{});
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].grade"), new String[]{});
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].homework_grades"), new Object[]{});

    // jsonPathArrayDefaultEmpty should work fine with existing fields.
    // CHECKSTYLE:OFF
    // @formatter:off
    jsonString = "{\n" +
        "    \"name\": \"Pete\",\n" +
        "    \"age\": 24,\n" +
        "    \"subjects\": [\n" +
        "        {\n" +
        "            \"name\": \"maths\",\n" +
        "            \"homework_grades\": [80, 85, 90, 95, 100],\n" +
        "            \"grade\": \"A\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\": \"english\",\n" +
        "            \"homework_grades\": [60, 65, 70, 85, 90],\n" +
        "            \"grade\": \"B\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
    // @formatter:on
    // CHECKSTYLE:ON
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.subjects[*].name"));
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].name"),
        new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].grade"), new String[]{"A", "B"});
    assertTrue(JsonFunctions.jsonPathExists(jsonString, "$.subjects[*].homework_grades"));
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
  }

  @Test
  public void testJsonFunctionExtractingArrayWithObjectArray()
      throws JsonProcessingException {
    // ImmutableList works fine with JsonPath with default JacksonJsonProvider. But on ingestion
    // path, JSONRecordExtractor converts all Collections in parsed JSON object to Object[].
    // Object[] doesn't work with default JsonPath, where "$.commits[*].sha" would return empty,
    // and "$.commits[1].sha" led to exception `Filter: [1]['sha'] can only be applied to arrays`.
    // Those failure could be reproduced by using the default JacksonJsonProvider for JsonPath.
    Map<String, Object> rawData = ImmutableMap.of("commits",
        ImmutableList.of(ImmutableMap.of("sha", 123, "name", "k"), ImmutableMap.of("sha", 456, "name", "j")));
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.commits[*].sha"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.commits[*].sha"), new Integer[]{123, 456});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.commits[1].sha"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.commits[1].sha"), new Integer[]{456});

    // ArrayAwareJacksonJsonProvider should fix this issue.
    rawData = ImmutableMap.of("commits",
        new Object[]{ImmutableMap.of("sha", 123, "name", "k"), ImmutableMap.of("sha", 456, "name", "j")});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.commits[*].sha"), new Integer[]{123, 456});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.commits[1].sha"), new Integer[]{456});
  }

  @Test
  public void testJsonFunctionExtractingArrayWithTopLevelObjectArray()
      throws JsonProcessingException {
    // JSON formatted string works fine with JsonPath, and we used to serialize Object[]
    // to JSON formatted string for JsonPath to work.
    String rawDataInStr = "[{\"sha\": 123, \"name\": \"k\"}, {\"sha\": 456, \"name\": \"j\"}]";
    assertTrue(JsonFunctions.jsonPathExists(rawDataInStr, "$.[*].sha"));
    assertEquals(JsonFunctions.jsonPathArray(rawDataInStr, "$.[*].sha"), new Integer[]{123, 456});
    assertTrue(JsonFunctions.jsonPathExists(rawDataInStr, "$.[1].sha"));
    assertEquals(JsonFunctions.jsonPathArray(rawDataInStr, "$.[1].sha"), new Integer[]{456});

    // ArrayAwareJacksonJsonProvider can work with Array directly, thus no need to serialize
    // Object[] any more.
    Object[] rawDataInAry =
        new Object[]{ImmutableMap.of("sha", 123, "name", "kk"), ImmutableMap.of("sha", 456, "name", "jj")};
    assertEquals(JsonFunctions.jsonPathArray(rawDataInAry, "$.[*].sha"), new Integer[]{123, 456});
    assertEquals(JsonFunctions.jsonPathArray(rawDataInAry, "$.[1].sha"), new Integer[]{456});
  }

  @Test
  public void testJsonFunctionOnJsonArray()
      throws JsonProcessingException {
    // CHECKSTYLE:OFF
    // @formatter:off
    String jsonArrayString =
        "[\n" +
            "        {\n" +
            "            \"name\": \"maths\",\n" +
            "            \"grade\": \"A\",\n" +
            "            \"homework_grades\": [80, 85, 90, 95, 100],\n" +
            "            \"score\": 90\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\": \"english\",\n" +
            "            \"grade\": \"B\",\n" +
            "            \"homework_grades\": [60, 65, 70, 85, 90],\n" +
            "            \"score\": 50\n" +
            "        }\n" +
            "]";
    // @formatter:on
    // CHECKSTYLE:ON
    assertTrue(JsonFunctions.jsonPathExists(jsonArrayString, "$.[*].name"));
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].name"), new String[]{"maths", "english"});
    assertTrue(JsonFunctions.jsonPathExists(jsonArrayString, "$.[*].grade"));
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].grade"), new String[]{"A", "B"});
    assertTrue(JsonFunctions.jsonPathExists(jsonArrayString, "$.[*].homework_grades"));
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertTrue(JsonFunctions.jsonPathExists(jsonArrayString, "$.[*].score"));
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].score"), new Integer[]{90, 50});
  }

  @Test
  public void testJsonFunctionOnList()
      throws JsonProcessingException {
    List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
    rawData.add(ImmutableMap
        .of("name", "maths", "grade", "A", "score", 90, "homework_grades", Arrays.asList(80, 85, 90, 95, 100)));
    rawData.add(ImmutableMap
        .of("name", "english", "grade", "B", "score", 50, "homework_grades", Arrays.asList(60, 65, 70, 85, 90)));
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].name"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].name"), new String[]{"maths", "english"});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].grade"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].grade"), new String[]{"A", "B"});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].homework_grades"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].score"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].score"), new Integer[]{90, 50});
  }

  @Test
  public void testJsonFunctionOnObjectArray()
      throws JsonProcessingException {
    Object[] rawData = new Object[]{
        ImmutableMap.of("name", "maths", "grade", "A", "score", 90, "homework_grades",
            Arrays.asList(80, 85, 90, 95, 100)),
        ImmutableMap.of("name", "english", "grade", "B", "score", 50, "homework_grades",
            Arrays.asList(60, 65, 70, 85, 90))
    };
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].name"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].name"), new String[]{"maths", "english"});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].grade"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].grade"), new String[]{"A", "B"});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].homework_grades"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertTrue(JsonFunctions.jsonPathExists(rawData, "$.[*].score"));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].score"), new Integer[]{90, 50});
  }

  @DataProvider
  public static Object[][] jsonPathStringTestCases() {
    return new Object[][]{
        {ImmutableMap.of("foo", "x", "bar", ImmutableMap.of("foo", "y")), "$.foo", "x"},
        {ImmutableMap.of("foo", "x", "bar", ImmutableMap.of("foo", "y")), "$.qux", null},
        {ImmutableMap.of("foo", "x", "bar", ImmutableMap.of("foo", "y")), "$.bar", "{\"foo\":\"y\"}"},
    };
  }

  @Test(dataProvider = "jsonPathStringTestCases")
  public void testJsonPathString(Map<String, Object> map, String path, String expected)
      throws JsonProcessingException {
    String value = JsonFunctions.jsonPathString(JsonUtils.objectToString(map), path);
    assertEquals(value, expected);
  }

  @Test(dataProvider = "jsonPathStringTestCases")
  public void testJsonPathStringWithDefaultValue(Map<String, Object> map, String path, String expected)
      throws JsonProcessingException {
    String value = JsonFunctions.jsonPathString(JsonUtils.objectToString(map), path, expected);
    assertEquals(value, expected);
  }

  @DataProvider
  public static Object[][] jsonPathArrayTestCases() {
    return new Object[][]{
        {ImmutableMap.of("foo", "x", "bar", ImmutableMap.of("foo", "y")), "$.foo", new Object[]{"x"}},
        {ImmutableMap.of("foo", "x", "bar", ImmutableMap.of("foo", "y")), "$.qux", null},
        {
            ImmutableMap.of("foo", "x", "bar", ImmutableMap.of("foo", "y")), "$.bar", new Object[]{
            ImmutableMap.of("foo", "y")
        }
        },
    };
  }

  @Test(dataProvider = "jsonPathArrayTestCases")
  public void testJsonPathArray(Map<String, Object> map, String path, Object[] expected)
      throws JsonProcessingException {
    Object[] value = JsonFunctions.jsonPathArray(JsonUtils.objectToString(map), path);
    if (expected == null) {
      assertNull(value);
    } else {
      assertEquals(value.length, expected.length);
      for (int i = 0; i < value.length; i++) {
        assertEquals(value[i], expected[i]);
      }
    }
  }

  @Test
  public void testJsonPathExistsNullObject() {
    assertFalse(JsonFunctions.jsonPathExists(null, "$.[*].name"));
    assertFalse(JsonFunctions.jsonPathExists(null, null));
  }

  @Test
  public void testJsonKeyValueArrayToMap() {
    String jsonString = "["
        + "{\"key\": \"k1\", \"value\": \"v1\"}, "
        + "{\"key\": \"k2\", \"value\": \"v2\"}, "
        + "{\"key\": \"k3\", \"value\": \"v3\"}, "
        + "{\"key\": \"k4\", \"value\": \"v4\"}, "
        + "{\"key\": \"k5\", \"value\": \"v5\"}"
        + "]";
    Map<String, Object> expected = ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5");
    assertEquals(JsonFunctions.jsonKeyValueArrayToMap(jsonString), expected);

    Object[] jsonArray = new Object[]{
        "{\"key\": \"k1\", \"value\": \"v1\"}",
        "{\"key\": \"k2\", \"value\": \"v2\"}",
        "{\"key\": \"k3\", \"value\": \"v3\"}",
        "{\"key\": \"k4\", \"value\": \"v4\"}",
        "{\"key\": \"k5\", \"value\": \"v5\"}"
    };
    assertEquals(JsonFunctions.jsonKeyValueArrayToMap(jsonArray), expected);

    List<Object> jsonList = ImmutableList.of(
        "{\"key\": \"k1\", \"value\": \"v1\"}",
        "{\"key\": \"k2\", \"value\": \"v2\"}",
        "{\"key\": \"k3\", \"value\": \"v3\"}",
        "{\"key\": \"k4\", \"value\": \"v4\"}",
        "{\"key\": \"k5\", \"value\": \"v5\"}"
    );
    assertEquals(JsonFunctions.jsonKeyValueArrayToMap(jsonList), expected);
  }

  @Test
  public void testJsonStringToCollection() {
    String jsonArrayString = "[{\"k1\":\"v1\"}, {\"k2\":\"v2\"}, {\"k3\":\"v3\"}, {\"k4\":\"v4\"}, {\"k5\":\"v5\"}]";
    List<Map<String, String>> expectedArray =
        List.of(Map.of("k1", "v1"), Map.of("k2", "v2"), Map.of("k3", "v3"), Map.of("k4", "v4"), Map.of("k5", "v5"));
    assertEquals(JsonFunctions.jsonStringToArray(jsonArrayString), expectedArray);
    assertEquals(JsonFunctions.jsonStringToListOrMap(jsonArrayString), expectedArray);

    String jsonMapString = "{\"k1\":\"v1\", \"k2\":\"v2\", \"k3\":\"v3\", \"k4\":\"v4\",\"k5\":\"v5\"}";
    Map<String, String> expectedMap =
        Map.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5");
    assertEquals(JsonFunctions.jsonStringToMap(jsonMapString), expectedMap);
    assertEquals(JsonFunctions.jsonStringToListOrMap(jsonMapString), expectedMap);

    String invalidJson = "[\"k1\":\"v1\"}";
    assertNull(JsonFunctions.jsonStringToMap(invalidJson));
    assertNull(JsonFunctions.jsonStringToListOrMap(invalidJson));
  }

  @Test
  public void testJsonKeysFlatAndNested()
      throws IOException {
    String flatJson = "{\"a\":1,\"b\":2}";
    String nestedJson = "{\"a\":1,\"b\":{\"c\":2,\"d\":3},\"f\":4}";

    // For extracting all keys at all levels, use $..**
    Assert.assertEqualsNoOrder(JsonFunctions.jsonExtractKey(flatJson, "$..**", "maxDepth=1").toArray(),
        new String[]{"$['a']", "$['b']"});

    // Test with nested JSON - $.** should give us all paths
    List<String> nestedResult = JsonFunctions.jsonExtractKey(nestedJson, "$..**", "maxDepth=2");
    System.out.println("Nested result: " + nestedResult);

    // Just test that we get some results for now
    Assert.assertTrue(nestedResult.size() > 0);
  }

  @Test
  public void testJsonKeysArrayAndNull()
      throws IOException {
    String arrayJson = "[{\"a\":1},{\"b\":2}]";
    List<String> result = JsonFunctions.jsonExtractKey(arrayJson, "$..**", "maxDepth=2");
    System.out.println("Array result: " + result);

    // Test null and invalid cases
    Assert.assertEquals(JsonFunctions.jsonExtractKey(null, "$..**", "maxDepth=2").size(), 0);
    Assert.assertEquals(JsonFunctions.jsonExtractKey("not a json", "$..**", "maxDepth=2").size(), 0);
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{\"a\":1}", "$..**", "maxDepth=0").size(), 0);
  }

  @Test
  public void testJsonKeysEdgeCases()
      throws IOException {
    // Test with negative depth
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{\"a\":1}", "$..**", "maxDepth=-1").size(), 1);

    // Test with empty string
    Assert.assertEquals(JsonFunctions.jsonExtractKey("", "$..**", "maxDepth=1").size(), 0);

    // Test with null JSON value
    Assert.assertEquals(JsonFunctions.jsonExtractKey("null", "$..**", "maxDepth=1").size(), 0);

    // Test with empty JSON object
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{}", "$..**", "maxDepth=1").size(), 0);

    // Test with empty JSON array
    Assert.assertEquals(JsonFunctions.jsonExtractKey("[]", "$..**", "maxDepth=1").size(), 0);

    // Test with various object types
    Map<String, Object> mapObj = new java.util.HashMap<>();
    mapObj.put("key1", "value1");
    mapObj.put("key2", 42);
    List<String> mapResult = JsonFunctions.jsonExtractKey(mapObj, "$..**", "maxDepth=1");
    System.out.println("Map result: " + mapResult);
    Assert.assertTrue(mapResult.size() > 0);

    List<Object> listObj = new java.util.ArrayList<>();
    listObj.add(Map.of("key1", "value1"));
    listObj.add(Map.of("key2", "value2"));
    List<String> listResult = JsonFunctions.jsonExtractKey(listObj, "$..**", "maxDepth=2");
    System.out.println("List result: " + listResult);
    Assert.assertTrue(listResult.size() > 0);

    String deepJson = "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}";
    List<String> deepResult = JsonFunctions.jsonExtractKey(deepJson, "$..**", "maxDepth=3");
    System.out.println("Deep result: " + deepResult);
    Assert.assertTrue(deepResult.size() > 0);
  }

  @Test
  public void testJsonExtractKeyDotNotation()
      throws IOException {
    String nestedJson = "{\"a\":1,\"b\":{\"c\":2,\"d\":{\"e\":3}}}";

    // Test 4-parameter version with dotNotation=true
    List<String> dotNotationResult = JsonFunctions.jsonExtractKey(nestedJson, "$..**", "maxDepth=3;dotNotation=true");
    List<String> expectedDotNotation = Arrays.asList("a", "b", "b.c", "b.d", "b.d.e");
    Assert.assertEqualsNoOrder(dotNotationResult.toArray(), expectedDotNotation.toArray());

    // Test 4-parameter version with dotNotation=false (JsonPath format)
    List<String> jsonPathResult = JsonFunctions.jsonExtractKey(nestedJson, "$..**", "maxDepth=3;dotNotation=false");
    List<String> expectedJsonPath = Arrays.asList("$['a']", "$['b']", "$['b']['c']", "$['b']['d']", "$['b']['d']['e']");
    Assert.assertEqualsNoOrder(jsonPathResult.toArray(), expectedJsonPath.toArray());

    // Test with arrays in dot notation
    String arrayJson = "{\"users\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}";
    List<String> arrayDotResult = JsonFunctions.jsonExtractKey(arrayJson, "$..**", "maxDepth=3;dotNotation=true");
    List<String> expectedArrayDot = Arrays.asList("users", "users.0", "users.0.name", "users.1", "users.1.name");
    Assert.assertEqualsNoOrder(arrayDotResult.toArray(), expectedArrayDot.toArray());
  }

  @Test
  public void testJsonExtractKeyDepthLimiting()
      throws IOException {
    String deepJson = "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}";

    // Test depth=1 (only top level)
    List<String> depth1 = JsonFunctions.jsonExtractKey(deepJson, "$..**", "maxDepth=1");
    Assert.assertEquals(depth1, Arrays.asList("$['a']"));

    // Test depth=2
    List<String> depth2 = JsonFunctions.jsonExtractKey(deepJson, "$..**", "maxDepth=2");
    Assert.assertEqualsNoOrder(depth2.toArray(), new String[]{"$['a']", "$['a']['b']"});

    // Test depth=3
    List<String> depth3 = JsonFunctions.jsonExtractKey(deepJson, "$..**", "maxDepth=3");
    Assert.assertEqualsNoOrder(depth3.toArray(), new String[]{"$['a']", "$['a']['b']", "$['a']['b']['c']"});

    // Test depth=4 (includes all levels)
    List<String> depth4 = JsonFunctions.jsonExtractKey(deepJson, "$..**", "maxDepth=4");
    Assert.assertEqualsNoOrder(depth4.toArray(),
        new String[]{"$['a']", "$['a']['b']", "$['a']['b']['c']", "$['a']['b']['c']['d']"});
  }

  @Test
  public void testJsonExtractKeyRecursiveExpressions()
      throws IOException {
    String json = "{\"a\":1,\"b\":{\"c\":2,\"d\":3}}";

    // Test $..**
    List<String> recursiveResult = JsonFunctions.jsonExtractKey(json, "$..**", "maxDepth=-1");
    List<String> expected = Arrays.asList("$['a']", "$['b']", "$['b']['c']", "$['b']['d']");
    Assert.assertEqualsNoOrder(recursiveResult.toArray(), expected.toArray());

    // Test $.. (should work the same as $..**)
    List<String> dotDotResult = JsonFunctions.jsonExtractKey(json, "$..", "maxDepth=-1");
    Assert.assertEqualsNoOrder(dotDotResult.toArray(), expected.toArray());

    // Test with mixed object and array structure
    String mixedJson = "{\"data\":[{\"id\":1,\"info\":{\"name\":\"test\"}}]}";
    List<String> mixedResult = JsonFunctions.jsonExtractKey(mixedJson, "$..**", "maxDepth=2147483647");
    List<String> expectedMixed = Arrays.asList(
        "$['data']", "$['data'][0]", "$['data'][0]['id']", "$['data'][0]['info']", "$['data'][0]['info']['name']");
    Assert.assertEqualsNoOrder(mixedResult.toArray(), expectedMixed.toArray());
  }

  @Test
  public void testJsonExtractKeyArrayHandling()
      throws IOException {
    String arrayJson = "[{\"a\":1},{\"b\":2},{\"c\":{\"d\":3}}]";

    // Test recursive extraction from array
    List<String> result = JsonFunctions.jsonExtractKey(arrayJson, "$..**", "maxDepth=3");
    List<String> expected = Arrays.asList("$[0]", "$[0]['a']", "$[1]", "$[1]['b']", "$[2]", "$[2]['c']",
        "$[2]['c']['d']");
    Assert.assertEqualsNoOrder(result.toArray(), expected.toArray());

    // Test with dot notation
    List<String> dotResult = JsonFunctions.jsonExtractKey(arrayJson, "$..**", "maxDepth=3;dotNotation=true");
    List<String> expectedDot = Arrays.asList("0", "0.a", "1", "1.b", "2", "2.c", "2.c.d");
    Assert.assertEqualsNoOrder(dotResult.toArray(), expectedDot.toArray());
  }

  @Test
  public void testJsonExtractKeyComplexStructures()
      throws IOException {
    // Test complex nested structure with various data types
    String complexJson = "{"
        + "\"users\":{"
        + "  \"active\":[{\"id\":1,\"profile\":{\"name\":\"Alice\",\"settings\":{\"theme\":\"dark\"}}}],"
        + "  \"inactive\":[{\"id\":2,\"profile\":{\"name\":\"Bob\"}}]"
        + "},"
        + "\"metadata\":{\"version\":\"1.0\",\"tags\":[\"important\",\"test\"]}"
        + "}";

    // Test with depth limiting
    List<String> depth2Result = JsonFunctions.jsonExtractKey(complexJson, "$..**", "maxDepth=2;dotNotation=true");
    Assert.assertTrue(depth2Result.contains("users"));
    Assert.assertTrue(depth2Result.contains("metadata"));
    Assert.assertTrue(depth2Result.contains("users.active"));
    Assert.assertTrue(depth2Result.contains("users.inactive"));
    Assert.assertTrue(depth2Result.contains("metadata.version"));
    Assert.assertTrue(depth2Result.contains("metadata.tags"));

    // Ensure we don't get deeper levels
    Assert.assertFalse(depth2Result.contains("users.active.0"));
    Assert.assertFalse(depth2Result.contains("metadata.tags.0"));
  }

  @Test
  public void testJsonExtractKeyNonRecursiveExpressions()
      throws IOException {
    String json = "{\"a\":1,\"b\":{\"c\":2,\"d\":3}}";

    // Test $.*  (top level only)
    List<String> topLevelResult = JsonFunctions.jsonExtractKey(json, "$.*", "maxDepth=-3");
    List<String> expectedTopLevel = Arrays.asList("$['a']", "$['b']");
    Assert.assertEqualsNoOrder(topLevelResult.toArray(), expectedTopLevel.toArray());

    // Test specific path $.b.*
    List<String> specificResult = JsonFunctions.jsonExtractKey(json, "$.b.*", "maxDepth=-1");
    List<String> expectedSpecific = Arrays.asList("$['b']['c']", "$['b']['d']");
    Assert.assertEqualsNoOrder(specificResult.toArray(), expectedSpecific.toArray());
  }

  @Test
  public void testJsonExtractKeyEdgeCasesWithDotNotation()
      throws IOException {
    // Test with zero depth
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{\"a\":1}", "$..**", "maxDepth=0;dotNotation=true").size(), 0);
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{\"a\":1}", "$..**", "maxDepth=0;dotNotation=false").size(), 0);

    // Test with negative depth
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{\"a\":1}", "$..**", "maxDepth=-1;dotNotation=true").size(), 1);
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{\"a\":1}", "$..**", "maxDepth=-1;dotNotation=false").size(), 1);

    // Test with empty objects and arrays
    Assert.assertEquals(JsonFunctions.jsonExtractKey("{}", "$..**", "maxDepth=5;dotNotation=true").size(), 0);
    Assert.assertEquals(JsonFunctions.jsonExtractKey("[]", "$..**", "maxDepth=5;dotNotation=false").size(), 0);

    // Test with invalid JSON
    Assert.assertEquals(JsonFunctions.jsonExtractKey("invalid json", "$..**", "maxDepth=5;dotNotation=true").size(), 0);
    Assert.assertEquals(JsonFunctions.jsonExtractKey(null, "$..**", "maxDepth=5;dotNotation=true").size(), 0);
  }

  @Test
  public void testJsonExtractKeyBackwardCompatibility()
      throws IOException {
    String json = "{\"a\":1,\"b\":{\"c\":2}}";

    // Test 2-parameter version (should default to maxDepth=Integer.MAX_VALUE, dotNotation=false)
    List<String> twoParamResult = JsonFunctions.jsonExtractKey(json, "$..**");
    List<String> fourParamResult = JsonFunctions.jsonExtractKey(json, "$..**", "maxDepth=2147483647;dotNotation=false");
    Assert.assertEquals(twoParamResult, fourParamResult);

    // Test 3-parameter version (should default to dotNotation=false)
    List<String> threeParamResult = JsonFunctions.jsonExtractKey(json, "$..**", "maxDepth=2");
    List<String> fourParamResultWithDepth = JsonFunctions.jsonExtractKey(json, "$..**", "maxDepth=2;dotNotation=false");
    Assert.assertEquals(threeParamResult, fourParamResultWithDepth);
  }

  @Test
  public void testJsonExtractKeySpecialCharacters()
      throws IOException {
    String specialJson = "{"
        + "\"field-with-dash\":1,"
        + "\"field.with.dots\":2,"
        + "\"field_with_underscores\":3,"
        + "\"field with spaces\":4"
        + "}";

    // Test with special characters in field names
    List<String> result = JsonFunctions.jsonExtractKey(specialJson, "$..**", "maxDepth=1;dotNotation=true");
    Assert.assertTrue(result.contains("field-with-dash"));
    Assert.assertTrue(result.contains("field.with.dots"));
    Assert.assertTrue(result.contains("field_with_underscores"));
    Assert.assertTrue(result.contains("field with spaces"));

    // Test JsonPath format
    List<String> jsonPathResult = JsonFunctions.jsonExtractKey(specialJson, "$..**", "maxDepth=1;dotNotation=false");
    Assert.assertTrue(jsonPathResult.contains("$['field-with-dash']"));
    Assert.assertTrue(jsonPathResult.contains("$['field.with.dots']"));
    Assert.assertTrue(jsonPathResult.contains("$['field_with_underscores']"));
    Assert.assertTrue(jsonPathResult.contains("$['field with spaces']"));
  }
}
