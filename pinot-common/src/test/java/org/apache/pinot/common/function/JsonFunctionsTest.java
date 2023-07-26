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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.id"), "33500718");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.id"), 33500718L);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.id"), 33500718.0);
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
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, null), new Object[0]);
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "not json"), new Object[0]);
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
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].name"),
        new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArrayDefaultEmpty(jsonString, "$.subjects[*].grade"), new String[]{"A", "B"});
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
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.commits[*].sha"), new Integer[]{123, 456});
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
    assertEquals(JsonFunctions.jsonPathArray(rawDataInStr, "$.[*].sha"), new Integer[]{123, 456});
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
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
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
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
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
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
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
}
