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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class JsonFunctionsTest {

  @Test
  public void testJsonFunction() throws JsonProcessingException {
    String jsonString =
        "{" + "  \"id\": \"7044885078\"," + "  \"type\": \"CreateEvent\"," + "  \"actor\": {" + "    \"id\": 33500718,"
            + "    \"login\": \"dipper-github-icn-bom-cdg\"," + "    \"display_login\": \"dipper-github-icn-bom-cdg\","
            + "    \"gravatar_id\": \"\"," + "    \"url\": \"https://api.github.com/users/dipper-github-icn-bom-cdg\","
            + "    \"avatar_url\": \"https://avatars.githubusercontent.com/u/33500718?\"" + "  }," + "  \"repo\": {"
            + "    \"id\": 112368043," + "    \"name\": \"dipper-github-icn-bom-cdg/test-ruby-sample\","
            + "    \"url\": \"https://api.github.com/repos/dipper-github-icn-bom-cdg/test-ruby-sample\"" + "  },"
            + "  \"payload\": {" + "    \"ref\": \"canary-test-7f3af0db-3ffa-4259-894f-950d2c76594b\","
            + "    \"ref_type\": \"branch\"," + "    \"master_branch\": \"master\"," + "    \"description\": null,"
            + "    \"pusher_type\": \"user\"" + "  }," + "  \"public\": true,"
            + "  \"created_at\": \"2018-01-01T11:12:53Z\"" + "}";
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.id"), "33500718");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.id"), 33500718L);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.id"), 33500718.0);
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.aaa", "null"), "null");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.aaa", 100L), 100L);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.aaa", 53.2), 53.2);
  }

  @Test
  public void testJsonFunctionExtractingArray() throws JsonProcessingException {
    String jsonString =
        "{\n" + "    \"name\": \"Pete\",\n" + "    \"age\": 24,\n" + "    \"subjects\": [\n" + "        {\n"
            + "            \"name\": \"maths\",\n" + "            \"homework_grades\": [80, 85, 90, 95, 100],\n"
            + "            \"grade\": \"A\"\n" + "        },\n" + "        {\n" + "            \"name\": \"english\",\n"
            + "            \"homework_grades\": [60, 65, 70, 85, 90],\n" + "            \"grade\": \"B\"\n"
            + "        }\n" + "    ]\n" + "}";
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(jsonString, "$.subjects[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
  }

  @Test
  public void testJsonFunctionOnJsonArray() throws JsonProcessingException {
    String jsonArrayString = "[\n" + "        {\n" + "            \"name\": \"maths\",\n"
        + "            \"grade\": \"A\",\n" + "            \"homework_grades\": [80, 85, 90, 95, 100],\n"
        + "            \"score\": 90\n" + "        },\n" + "        {\n" + "            \"name\": \"english\",\n"
        + "            \"grade\": \"B\",\n" + "            \"homework_grades\": [60, 65, 70, 85, 90],\n"
        + "            \"score\": 50\n" + "        }\n" + "]";
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertEquals(JsonFunctions.jsonPathArray(jsonArrayString, "$.[*].score"), new Integer[]{90, 50});
  }

  @Test
  public void testJsonFunctionOnList() throws JsonProcessingException {
    List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
    rawData.add(ImmutableMap.of("name", "maths", "grade", "A", "score", 90, "homework_grades",
        Arrays.asList(80, 85, 90, 95, 100)));
    rawData.add(ImmutableMap.of("name", "english", "grade", "B", "score", 50, "homework_grades",
        Arrays.asList(60, 65, 70, 85, 90)));
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].score"), new Integer[]{90, 50});
  }

  @Test
  public void testJsonFunctionOnObjectArray() throws JsonProcessingException {
    Object[] rawData = new Object[]{ImmutableMap.of("name", "maths", "grade", "A", "score", 90, "homework_grades",
        Arrays.asList(80, 85, 90, 95, 100)), ImmutableMap.of("name", "english", "grade", "B", "score", 50,
            "homework_grades", Arrays.asList(60, 65, 70, 85, 90))};
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].name"), new String[]{"maths", "english"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].grade"), new String[]{"A", "B"});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].homework_grades"),
        new Object[]{Arrays.asList(80, 85, 90, 95, 100), Arrays.asList(60, 65, 70, 85, 90)});
    assertEquals(JsonFunctions.jsonPathArray(rawData, "$.[*].score"), new Integer[]{90, 50});
  }
}
