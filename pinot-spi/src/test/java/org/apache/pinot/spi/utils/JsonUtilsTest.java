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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class JsonUtilsTest {
  private static final String JSON_FILE = "json_util_test.json";

  @Test
  public void testFlatten()
      throws IOException {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("null");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("123");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      assertEquals(flattenedRecords.get(0), Collections.singletonMap("", "123"));
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("[]");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("[1,2,3]");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 3);
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.size(), 2);
      assertEquals(flattenedRecord0.get(".$index"), "0");
      assertEquals(flattenedRecord0.get("."), "1");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.size(), 2);
      assertEquals(flattenedRecord1.get(".$index"), "1");
      assertEquals(flattenedRecord1.get("."), "2");
      Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
      assertEquals(flattenedRecord2.size(), 2);
      assertEquals(flattenedRecord2.get(".$index"), "2");
      assertEquals(flattenedRecord2.get("."), "3");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("[1,[2,3],[4,[5,6]]]]");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 6);
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.size(), 2);
      assertEquals(flattenedRecord0.get(".$index"), "0");
      assertEquals(flattenedRecord0.get("."), "1");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.size(), 3);
      assertEquals(flattenedRecord1.get(".$index"), "1");
      assertEquals(flattenedRecord1.get("..$index"), "0");
      assertEquals(flattenedRecord1.get(".."), "2");
      Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
      assertEquals(flattenedRecord2.size(), 3);
      assertEquals(flattenedRecord2.get(".$index"), "1");
      assertEquals(flattenedRecord2.get("..$index"), "1");
      assertEquals(flattenedRecord2.get(".."), "3");
      Map<String, String> flattenedRecord3 = flattenedRecords.get(3);
      assertEquals(flattenedRecord3.size(), 3);
      assertEquals(flattenedRecord3.get(".$index"), "2");
      assertEquals(flattenedRecord3.get("..$index"), "0");
      assertEquals(flattenedRecord3.get(".."), "4");
      Map<String, String> flattenedRecord4 = flattenedRecords.get(4);
      assertEquals(flattenedRecord4.size(), 4);
      assertEquals(flattenedRecord4.get(".$index"), "2");
      assertEquals(flattenedRecord4.get("..$index"), "1");
      assertEquals(flattenedRecord4.get("...$index"), "0");
      assertEquals(flattenedRecord4.get("..."), "5");
      Map<String, String> flattenedRecord5 = flattenedRecords.get(5);
      assertEquals(flattenedRecord5.size(), 4);
      assertEquals(flattenedRecord5.get(".$index"), "2");
      assertEquals(flattenedRecord5.get("..$index"), "1");
      assertEquals(flattenedRecord5.get("...$index"), "1");
      assertEquals(flattenedRecord5.get("..."), "6");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("{}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("{\"key\":{}}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("[{},{},{}]");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("{\"key\":[]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("{\"name\":\"adam\",\"age\":20}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      Map<String, String> flattenedRecord = flattenedRecords.get(0);
      assertEquals(flattenedRecord.size(), 2);
      assertEquals(flattenedRecord.get(".name"), "adam");
      assertEquals(flattenedRecord.get(".age"), "20");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},{\"country\":\"ca\",\"street\":\"second st\","
              + "\"number\":2}]");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 2);
      for (Map<String, String> flattenedRecord : flattenedRecords) {
        assertEquals(flattenedRecord.size(), 4);
        assertTrue(flattenedRecord.containsKey(".$index"));
        assertTrue(flattenedRecord.containsKey("..country"));
        assertTrue(flattenedRecord.containsKey("..street"));
        assertTrue(flattenedRecord.containsKey("..number"));
      }
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.get(".$index"), "0");
      assertEquals(flattenedRecord0.get("..country"), "us");
      assertEquals(flattenedRecord0.get("..street"), "main st");
      assertEquals(flattenedRecord0.get("..number"), "1");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.get(".$index"), "1");
      assertEquals(flattenedRecord1.get("..country"), "ca");
      assertEquals(flattenedRecord1.get("..street"), "second st");
      assertEquals(flattenedRecord1.get("..number"), "2");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"adam\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},"
              + "{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 2);
      for (Map<String, String> flattenedRecord : flattenedRecords) {
        assertEquals(flattenedRecord.size(), 5);
        assertEquals(flattenedRecord.get(".name"), "adam");
        assertTrue(flattenedRecord.containsKey(".addresses.$index"));
        assertTrue(flattenedRecord.containsKey(".addresses..country"));
        assertTrue(flattenedRecord.containsKey(".addresses..street"));
        assertTrue(flattenedRecord.containsKey(".addresses..number"));
      }
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord0.get(".addresses..country"), "us");
      assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
      assertEquals(flattenedRecord0.get(".addresses..number"), "1");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.get(".addresses.$index"), "1");
      assertEquals(flattenedRecord1.get(".addresses..country"), "ca");
      assertEquals(flattenedRecord1.get(".addresses..street"), "second st");
      assertEquals(flattenedRecord1.get(".addresses..number"), "2");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"adam\",\"age\":20,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},"
              + "{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}],\"skills\":[\"english\","
              + "\"programming\"]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 4);
      for (Map<String, String> flattenedRecord : flattenedRecords) {
        assertEquals(flattenedRecord.size(), 8);
        assertEquals(flattenedRecord.get(".name"), "adam");
        assertEquals(flattenedRecord.get(".age"), "20");
        assertTrue(flattenedRecord.containsKey(".addresses.$index"));
        assertTrue(flattenedRecord.containsKey(".addresses..country"));
        assertTrue(flattenedRecord.containsKey(".addresses..street"));
        assertTrue(flattenedRecord.containsKey(".addresses..number"));
        assertTrue(flattenedRecord.containsKey(".skills.$index"));
        assertTrue(flattenedRecord.containsKey(".skills."));
      }
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord0.get(".addresses..country"), "us");
      assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
      assertEquals(flattenedRecord0.get(".addresses..number"), "1");
      assertEquals(flattenedRecord0.get(".skills.$index"), "0");
      assertEquals(flattenedRecord0.get(".skills."), "english");
      Map<String, String> flattenedRecord3 = flattenedRecords.get(3);
      assertEquals(flattenedRecord3.get(".addresses.$index"), "1");
      assertEquals(flattenedRecord3.get(".addresses..country"), "ca");
      assertEquals(flattenedRecord3.get(".addresses..street"), "second st");
      assertEquals(flattenedRecord3.get(".addresses..number"), "2");
      assertEquals(flattenedRecord3.get(".skills.$index"), "1");
      assertEquals(flattenedRecord3.get(".skills."), "programming");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"bob\",\"age\":null,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\"}],\"skills\":[],"
              + "\"hobbies\":[null]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      Map<String, String> flattenedRecord = flattenedRecords.get(0);
      assertEquals(flattenedRecord.size(), 4);
      assertEquals(flattenedRecord.get(".name"), "bob");
      assertEquals(flattenedRecord.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord.get(".addresses..country"), "us");
      assertEquals(flattenedRecord.get(".addresses..street"), "main st");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"bob\",\"age\":null,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\"}],\"skills\":[],"
              + "\"hobbies\":[null," + "\"football\"]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      Map<String, String> flattenedRecord = flattenedRecords.get(0);
      assertEquals(flattenedRecord.size(), 6);
      assertEquals(flattenedRecord.get(".name"), "bob");
      assertEquals(flattenedRecord.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord.get(".addresses..country"), "us");
      assertEquals(flattenedRecord.get(".addresses..street"), "main st");
      assertEquals(flattenedRecord.get(".hobbies.$index"), "1");
      assertEquals(flattenedRecord.get(".hobbies."), "football");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"charles\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"types\":[\"home\","
              + "\"office\"]}," + "{\"country\":\"ca\",\"street\":\"second st\"}]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 3);
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.size(), 6);
      assertEquals(flattenedRecord0.get(".name"), "charles");
      assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord0.get(".addresses..country"), "us");
      assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
      assertEquals(flattenedRecord0.get(".addresses..types.$index"), "0");
      assertEquals(flattenedRecord0.get(".addresses..types."), "home");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.size(), 6);
      assertEquals(flattenedRecord1.get(".name"), "charles");
      assertEquals(flattenedRecord1.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord1.get(".addresses..country"), "us");
      assertEquals(flattenedRecord1.get(".addresses..street"), "main st");
      assertEquals(flattenedRecord1.get(".addresses..types.$index"), "1");
      assertEquals(flattenedRecord1.get(".addresses..types."), "office");
      Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
      assertEquals(flattenedRecord2.size(), 4);
      assertEquals(flattenedRecord2.get(".name"), "charles");
      assertEquals(flattenedRecord2.get(".addresses.$index"), "1");
      assertEquals(flattenedRecord2.get(".addresses..country"), "ca");
      assertEquals(flattenedRecord2.get(".addresses..street"), "second st");
    }
  }

  @Test
  public void testFlattenWithMaxLevels()
      throws IOException {
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode("[1,[2,3],[4,[5,6]]]]");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      jsonIndexConfig.setMaxLevels(3);
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      jsonIndexConfig.setMaxLevels(2);
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 4);
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.size(), 2);
      assertEquals(flattenedRecord0.get(".$index"), "0");
      assertEquals(flattenedRecord0.get("."), "1");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.size(), 3);
      assertEquals(flattenedRecord1.get(".$index"), "1");
      assertEquals(flattenedRecord1.get("..$index"), "0");
      assertEquals(flattenedRecord1.get(".."), "2");
      Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
      assertEquals(flattenedRecord2.size(), 3);
      assertEquals(flattenedRecord2.get(".$index"), "1");
      assertEquals(flattenedRecord2.get("..$index"), "1");
      assertEquals(flattenedRecord2.get(".."), "3");
      Map<String, String> flattenedRecord3 = flattenedRecords.get(3);
      assertEquals(flattenedRecord3.size(), 3);
      assertEquals(flattenedRecord3.get(".$index"), "2");
      assertEquals(flattenedRecord3.get("..$index"), "0");
      assertEquals(flattenedRecord3.get(".."), "4");

      jsonIndexConfig.setMaxLevels(1);
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      Map<String, String> flattenedRecord = flattenedRecords.get(0);
      assertEquals(flattenedRecord.size(), 2);
      assertEquals(flattenedRecord.get(".$index"), "0");
      assertEquals(flattenedRecord.get("."), "1");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},{\"country\":\"ca\",\"street\":\"second st\","
              + "\"number\":2}]");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      jsonIndexConfig.setMaxLevels(2);
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      jsonIndexConfig.setMaxLevels(1);
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"adam\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},"
              + "{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}]}");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      jsonIndexConfig.setMaxLevels(3);
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      jsonIndexConfig.setMaxLevels(2);
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "adam"));

      jsonIndexConfig.setMaxLevels(1);
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"charles\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"types\":[\"home\","
              + "\"office\"]}," + "{\"country\":\"ca\",\"street\":\"second st\"}]}");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      jsonIndexConfig.setMaxLevels(4);
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      jsonIndexConfig.setMaxLevels(3);
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 2);
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.size(), 4);
      assertEquals(flattenedRecord0.get(".name"), "charles");
      assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord0.get(".addresses..country"), "us");
      assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.size(), 4);
      assertEquals(flattenedRecord1.get(".name"), "charles");
      assertEquals(flattenedRecord1.get(".addresses.$index"), "1");
      assertEquals(flattenedRecord1.get(".addresses..country"), "ca");
      assertEquals(flattenedRecord1.get(".addresses..street"), "second st");
    }
  }

  @Test
  public void testFlattenWithDisableCrossArrayUnnest()
      throws IOException {
    JsonNode jsonNode = JsonUtils.stringToJsonNode(
        "{\"name\":\"adam\",\"age\":20,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},"
            + "{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}],\"skills\":[\"english\","
            + "\"programming\"]}");
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setDisableCrossArrayUnnest(true);
    List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 4);
    Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
    assertEquals(flattenedRecord0.size(), 6);
    assertEquals(flattenedRecord0.get(".name"), "adam");
    assertEquals(flattenedRecord0.get(".age"), "20");
    assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..country"), "us");
    assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
    assertEquals(flattenedRecord0.get(".addresses..number"), "1");
    Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
    assertEquals(flattenedRecord1.size(), 6);
    assertEquals(flattenedRecord1.get(".name"), "adam");
    assertEquals(flattenedRecord1.get(".age"), "20");
    assertEquals(flattenedRecord1.get(".addresses.$index"), "1");
    assertEquals(flattenedRecord1.get(".addresses..country"), "ca");
    assertEquals(flattenedRecord1.get(".addresses..street"), "second st");
    assertEquals(flattenedRecord1.get(".addresses..number"), "2");
    Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
    assertEquals(flattenedRecord2.get(".name"), "adam");
    assertEquals(flattenedRecord2.get(".age"), "20");
    assertEquals(flattenedRecord2.get(".skills.$index"), "0");
    assertEquals(flattenedRecord2.get(".skills."), "english");
    Map<String, String> flattenedRecord3 = flattenedRecords.get(3);
    assertEquals(flattenedRecord3.get(".name"), "adam");
    assertEquals(flattenedRecord3.get(".age"), "20");
    assertEquals(flattenedRecord3.get(".skills.$index"), "1");
    assertEquals(flattenedRecord3.get(".skills."), "programming");
  }

  @Test
  public void testFlattenIncludePaths()
      throws IOException {
    JsonNode jsonNode = JsonUtils.stringToJsonNode(
        "{\"name\":\"charles\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"types\":[\"home\","
            + "\"office\"]}," + "{\"country\":\"ca\",\"street\":\"second st\"}]}");

    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setIncludePaths(Collections.singleton("$.name"));
    List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 1);
    assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "charles"));

    jsonIndexConfig.setIncludePaths(Collections.singleton("$.addresses"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 3);
    Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
    assertEquals(flattenedRecord0.size(), 5);
    assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..country"), "us");
    assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
    assertEquals(flattenedRecord0.get(".addresses..types.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..types."), "home");
    Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
    assertEquals(flattenedRecord1.size(), 5);
    assertEquals(flattenedRecord1.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord1.get(".addresses..country"), "us");
    assertEquals(flattenedRecord1.get(".addresses..street"), "main st");
    assertEquals(flattenedRecord1.get(".addresses..types.$index"), "1");
    assertEquals(flattenedRecord1.get(".addresses..types."), "office");
    Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
    assertEquals(flattenedRecord2.size(), 3);
    assertEquals(flattenedRecord2.get(".addresses.$index"), "1");
    assertEquals(flattenedRecord2.get(".addresses..country"), "ca");
    assertEquals(flattenedRecord2.get(".addresses..street"), "second st");

    jsonIndexConfig.setIncludePaths(Collections.singleton("$.addresses[*]"));
    assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

    jsonIndexConfig.setIncludePaths(Collections.singleton("$.addresses[*].types"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 2);
    flattenedRecord0 = flattenedRecords.get(0);
    assertEquals(flattenedRecord0.size(), 3);
    assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..types.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..types."), "home");
    flattenedRecord1 = flattenedRecords.get(1);
    assertEquals(flattenedRecord1.size(), 3);
    assertEquals(flattenedRecord1.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord1.get(".addresses..types.$index"), "1");
    assertEquals(flattenedRecord1.get(".addresses..types."), "office");

    jsonIndexConfig.setIncludePaths(ImmutableSet.of("$.name", "$.addresses[*].types"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 2);
    flattenedRecord0 = flattenedRecords.get(0);
    assertEquals(flattenedRecord0.size(), 4);
    assertEquals(flattenedRecord0.get(".name"), "charles");
    assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..types.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..types."), "home");
    flattenedRecord1 = flattenedRecords.get(1);
    assertEquals(flattenedRecord1.size(), 4);
    assertEquals(flattenedRecord1.get(".name"), "charles");
    assertEquals(flattenedRecord1.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord1.get(".addresses..types.$index"), "1");
    assertEquals(flattenedRecord1.get(".addresses..types."), "office");

    jsonIndexConfig.setIncludePaths(Collections.singleton("$.no_match"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertTrue(flattenedRecords.isEmpty());
  }

  @Test
  public void testFlattenExclude()
      throws IOException {
    JsonNode jsonNode = JsonUtils.stringToJsonNode(
        "{\"name\":\"charles\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"types\":[\"home\","
            + "\"office\"]}," + "{\"country\":\"ca\",\"street\":\"second st\"}]}");

    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setExcludeArray(true);
    List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 1);
    assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "charles"));

    jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setExcludePaths(Collections.singleton("$.name"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 3);
    Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
    assertEquals(flattenedRecord0.size(), 5);
    assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..country"), "us");
    assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
    assertEquals(flattenedRecord0.get(".addresses..types.$index"), "0");
    assertEquals(flattenedRecord0.get(".addresses..types."), "home");
    Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
    assertEquals(flattenedRecord1.size(), 5);
    assertEquals(flattenedRecord1.get(".addresses.$index"), "0");
    assertEquals(flattenedRecord1.get(".addresses..country"), "us");
    assertEquals(flattenedRecord1.get(".addresses..street"), "main st");
    assertEquals(flattenedRecord1.get(".addresses..types.$index"), "1");
    assertEquals(flattenedRecord1.get(".addresses..types."), "office");
    Map<String, String> flattenedRecord2 = flattenedRecords.get(2);
    assertEquals(flattenedRecord2.size(), 3);
    assertEquals(flattenedRecord2.get(".addresses.$index"), "1");
    assertEquals(flattenedRecord2.get(".addresses..country"), "ca");
    assertEquals(flattenedRecord2.get(".addresses..street"), "second st");

    jsonIndexConfig.setExcludePaths(Collections.singleton("$.addresses"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 1);
    assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "charles"));

    jsonIndexConfig.setExcludePaths(Collections.singleton("$.addresses[*]"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 1);
    assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "charles"));

    jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setExcludeFields(Collections.singleton("addresses"));
    flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertEquals(flattenedRecords.size(), 1);
    assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "charles"));
  }

  @Test
  public void testUnrecognizedJsonProperties()
      throws Exception {
    String inputJsonMissingProp = "{\"primitiveIntegerField\": 123, \"missingProp\": 567,"
        + " \"missingObjectProp\": {\"somestuff\": \"data\", \"somemorestuff\":\"moredata\"},"
        + "  \"classField\": {\"internalIntField\": 12, \"internalMissingField\": \"somedata\"}}";
    Pair<JsonUtilsTestSamplePojo, Map<String, Object>> parsedResp =
        JsonUtils.stringToObjectAndUnrecognizedProperties(inputJsonMissingProp, JsonUtilsTestSamplePojo.class);

    assertTrue(parsedResp.getRight().containsKey("/missingProp"));
    assertTrue(parsedResp.getRight().containsKey("/missingObjectProp/somestuff"));
    assertTrue(parsedResp.getRight().containsKey("/missingObjectProp/somemorestuff"));
    assertTrue(parsedResp.getRight().containsKey("/classField/internalMissingField"));
  }

  @Test
  public void testInferSchema()
      throws Exception {
    ClassLoader classLoader = JsonUtilsTest.class.getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource(JSON_FILE)).getFile());
    Map<String, FieldSpec.FieldType> fieldSpecMap =
        Map.of("d1", FieldSpec.FieldType.DIMENSION, "hoursSinceEpoch", FieldSpec.FieldType.DATE_TIME, "m1",
            FieldSpec.FieldType.METRIC);
    Schema inferredPinotSchema =
        JsonUtils.getPinotSchemaFromJsonFile(file, fieldSpecMap, TimeUnit.HOURS, new ArrayList<>(), ".",
            ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    Schema expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addMetric("m1", FieldSpec.DataType.INT)
        .addSingleValueDimension("tuple.address.streetaddress", FieldSpec.DataType.STRING)
        .addSingleValueDimension("tuple.address.city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("entries", FieldSpec.DataType.STRING)
        .addMultiValueDimension("d2", FieldSpec.DataType.INT)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "EPOCH|HOURS", "1:HOURS").build();
    Assert.assertEquals(inferredPinotSchema, expectedSchema);

    // unnest collection entries
    inferredPinotSchema =
        JsonUtils.getPinotSchemaFromJsonFile(file, fieldSpecMap, TimeUnit.HOURS, Collections.singletonList("entries"),
            ".", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addMetric("m1", FieldSpec.DataType.INT)
        .addSingleValueDimension("tuple.address.streetaddress", FieldSpec.DataType.STRING)
        .addSingleValueDimension("tuple.address.city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("entries.id", FieldSpec.DataType.INT)
        .addSingleValueDimension("entries.description", FieldSpec.DataType.STRING)
        .addMultiValueDimension("d2", FieldSpec.DataType.INT)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "EPOCH|HOURS", "1:HOURS").build();
    Assert.assertEquals(inferredPinotSchema, expectedSchema);

    // change delimiter
    inferredPinotSchema =
        JsonUtils.getPinotSchemaFromJsonFile(file, fieldSpecMap, TimeUnit.HOURS, Collections.singletonList(""), "_",
            ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addMetric("m1", FieldSpec.DataType.INT)
        .addSingleValueDimension("tuple_address_streetaddress", FieldSpec.DataType.STRING)
        .addSingleValueDimension("tuple_address_city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("entries", FieldSpec.DataType.STRING)
        .addMultiValueDimension("d2", FieldSpec.DataType.INT)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "EPOCH|HOURS", "1:HOURS").build();
    Assert.assertEquals(inferredPinotSchema, expectedSchema);

    // change the handling of collection-to-json option, d2 will become string
    inferredPinotSchema =
        JsonUtils.getPinotSchemaFromJsonFile(file, fieldSpecMap, TimeUnit.HOURS, Collections.singletonList("entries"),
            ".", ComplexTypeConfig.CollectionNotUnnestedToJson.ALL);
    expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addMetric("m1", FieldSpec.DataType.INT)
        .addSingleValueDimension("tuple.address.streetaddress", FieldSpec.DataType.STRING)
        .addSingleValueDimension("tuple.address.city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("entries.id", FieldSpec.DataType.INT)
        .addSingleValueDimension("entries.description", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d2", FieldSpec.DataType.STRING)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "EPOCH|HOURS", "1:HOURS").build();
    Assert.assertEquals(inferredPinotSchema, expectedSchema);
  }

  @Test
  public void testEmptyString()
      throws IOException {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    JsonNode jsonNode = JsonUtils.stringToJsonNode("");
    List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
    assertTrue(flattenedRecords.isEmpty());
  }

  @Test
  public void testFlattenWithIndexPaths()
      throws IOException {
    {
      /* input json
      [
        {
          "country": "us",
          "street": "main st",
          "number": 1
        },
        {
          "country": "ca",
          "street": "second st"
          "number": 2
        }
      ]
       */
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},{\"country\":\"ca\",\"street\":\"second st\","
              + "\"number\":2}]");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      // flatten everything within 2 layers
      jsonIndexConfig.setIndexPaths(Collections.singleton("*.*"));
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      // flatten "a." prefix till 2 layers
      jsonIndexConfig.setIndexPaths(Collections.singleton("a.*"));
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertTrue(flattenedRecords.isEmpty());
    }
    {
      /* input json
      {
        "name": "adam",
        "addresses": [
          {
            "country": "us",
            "street": "main st",
            "number": 1
          },
          {
            "country": "ca",
            "street": "second st"
            "number": 2
          }
        ]
      }
       */
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"adam\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},"
              + "{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}]}");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      // flatten everything
      jsonIndexConfig.setIndexPaths(Collections.singleton("**"));
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      // flatten everything within 3 layers
      jsonIndexConfig.setIndexPaths(Collections.singleton("*.*.*"));
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      // flatten "name" 1 layer and "addresses" infinite layers
      jsonIndexConfig.setIndexPaths(ImmutableSet.of("name", "addresses.**"));
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      // flatten everything within 2 layers
      jsonIndexConfig.setIndexPaths(Collections.singleton("*.*"));
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 1);
      assertEquals(flattenedRecords.get(0), Collections.singletonMap(".name", "adam"));

      // flatten "name." prefix with infinite layers
      jsonIndexConfig.setIndexPaths(Collections.singleton("name.**"));
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);
    }
    {
      /* input json
      {
        "name": "charles",
        "addresses": [
          {
            "country": "us",
            "street": "main st",
            "types": ["home", "office"]
          },
          {
            "country": "ca",
            "street": "second st"
          }
        ]
      }
       */
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"charles\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"types\":[\"home\","
              + "\"office\"]}," + "{\"country\":\"ca\",\"street\":\"second st\"}]}");
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);

      // flatten everything
      jsonIndexConfig.setIndexPaths(Collections.singleton("*.*.**"));
      assertEquals(JsonUtils.flatten(jsonNode, jsonIndexConfig), flattenedRecords);

      // flatten addresses array with one more layer
      jsonIndexConfig.setIndexPaths(Collections.singleton("addresses..*"));
      flattenedRecords = JsonUtils.flatten(jsonNode, jsonIndexConfig);
      assertEquals(flattenedRecords.size(), 2);
      Map<String, String> flattenedRecord0 = flattenedRecords.get(0);
      assertEquals(flattenedRecord0.size(), 3);
      assertEquals(flattenedRecord0.get(".addresses.$index"), "0");
      assertEquals(flattenedRecord0.get(".addresses..country"), "us");
      assertEquals(flattenedRecord0.get(".addresses..street"), "main st");
      Map<String, String> flattenedRecord1 = flattenedRecords.get(1);
      assertEquals(flattenedRecord1.size(), 3);
      assertEquals(flattenedRecord1.get(".addresses.$index"), "1");
      assertEquals(flattenedRecord1.get(".addresses..country"), "ca");
      assertEquals(flattenedRecord1.get(".addresses..street"), "second st");
    }
  }

  // ==================== Tests for bytesToMap optimization ====================
  // These tests verify that bytesToMap produces identical results to the old
  // two-step approach (bytesToJsonNode + jsonNodeToMap)

  /**
   * Data provider for various JSON payloads to test bytesToMap equivalence
   */
  @DataProvider(name = "jsonPayloads")
  public Object[][] jsonPayloads() {
    return new Object[][]{
        // Empty and null cases
        {"{}"},
        {"{\"key\":null}"},

        // Simple primitives
        {"{\"string\":\"value\"}"},
        {"{\"integer\":42}"},
        {"{\"negative\":-123}"},
        {"{\"float\":3.14159}"},
        {"{\"negative_float\":-2.718}"},
        {"{\"boolean_true\":true}"},
        {"{\"boolean_false\":false}"},
        {"{\"zero\":0}"},
        {"{\"zero_float\":0.0}"},

        // Large numbers
        {"{\"large_int\":9223372036854775807}"},
        {"{\"large_negative\":-9223372036854775808}"},
        {"{\"scientific\":1.23e10}"},
        {"{\"scientific_negative\":1.23e-10}"},

        // String edge cases
        {"{\"empty_string\":\"\"}"},
        {"{\"whitespace\":\"   \"}"},
        {"{\"with_quotes\":\"she said \\\"hello\\\"\"}"},
        {"{\"with_backslash\":\"path\\\\to\\\\file\"}"},
        {"{\"with_newline\":\"line1\\nline2\"}"},
        {"{\"with_tab\":\"col1\\tcol2\"}"},
        {"{\"with_unicode\":\"日本語\"}"},
        {"{\"emoji\":\"Hello 👋 World 🌍\"}"},
        {"{\"special_chars\":\"<>&'\\\"\"}"},

        // Arrays
        {"{\"empty_array\":[]}"},
        {"{\"int_array\":[1,2,3,4,5]}"},
        {"{\"string_array\":[\"a\",\"b\",\"c\"]}"},
        {"{\"mixed_array\":[1,\"two\",3.0,true,null]}"},
        {"{\"nested_array\":[[1,2],[3,4],[5,6]]}"},
        {"{\"array_of_objects\":[{\"a\":1},{\"b\":2}]}"},

        // Nested objects
        {"{\"nested\":{\"level1\":{\"level2\":{\"level3\":\"deep\"}}}}"},
        {"{\"person\":{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}}}"},

        // Complex realistic payloads
        {
            "{\"event\":{\"id\":123,\"type\":\"click\",\"timestamp\":1609459200000,"
                + "\"user\":{\"id\":\"user123\",\"session\":\"sess456\"},"
                + "\"data\":{\"page\":\"/products\",\"element\":\"button\",\"coordinates\":{\"x\":100,\"y\":200}}}}"
        },

        // Multiple fields with same type
        {"{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5,\"f\":6,\"g\":7,\"h\":8,\"i\":9,\"j\":10}"},

        // Field names with special characters
        {"{\"field-with-dash\":1}"},
        {"{\"field.with.dots\":2}"},
        {"{\"field_with_underscore\":3}"},
        {"{\"123numeric_start\":4}"},

        // Boolean and null combinations
        {"{\"flags\":{\"active\":true,\"deleted\":false,\"pending\":null}}"},

        // Array with nulls
        {"{\"array_with_nulls\":[1,null,3,null,5]}"},

        // Unicode field names
        {"{\"日本語キー\":\"value\"}"},

        // Very long string value
        {"{\"long_string\":\"" + "a".repeat(1000) + "\"}"},

        // Deeply nested structure
        {"{\"l1\":{\"l2\":{\"l3\":{\"l4\":{\"l5\":{\"value\":\"deep\"}}}}}}"},

        // Real-world like Kafka message
        {
            "{\"topic\":\"events\",\"partition\":0,\"offset\":12345,\"timestamp\":1609459200000,"
                + "\"key\":\"user123\",\"value\":{\"event_type\":\"purchase\","
                + "\"items\":[{\"sku\":\"ABC123\",\"qty\":2,\"price\":29.99},"
                + "{\"sku\":\"XYZ789\",\"qty\":1,\"price\":49.99}],"
                + "\"total\":109.97,\"currency\":\"USD\"}}"
        }
    };
  }

  /**
   * Test that bytesToMap produces identical results to bytesToJsonNode + jsonNodeToMap
   * for all payload types
   */
  @Test(dataProvider = "jsonPayloads")
  public void testBytesToMapEquivalence(String jsonString)
      throws IOException {
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    // Old approach: bytesToJsonNode + jsonNodeToMap
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes, 0, jsonBytes.length);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);

    // New approach: bytesToMap directly
    Map<String, Object> newResult = JsonUtils.bytesToMap(jsonBytes, 0, jsonBytes.length);

    // Verify they are equal
    assertEquals(newResult, oldResult,
        "bytesToMap should produce identical result to bytesToJsonNode + jsonNodeToMap for: " + jsonString);
  }

  /**
   * Test bytesToMap with offset and length parameters (partial array reading)
   */
  @Test
  public void testBytesToMapWithOffset()
      throws IOException {
    String prefix = "GARBAGE";
    String jsonString = "{\"key\":\"value\",\"num\":42}";
    String suffix = "MORE_GARBAGE";
    String combined = prefix + jsonString + suffix;

    byte[] fullBytes = combined.getBytes(StandardCharsets.UTF_8);
    int offset = prefix.length();
    int length = jsonString.length();

    // Old approach
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(fullBytes, offset, length);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);

    // New approach
    Map<String, Object> newResult = JsonUtils.bytesToMap(fullBytes, offset, length);

    assertEquals(newResult, oldResult);
    assertEquals(newResult.get("key"), "value");
    assertEquals(newResult.get("num"), 42);
  }

  /**
   * Test bytesToMap without offset (full array)
   */
  @Test
  public void testBytesToMapFullArray()
      throws IOException {
    String jsonString = "{\"name\":\"test\",\"values\":[1,2,3]}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    // Old approach using full array methods
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);

    // New approach
    Map<String, Object> newResult = JsonUtils.bytesToMap(jsonBytes);

    assertEquals(newResult, oldResult);
  }

  /**
   * Test that numeric types are preserved correctly
   */
  @Test
  public void testBytesToMapNumericTypes()
      throws IOException {
    String jsonString = "{\"int\":42,\"long\":9223372036854775807,\"double\":3.14159,\"float\":1.5}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    // Verify the types and values
    assertEquals(result.get("int"), 42);
    assertEquals(result.get("long"), 9223372036854775807L);
    assertEquals(result.get("double"), 3.14159);
    assertEquals(result.get("float"), 1.5);

    // Also verify equivalence with old approach
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test nested object access after parsing
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testBytesToMapNestedAccess()
      throws IOException {
    String jsonString = "{\"outer\":{\"inner\":{\"value\":\"deep\"},\"array\":[1,2,3]}}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);
    Map<String, Object> outer = (Map<String, Object>) result.get("outer");
    Map<String, Object> inner = (Map<String, Object>) outer.get("inner");
    List<Object> array = (List<Object>) outer.get("array");

    assertEquals(inner.get("value"), "deep");
    assertEquals(array, Arrays.asList(1, 2, 3));

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test null handling in bytesToMap
   */
  @Test
  public void testBytesToMapNullHandling()
      throws IOException {
    String jsonString = "{\"nullField\":null,\"nested\":{\"alsoNull\":null}}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    assertTrue(result.containsKey("nullField"));
    assertNull(result.get("nullField"));

    @SuppressWarnings("unchecked")
    Map<String, Object> nested = (Map<String, Object>) result.get("nested");
    assertTrue(nested.containsKey("alsoNull"));
    assertNull(nested.get("alsoNull"));

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test boolean handling
   */
  @Test
  public void testBytesToMapBooleanHandling()
      throws IOException {
    String jsonString = "{\"trueVal\":true,\"falseVal\":false}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    assertEquals(result.get("trueVal"), true);
    assertEquals(result.get("falseVal"), false);

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test empty object and array
   */
  @Test
  public void testBytesToMapEmptyStructures()
      throws IOException {
    String jsonString = "{\"emptyObj\":{},\"emptyArr\":[]}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    @SuppressWarnings("unchecked")
    Map<String, Object> emptyObj = (Map<String, Object>) result.get("emptyObj");
    assertTrue(emptyObj.isEmpty());

    @SuppressWarnings("unchecked")
    List<Object> emptyArr = (List<Object>) result.get("emptyArr");
    assertTrue(emptyArr.isEmpty());

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test that the method correctly handles UTF-8 encoded bytes
   */
  @Test
  public void testBytesToMapUtf8Encoding()
      throws IOException {
    // Test various UTF-8 characters
    String jsonString = "{\"chinese\":\"中文\",\"japanese\":\"日本語\",\"korean\":\"한국어\","
        + "\"emoji\":\"🎉🎊🎁\",\"mixed\":\"Hello世界\"}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    assertEquals(result.get("chinese"), "中文");
    assertEquals(result.get("japanese"), "日本語");
    assertEquals(result.get("korean"), "한국어");
    assertEquals(result.get("emoji"), "🎉🎊🎁");
    assertEquals(result.get("mixed"), "Hello世界");

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test escape sequences in strings
   */
  @Test
  public void testBytesToMapEscapeSequences()
      throws IOException {
    String jsonString = "{\"quotes\":\"\\\"quoted\\\"\",\"backslash\":\"a\\\\b\","
        + "\"newline\":\"line1\\nline2\",\"tab\":\"col1\\tcol2\","
        + "\"unicode\":\"\\u0048\\u0065\\u006C\\u006C\\u006F\"}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    assertEquals(result.get("quotes"), "\"quoted\"");
    assertEquals(result.get("backslash"), "a\\b");
    assertEquals(result.get("newline"), "line1\nline2");
    assertEquals(result.get("tab"), "col1\tcol2");
    assertEquals(result.get("unicode"), "Hello");

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test mixed type arrays
   */
  @Test
  public void testBytesToMapMixedArrays()
      throws IOException {
    String jsonString = "{\"mixed\":[1,\"two\",3.0,true,null,{\"nested\":\"obj\"},[1,2]]}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    @SuppressWarnings("unchecked")
    List<Object> mixed = (List<Object>) result.get("mixed");
    assertEquals(mixed.size(), 7);
    assertEquals(mixed.get(0), 1);
    assertEquals(mixed.get(1), "two");
    assertEquals(mixed.get(2), 3.0);
    assertEquals(mixed.get(3), true);
    assertNull(mixed.get(4));

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }

  /**
   * Test error handling - malformed JSON should throw IOException
   */
  @Test(expectedExceptions = IOException.class)
  public void testBytesToMapMalformedJson()
      throws IOException {
    String malformedJson = "{\"key\":value}"; // missing quotes around value
    byte[] jsonBytes = malformedJson.getBytes(StandardCharsets.UTF_8);
    JsonUtils.bytesToMap(jsonBytes);
  }

  /**
   * Test error handling - incomplete JSON
   */
  @Test(expectedExceptions = IOException.class)
  public void testBytesToMapIncompleteJson()
      throws IOException {
    String incompleteJson = "{\"key\":\"value\""; // missing closing brace
    byte[] jsonBytes = incompleteJson.getBytes(StandardCharsets.UTF_8);
    JsonUtils.bytesToMap(jsonBytes);
  }

  /**
   * Stress test with a large number of fields
   */
  @Test
  public void testBytesToMapManyFields()
      throws IOException {
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("\"field").append(i).append("\":").append(i);
    }
    sb.append("}");
    String jsonString = sb.toString();
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    // Old approach
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);

    // New approach
    Map<String, Object> newResult = JsonUtils.bytesToMap(jsonBytes);

    assertEquals(newResult, oldResult);
    assertEquals(newResult.size(), 100);

    for (int i = 0; i < 100; i++) {
      assertEquals(newResult.get("field" + i), i);
    }
  }

  /**
   * Test with a deeply nested array structure
   */
  @Test
  public void testBytesToMapDeeplyNestedArrays()
      throws IOException {
    String jsonString = "{\"data\":[[[[[\"deep\"]]]]]}";
    byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);

    Map<String, Object> result = JsonUtils.bytesToMap(jsonBytes);

    // Verify equivalence
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(jsonBytes);
    Map<String, Object> oldResult = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(result, oldResult);
  }
}
