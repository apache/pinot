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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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
        ImmutableMap.of("d1", FieldSpec.FieldType.DIMENSION, "hoursSinceEpoch", FieldSpec.FieldType.DATE_TIME, "m1",
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
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS").build();
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
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS").build();
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
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS").build();
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
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS").build();
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
}
