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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class JsonUtilsTest {

  @Test
  public void testFlatten()
      throws IOException {
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"adam\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode);
      assertEquals(flattenedRecords.size(), 2);
      for (Map<String, String> flattenedRecord : flattenedRecords) {
        assertEquals(flattenedRecord.size(), 5);
        assertEquals(flattenedRecord.get("name"), "adam");
        assertTrue(flattenedRecord.containsKey("addresses.$index"));
        assertTrue(flattenedRecord.containsKey("addresses.country"));
        assertTrue(flattenedRecord.containsKey("addresses.street"));
        assertTrue(flattenedRecord.containsKey("addresses.number"));
      }
      Map<String, String> firstFlattenedRecord = flattenedRecords.get(0);
      assertEquals(firstFlattenedRecord.get("addresses.$index"), "0");
      assertEquals(firstFlattenedRecord.get("addresses.country"), "us");
      assertEquals(firstFlattenedRecord.get("addresses.street"), "main st");
      assertEquals(firstFlattenedRecord.get("addresses.number"), "1");
      Map<String, String> secondFlattenedRecord = flattenedRecords.get(1);
      assertEquals(secondFlattenedRecord.get("addresses.$index"), "1");
      assertEquals(secondFlattenedRecord.get("addresses.country"), "ca");
      assertEquals(secondFlattenedRecord.get("addresses.street"), "second st");
      assertEquals(secondFlattenedRecord.get("addresses.number"), "2");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"adam\",\"age\":20,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"number\":1},{\"country\":\"ca\",\"street\":\"second st\",\"number\":2}],\"skills\":[\"english\",\"programming\"]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode);
      assertEquals(flattenedRecords.size(), 4);
      for (Map<String, String> flattenedRecord : flattenedRecords) {
        assertEquals(flattenedRecord.size(), 8);
        assertEquals(flattenedRecord.get("name"), "adam");
        assertEquals(flattenedRecord.get("age"), "20");
        assertTrue(flattenedRecord.containsKey("addresses.$index"));
        assertTrue(flattenedRecord.containsKey("addresses.country"));
        assertTrue(flattenedRecord.containsKey("addresses.street"));
        assertTrue(flattenedRecord.containsKey("addresses.number"));
        assertTrue(flattenedRecord.containsKey("skills.$index"));
        assertTrue(flattenedRecord.containsKey("skills"));
      }
      Map<String, String> firstFlattenedRecord = flattenedRecords.get(0);
      assertEquals(firstFlattenedRecord.get("addresses.$index"), "0");
      assertEquals(firstFlattenedRecord.get("addresses.country"), "us");
      assertEquals(firstFlattenedRecord.get("addresses.street"), "main st");
      assertEquals(firstFlattenedRecord.get("addresses.number"), "1");
      assertEquals(firstFlattenedRecord.get("skills.$index"), "0");
      assertEquals(firstFlattenedRecord.get("skills"), "english");
      Map<String, String> lastFlattenedRecord = flattenedRecords.get(3);
      assertEquals(lastFlattenedRecord.get("addresses.$index"), "1");
      assertEquals(lastFlattenedRecord.get("addresses.country"), "ca");
      assertEquals(lastFlattenedRecord.get("addresses.street"), "second st");
      assertEquals(lastFlattenedRecord.get("addresses.number"), "2");
      assertEquals(lastFlattenedRecord.get("skills.$index"), "1");
      assertEquals(lastFlattenedRecord.get("skills"), "programming");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"bob\",\"age\":null,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\"}],\"skills\":[],\"hobbies\":[null]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode);
      assertEquals(flattenedRecords.size(), 1);
      Map<String, String> flattenedRecord = flattenedRecords.get(0);
      assertEquals(flattenedRecord.size(), 4);
      assertEquals(flattenedRecord.get("name"), "bob");
      assertEquals(flattenedRecord.get("addresses.$index"), "0");
      assertEquals(flattenedRecord.get("addresses.country"), "us");
      assertEquals(flattenedRecord.get("addresses.street"), "main st");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"bob\",\"age\":null,\"addresses\":[{\"country\":\"us\",\"street\":\"main st\"}],\"skills\":[],\"hobbies\":[null,\"football\"]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode);
      assertEquals(flattenedRecords.size(), 1);
      Map<String, String> flattenedRecord = flattenedRecords.get(0);
      assertEquals(flattenedRecord.size(), 6);
      assertEquals(flattenedRecord.get("name"), "bob");
      assertEquals(flattenedRecord.get("addresses.$index"), "0");
      assertEquals(flattenedRecord.get("addresses.country"), "us");
      assertEquals(flattenedRecord.get("addresses.street"), "main st");
      assertEquals(flattenedRecord.get("hobbies.$index"), "1");
      assertEquals(flattenedRecord.get("hobbies"), "football");
    }
    {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(
          "{\"name\":\"charles\",\"addresses\":[{\"country\":\"us\",\"street\":\"main st\",\"types\":[\"home\",\"office\"]},{\"country\":\"ca\",\"street\":\"second st\"}]}");
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonNode);
      assertEquals(flattenedRecords.size(), 3);
      Map<String, String> firstFlattenedRecord = flattenedRecords.get(0);
      assertEquals(firstFlattenedRecord.size(), 6);
      assertEquals(firstFlattenedRecord.get("name"), "charles");
      assertEquals(firstFlattenedRecord.get("addresses.$index"), "0");
      assertEquals(firstFlattenedRecord.get("addresses.country"), "us");
      assertEquals(firstFlattenedRecord.get("addresses.street"), "main st");
      assertEquals(firstFlattenedRecord.get("addresses.types.$index"), "0");
      assertEquals(firstFlattenedRecord.get("addresses.types"), "home");
      Map<String, String> secondFlattenedRecord = flattenedRecords.get(1);
      assertEquals(secondFlattenedRecord.size(), 6);
      assertEquals(secondFlattenedRecord.get("name"), "charles");
      assertEquals(secondFlattenedRecord.get("addresses.$index"), "0");
      assertEquals(secondFlattenedRecord.get("addresses.country"), "us");
      assertEquals(secondFlattenedRecord.get("addresses.street"), "main st");
      assertEquals(secondFlattenedRecord.get("addresses.types.$index"), "1");
      assertEquals(secondFlattenedRecord.get("addresses.types"), "office");
      Map<String, String> thirdFlattenedRecord = flattenedRecords.get(2);
      assertEquals(thirdFlattenedRecord.size(), 4);
      assertEquals(thirdFlattenedRecord.get("name"), "charles");
      assertEquals(thirdFlattenedRecord.get("addresses.$index"), "1");
      assertEquals(thirdFlattenedRecord.get("addresses.country"), "ca");
      assertEquals(thirdFlattenedRecord.get("addresses.street"), "second st");
    }
  }
}
