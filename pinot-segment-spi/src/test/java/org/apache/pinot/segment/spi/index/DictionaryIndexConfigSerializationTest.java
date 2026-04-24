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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.spi.config.table.Intern;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link DictionaryIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each field only when it differs from its
 * class default.
 */
public class DictionaryIndexConfigSerializationTest {

  @Test
  public void testDefaultPojoSerializesToEmptyJson()
      throws Exception {
    JsonNode node = serializeToNode(DictionaryIndexConfig.DEFAULT);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    JsonNode node = serializeToNode(DictionaryIndexConfig.DISABLED);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testOnHeapEmittedWhenTrue()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, false);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "onHeap");
  }

  @Test
  public void testUseVarLengthDictionaryEmittedWhenTrue()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(false, true);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "useVarLengthDictionary");
  }

  @Test
  public void testInternEmittedWhenSet()
      throws Exception {
    // Intern requires onHeap=true.
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, false, new Intern(1024));

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "onHeap", "intern");
    assertEquals(node.get("intern").get("capacity").asInt(), 1024);
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{"
        + "\"disabled\":false,"
        + "\"onHeap\":false,"
        + "\"useVarLengthDictionary\":false,"
        + "\"intern\":null"
        + "}";

    DictionaryIndexConfig config = JsonUtils.stringToObject(fat, DictionaryIndexConfig.class);

    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, true);

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, false);

    String json = new ObjectMapper().writeValueAsString(config);

    assertEquals(JsonUtils.stringToJsonNode(json), config.toJsonObject());
  }

  // ---- Helpers ----

  private static JsonNode serializeToNode(Object pojo)
      throws Exception {
    return JsonUtils.stringToJsonNode(JsonUtils.objectToString(pojo));
  }

  private static void assertOnlyKeys(JsonNode node, String... expectedKeys) {
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    assertEquals(actual, Set.of(expectedKeys), "Unexpected key set: " + node);
  }
}
