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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link MultiColumnTextIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method. {@code columns} is required and always emitted;
 * the optional maps appear only when non-null.
 */
public class MultiColumnTextIndexConfigSerializationTest {

  @Test
  public void testColumnsOnlyIsMinimal()
      throws Exception {
    MultiColumnTextIndexConfig config = new MultiColumnTextIndexConfig(List.of("a", "b"));

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "columns");
    assertEquals(node.get("columns").size(), 2);
  }

  @Test
  public void testPropertiesEmittedWhenNonNull()
      throws Exception {
    MultiColumnTextIndexConfig config = new MultiColumnTextIndexConfig(
        List.of("a"), Map.of("k", "v"), null);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "columns", "properties");
    assertEquals(node.get("properties").get("k").asText(), "v");
  }

  @Test
  public void testPerColumnPropertiesEmittedWhenNonNull()
      throws Exception {
    MultiColumnTextIndexConfig config = new MultiColumnTextIndexConfig(
        List.of("a"), null, Map.of("a", Map.of("k", "v")));

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "columns", "perColumnProperties");
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{\"columns\":[\"a\"],\"properties\":null,\"perColumnProperties\":null}";

    MultiColumnTextIndexConfig config = JsonUtils.stringToObject(fat, MultiColumnTextIndexConfig.class);

    assertOnlyKeys(serializeToNode(config), "columns");
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    MultiColumnTextIndexConfig config = new MultiColumnTextIndexConfig(
        List.of("a"), Map.of("k", "v"), null);

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    MultiColumnTextIndexConfig config = new MultiColumnTextIndexConfig(List.of("a"));

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
    assertTrue(node.has("columns"), "columns is required and must always be present: " + node);
  }
}
