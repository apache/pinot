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
package org.apache.pinot.segment.spi.index.creator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link H3IndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method. The JSON key is {@code "resolution"} (singular),
 * matching the {@code @JsonProperty} on the {@code @JsonCreator} parameter.
 */
public class H3IndexConfigSerializationTest {

  @Test
  public void testDisabledIsMinimal()
      throws Exception {
    JsonNode node = serializeToNode(H3IndexConfig.DISABLED);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testResolutionEmittedWhenSet()
      throws Exception {
    H3IndexConfig config = new H3IndexConfig(new H3IndexResolution(List.of(5, 6)));

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "resolution");
    // Resolution serializes as an int list via H3IndexResolution.ToIntListConverted.
    assertEquals(node.get("resolution").size(), 2);
  }

  @Test
  public void testJsonKeyIsSingularResolution()
      throws Exception {
    H3IndexConfig config = new H3IndexConfig(new H3IndexResolution(List.of(7)));

    JsonNode node = serializeToNode(config);

    assertTrue(node.has("resolution"));
    assertTrue(!node.has("resolutions"), "Key must be singular 'resolution': " + node);
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{\"disabled\":false,\"resolution\":[5,6,7]}";

    H3IndexConfig config = JsonUtils.stringToObject(fat, H3IndexConfig.class);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "resolution");
    assertEquals(node.get("resolution").size(), 3);
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    H3IndexConfig config = new H3IndexConfig(new H3IndexResolution(List.of(5)));

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    H3IndexConfig config = new H3IndexConfig(new H3IndexResolution(List.of(5)));

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
