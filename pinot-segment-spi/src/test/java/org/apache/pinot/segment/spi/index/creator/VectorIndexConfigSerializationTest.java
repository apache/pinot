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
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link VectorIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each field only when it differs from its
 * class default.
 *
 * <p>{@code vectorDimension} and {@code version} are primitive {@code int} in the
 * {@code @JsonCreator}, so a value of {@code 0} is indistinguishable from absent and is
 * intentionally omitted from the slim form.
 */
public class VectorIndexConfigSerializationTest {

  @Test
  public void testEnabledDefaultIsEmpty()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(false);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    JsonNode node = serializeToNode(VectorIndexConfig.DISABLED);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testZeroDimensionAndVersionOmitted()
      throws Exception {
    // vectorDimension=0 and version=0 are treated as defaults (primitive int absent).
    VectorIndexConfig config = new VectorIndexConfig(false, null, 0, 0, null, null);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testFullySpecifiedConfigEmitsAllFields()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, Map.of("maxCon", "16"));

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "vectorIndexType", "vectorDimension", "version", "vectorDistanceFunction", "properties");
    assertEquals(node.get("vectorIndexType").asText(), "HNSW");
    assertEquals(node.get("vectorDimension").asInt(), 768);
    assertEquals(node.get("version").asInt(), 1);
    assertEquals(node.get("vectorDistanceFunction").asText(), "COSINE");
    assertEquals(node.get("properties").get("maxCon").asText(), "16");
  }

  @Test
  public void testEmptyPropertiesMapOmitted()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(false, null, 0, 0, null, Map.of());

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{"
        + "\"disabled\":false,"
        + "\"vectorIndexType\":null,"
        + "\"vectorDimension\":0,"
        + "\"version\":0,"
        + "\"vectorDistanceFunction\":null,"
        + "\"properties\":null"
        + "}";

    VectorIndexConfig config = JsonUtils.stringToObject(fat, VectorIndexConfig.class);

    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 256, 1, null, null);

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 128, 1, null, null);

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
