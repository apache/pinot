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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Slim-serialization contract for [VectorIndexConfig].
public class VectorIndexConfigSerializationTest {

  @Test
  public void testRoundTripAllNull()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(null, null, null, null, null, null);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node);
    assertRoundTripIdempotent(config);

    assertEquals(config.getVectorIndexType(), null);
    assertEquals(config.getVectorDimension(), 0);
    assertEquals(config.getVersion(), 0);
    assertEquals(config.getVectorDistanceFunction(), null);
    assertNotNull(config.getProperties());
    assertTrue(config.getProperties().isEmpty());
  }

  @Test
  public void testRoundTripPartial()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(null, "HNSW", 768, null, null, null);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "vectorIndexType", "vectorDimension");
    assertEquals(node.get("vectorIndexType").asText(), "HNSW");
    assertEquals(node.get("vectorDimension").asInt(), 768);
    assertRoundTripIdempotent(config);

    assertEquals(config.getVectorIndexType(), "HNSW");
    assertEquals(config.getVectorDimension(), 768);
    assertEquals(config.getVersion(), 0);
  }

  @Test
  public void testRoundTripAllSet()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(Boolean.FALSE, "HNSW", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, Map.of("maxCon", "16"));

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "vectorIndexType", "vectorDimension", "version", "vectorDistanceFunction", "properties");
    assertEquals(node.get("vectorIndexType").asText(), "HNSW");
    assertEquals(node.get("vectorDimension").asInt(), 768);
    assertEquals(node.get("version").asInt(), 1);
    assertEquals(node.get("vectorDistanceFunction").asText(), "COSINE");
    assertEquals(node.get("properties").get("maxCon").asText(), "16");
    assertRoundTripIdempotent(config);

    assertEquals(config.getVectorIndexType(), "HNSW");
    assertEquals(config.getVectorDimension(), 768);
    assertEquals(config.getVersion(), 1);
    assertEquals(config.getVectorDistanceFunction(), VectorIndexConfig.VectorDistanceFunction.COSINE);
  }

  @Test
  public void testEnabledDefaultIsEmpty()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(false);
    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    JsonNode node = serializeToNode(VectorIndexConfig.DISABLED);
    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testExplicitDefaultPreserved()
      throws Exception {
    // User explicitly says vectorDimension=0 (matches "absent" sentinel) — wrapper preserves the
    // explicit Integer.valueOf(0). Slim form keeps the key so a future default change cannot
    // silently swallow the user's intent.
    VectorIndexConfig config = new VectorIndexConfig(null, null, 0, null, null, null);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "vectorDimension");
    assertEquals(node.get("vectorDimension").asInt(), 0);

    assertNotEquals(config, new VectorIndexConfig(false),
        "Explicit-zero config must NOT equal the all-null no-arg constructor");
  }

  @Test
  public void testEmptyPropertiesMapOmitted()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(null, null, null, null, null, Map.of());
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node);
    assertNotNull(config.getProperties());
    assertTrue(config.getProperties().isEmpty());
  }

  @Test
  public void testPropertiesNullAndEmptyMapRoundTripToNonNull()
      throws Exception {
    VectorIndexConfig fromNull = JsonUtils.stringToObject("{\"properties\":null}", VectorIndexConfig.class);
    assertNotNull(fromNull.getProperties());
    assertTrue(fromNull.getProperties().isEmpty());
    assertOnlyKeys(serializeToNode(fromNull));

    VectorIndexConfig fromEmpty = JsonUtils.stringToObject("{\"properties\":{}}", VectorIndexConfig.class);
    assertNotNull(fromEmpty.getProperties());
    assertTrue(fromEmpty.getProperties().isEmpty());
    assertOnlyKeys(serializeToNode(fromEmpty));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(Boolean.FALSE, "HNSW", 256, 1, null, null);
    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    VectorIndexConfig config = new VectorIndexConfig(Boolean.FALSE, "HNSW", 128, 1, null, null);
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

  private static void assertRoundTripIdempotent(VectorIndexConfig original)
      throws Exception {
    JsonNode firstNode = serializeToNode(original);
    VectorIndexConfig restored =
        JsonUtils.stringToObject(JsonUtils.objectToString(original), VectorIndexConfig.class);
    JsonNode secondNode = serializeToNode(restored);
    assertEquals(secondNode, firstNode, "Slim form must be byte-equivalent across a round-trip");
  }
}
