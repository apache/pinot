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
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/// Slim-serialization contract for [RangeIndexConfig].
public class RangeIndexConfigSerializationTest {

  @Test
  public void testRoundTripAllNull()
      throws Exception {
    RangeIndexConfig config = new RangeIndexConfig((Boolean) null, null);

    assertOnlyKeys(serializeToNode(config));
    assertRoundTripIdempotent(config);

    assertEquals(config.getVersion(), RangeIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void testRoundTripPartial()
      throws Exception {
    RangeIndexConfig config = new RangeIndexConfig(3);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "version");
    assertEquals(node.get("version").asInt(), 3);
    assertRoundTripIdempotent(config);

    assertEquals(config.getVersion(), 3);
  }

  @Test
  public void testRoundTripAllSet()
      throws Exception {
    RangeIndexConfig config = new RangeIndexConfig(true, 5);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "disabled", "version");
    assertTrue(node.get("disabled").asBoolean());
    assertEquals(node.get("version").asInt(), 5);
    assertRoundTripIdempotent(config);
  }

  @Test
  public void testDefaultConstantSerializesEmpty()
      throws Exception {
    assertOnlyKeys(serializeToNode(RangeIndexConfig.DEFAULT));
  }

  @Test
  public void testDisabledConstantOnlyEmitsDisabled()
      throws Exception {
    JsonNode node = serializeToNode(RangeIndexConfig.DISABLED);
    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testExplicitDefaultVersionPreserved()
      throws Exception {
    // User says version=2 (today's default). Slim form keeps the key so a future default change
    // does not silently swallow user intent.
    RangeIndexConfig config = new RangeIndexConfig(RangeIndexConfig.DEFAULT_VERSION);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "version");
    assertEquals(node.get("version").asInt(), RangeIndexConfig.DEFAULT_VERSION);

    assertNotEquals(config, RangeIndexConfig.DEFAULT,
        "Explicit-default-value config must NOT equal the all-null DEFAULT constant");
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    RangeIndexConfig config = new RangeIndexConfig(3);
    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    RangeIndexConfig config = new RangeIndexConfig(5);
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

  private static void assertRoundTripIdempotent(RangeIndexConfig original)
      throws Exception {
    JsonNode firstNode = serializeToNode(original);
    RangeIndexConfig restored =
        JsonUtils.stringToObject(JsonUtils.objectToString(original), RangeIndexConfig.class);
    JsonNode secondNode = serializeToNode(restored);
    assertEquals(secondNode, firstNode, "Slim form must be byte-equivalent across a round-trip");
  }
}
