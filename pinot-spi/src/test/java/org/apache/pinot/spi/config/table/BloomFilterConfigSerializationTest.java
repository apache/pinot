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
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Slim-serialization contract for [BloomFilterConfig] under the wrapper-typed contract:
/// fields are stored raw (null = not configured), runtime getters apply defaults, the slim form
/// emits only fields that were explicitly configured.
public class BloomFilterConfigSerializationTest {

  // ---- Round-trip matrix ----

  @Test
  public void testRoundTripAllNull()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(null, null, null, null);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node);
    assertRoundTripIdempotent(config);

    // Getters fall back to documented defaults.
    assertEquals(config.getFpp(), BloomFilterConfig.DEFAULT_FPP);
    assertEquals(config.getMaxSizeInBytes(), 0);
    assertEquals(config.isLoadOnHeap(), false);
  }

  @Test
  public void testRoundTripPartial()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(null, 0.01, null, null);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "fpp");
    assertEquals(node.get("fpp").asDouble(), 0.01);
    assertRoundTripIdempotent(config);

    // Explicit field returns its value; null fields fall back.
    assertEquals(config.getFpp(), 0.01);
    assertEquals(config.getMaxSizeInBytes(), 0);
    assertEquals(config.isLoadOnHeap(), false);
  }

  @Test
  public void testRoundTripAllSet()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(null, 0.02, 2048, true);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "fpp", "maxSizeInBytes", "loadOnHeap");
    assertEquals(node.get("fpp").asDouble(), 0.02);
    assertEquals(node.get("maxSizeInBytes").asInt(), 2048);
    assertTrue(node.get("loadOnHeap").asBoolean());
    assertRoundTripIdempotent(config);

    assertEquals(config.getFpp(), 0.02);
    assertEquals(config.getMaxSizeInBytes(), 2048);
    assertEquals(config.isLoadOnHeap(), true);
  }

  // ---- Constants ----

  @Test
  public void testDefaultConstantSerializesEmpty()
      throws Exception {
    assertOnlyKeys(serializeToNode(BloomFilterConfig.DEFAULT));
    assertRoundTripIdempotent(BloomFilterConfig.DEFAULT);
  }

  @Test
  public void testDisabledConstantOnlyEmitsDisabled()
      throws Exception {
    JsonNode node = serializeToNode(BloomFilterConfig.DISABLED);
    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
    assertRoundTripIdempotent(BloomFilterConfig.DISABLED);
  }

  // ---- The load-bearing distinction ----

  @Test
  public void testExplicitDefaultValueIsPreserved()
      throws Exception {
    // User sets fpp explicitly to today's default (0.05). Slim form keeps the key so a future
    // default change does not silently swallow the user's intent.
    BloomFilterConfig config = new BloomFilterConfig(null, BloomFilterConfig.DEFAULT_FPP, null, null);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "fpp");
    assertEquals(node.get("fpp").asDouble(), BloomFilterConfig.DEFAULT_FPP);

    assertNotEquals(config, BloomFilterConfig.DEFAULT,
        "Explicit-default-value config must not equal the all-null DEFAULT constant");
  }

  @Test
  public void testFppZeroInputPreservedAsExplicit()
      throws Exception {
    // The historical sentinel `fpp == 0.0` was previously coerced to DEFAULT_FPP. With wrappers we
    // keep the raw input so the slim form preserves it; the runtime getter still returns
    // DEFAULT_FPP for the sentinel.
    BloomFilterConfig config = JsonUtils.stringToObject("{\"fpp\":0.0}", BloomFilterConfig.class);
    assertEquals(config.getFpp(), BloomFilterConfig.DEFAULT_FPP);
    assertOnlyKeys(serializeToNode(config), "fpp");
  }

  // ---- Validation preserved ----

  @Test
  public void testFppValidationRejectsOutOfRange() {
    assertThrows(IllegalArgumentException.class,
        () -> new BloomFilterConfig(null, 2.0, null, null));
    assertThrows(IllegalArgumentException.class,
        () -> new BloomFilterConfig(null, -0.5, null, null));
  }

  @Test
  public void testFppNullSkipsValidation() {
    // null bypasses validation — required so absent JSON keys don't throw.
    new BloomFilterConfig(null, null, null, null);
  }

  // ---- Helpers ----

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(null, 0.02, 2048, true);
    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(null, 0.02, null, null);
    String json = new ObjectMapper().writeValueAsString(config);
    assertEquals(JsonUtils.stringToJsonNode(json), config.toJsonObject());
  }

  private static JsonNode serializeToNode(Object pojo)
      throws Exception {
    return JsonUtils.stringToJsonNode(JsonUtils.objectToString(pojo));
  }

  private static void assertOnlyKeys(JsonNode node, String... expectedKeys) {
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    assertEquals(actual, Set.of(expectedKeys), "Unexpected key set: " + node);
  }

  private static void assertRoundTripIdempotent(BloomFilterConfig original)
      throws Exception {
    JsonNode firstNode = serializeToNode(original);
    BloomFilterConfig restored = JsonUtils.stringToObject(JsonUtils.objectToString(original), BloomFilterConfig.class);
    JsonNode secondNode = serializeToNode(restored);
    assertEquals(secondNode, firstNode, "Slim form must be byte-equivalent across a round-trip");
  }
}
