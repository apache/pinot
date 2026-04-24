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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link BloomFilterConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each field only when it differs from its
 * class default.
 *
 * <p>Calls out the {@code fpp == 0.0 → DEFAULT_FPP} ctor coercion: an explicit {@code 0.0} input
 * is indistinguishable from "absent", so the only sane slim form for the default fpp is omission.
 */
public class BloomFilterConfigSerializationTest {

  @Test
  public void testDefaultPojoSerializesToEmptyJson()
      throws Exception {
    BloomFilterConfig config = BloomFilterConfig.DEFAULT;

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    BloomFilterConfig config = BloomFilterConfig.DISABLED;

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testNonDefaultFppEmitted()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(0.01, 0, false);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "fpp");
    assertEquals(node.get("fpp").asDouble(), 0.01);
  }

  @Test
  public void testNonDefaultMaxSizeAndLoadOnHeapEmitted()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 1024, true);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "maxSizeInBytes", "loadOnHeap");
    assertEquals(node.get("maxSizeInBytes").asInt(), 1024);
    assertTrue(node.get("loadOnHeap").asBoolean());
  }

  /**
   * Documents the ctor coercion: {@code fpp == 0.0} → {@code DEFAULT_FPP}, so the slim output
   * after a fat-input parse omits {@code fpp}, and the round-tripped POJO matches the default.
   */
  @Test
  public void testFppZeroInputCoercedToDefaultAndOmittedOnSerialization()
      throws Exception {
    String fat = "{\"fpp\":0.0,\"maxSizeInBytes\":0,\"loadOnHeap\":false}";

    BloomFilterConfig config = JsonUtils.stringToObject(fat, BloomFilterConfig.class);

    assertEquals(config.getFpp(), BloomFilterConfig.DEFAULT_FPP);
    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    // Pre-fix output explicitly carried the default fpp, so verify it round-trips back to slim.
    String fat = "{\"disabled\":false,\"fpp\":0.05,\"maxSizeInBytes\":0,\"loadOnHeap\":false}";

    BloomFilterConfig config = JsonUtils.stringToObject(fat, BloomFilterConfig.class);

    assertOnlyKeys(serializeToNode(config));
    assertEquals(config, BloomFilterConfig.DEFAULT);
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(0.02, 2048, true);

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    BloomFilterConfig config = new BloomFilterConfig(0.02, 0, false);

    String json = new ObjectMapper().writeValueAsString(config);

    assertEquals(JsonUtils.stringToJsonNode(json), config.toJsonObject());
  }

  @Test
  public void testEqualPojosProduceIdenticalJson()
      throws Exception {
    BloomFilterConfig a = new BloomFilterConfig(0.01, 100, true);
    BloomFilterConfig b = new BloomFilterConfig(0.01, 100, true);

    assertEquals(a, b);
    assertEquals(serializeToNode(a), serializeToNode(b));
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
    // Defensive: ensure the resulting node doesn't accidentally carry stale "disabled" defaults.
    if (!actual.contains("disabled")) {
      assertFalse(node.has("disabled"));
    }
  }
}
