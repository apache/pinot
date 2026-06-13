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
import static org.testng.Assert.assertTrue;


/// Slim-serialization contract for [JsonIndexConfig].
public class JsonIndexConfigSerializationTest {

  @Test
  public void testRoundTripAllNull()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();

    assertOnlyKeys(serializeToNode(config));
    assertRoundTripIdempotent(config);

    assertEquals(config.getMaxLevels(), -1);
    assertEquals(config.isExcludeArray(), false);
    assertEquals(config.isDisableCrossArrayUnnest(), false);
    assertEquals(config.getMaxValueLength(), 0);
    assertEquals(config.getSkipInvalidJson(), false);
  }

  @Test
  public void testRoundTripPartial()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(3);
    config.setExcludeArray(true);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "maxLevels", "excludeArray");
    assertEquals(node.get("maxLevels").asInt(), 3);
    assertTrue(node.get("excludeArray").asBoolean());
    assertRoundTripIdempotent(config);

    assertEquals(config.getMaxLevels(), 3);
    assertTrue(config.isExcludeArray());
    assertEquals(config.isDisableCrossArrayUnnest(), false);
  }

  @Test
  public void testRoundTripAllSet()
      throws Exception {
    // Wrapper-typed args force selection of the @JsonCreator ctor over the deprecated primitive overload.
    JsonIndexConfig config = new JsonIndexConfig(Boolean.FALSE, Integer.valueOf(5), Boolean.TRUE, Boolean.TRUE,
        Set.of("$.a"), null, Set.of("c"), Set.of("$.d"), Integer.valueOf(128), Boolean.TRUE, Long.valueOf(1024L));

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "maxLevels", "excludeArray", "disableCrossArrayUnnest", "includePaths", "excludeFields",
        "indexPaths", "maxValueLength", "skipInvalidJson", "maxBytesSize");
    assertRoundTripIdempotent(config);

    assertEquals(config.getMaxLevels(), 5);
    assertTrue(config.isExcludeArray());
    assertTrue(config.isDisableCrossArrayUnnest());
    assertEquals(config.getMaxValueLength(), 128);
    assertTrue(config.getSkipInvalidJson());
    assertEquals(config.getMaxBytesSize(), Long.valueOf(1024L));
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig(true);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testEachNonDefaultFieldRoundTrips()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(5);
    assertRoundTripsWithKey(config, "maxLevels");

    config = new JsonIndexConfig();
    config.setExcludeArray(true);
    assertRoundTripsWithKey(config, "excludeArray");

    config = new JsonIndexConfig();
    config.setDisableCrossArrayUnnest(true);
    assertRoundTripsWithKey(config, "disableCrossArrayUnnest");

    config = new JsonIndexConfig();
    config.setIncludePaths(Set.of("$.a"));
    assertRoundTripsWithKey(config, "includePaths");

    config = new JsonIndexConfig();
    config.setExcludePaths(Set.of("$.b"));
    assertRoundTripsWithKey(config, "excludePaths");

    config = new JsonIndexConfig();
    config.setExcludeFields(Set.of("c"));
    assertRoundTripsWithKey(config, "excludeFields");

    config = new JsonIndexConfig();
    config.setIndexPaths(Set.of("$.d"));
    assertRoundTripsWithKey(config, "indexPaths");

    config = new JsonIndexConfig();
    config.setMaxValueLength(128);
    assertRoundTripsWithKey(config, "maxValueLength");

    config = new JsonIndexConfig();
    config.setSkipInvalidJson(true);
    assertRoundTripsWithKey(config, "skipInvalidJson");

    config = new JsonIndexConfig();
    config.setMaxBytesSize(1024L);
    assertRoundTripsWithKey(config, "maxBytesSize");
  }

  @Test
  public void testExplicitFalsePreservedThroughRoundTrip()
      throws Exception {
    // Explicit false / 0 / -1 survives the round-trip — wrapper contract distinguishes "set" from
    // "not set" so future default changes do not silently mutate user intent.
    String fat = "{"
        + "\"maxLevels\":-1,"
        + "\"excludeArray\":false,"
        + "\"disableCrossArrayUnnest\":false,"
        + "\"maxValueLength\":0,"
        + "\"skipInvalidJson\":false"
        + "}";

    JsonIndexConfig config = JsonUtils.stringToObject(fat, JsonIndexConfig.class);

    assertOnlyKeys(serializeToNode(config), "maxLevels", "excludeArray", "disableCrossArrayUnnest",
        "maxValueLength", "skipInvalidJson");
    assertNotEquals(config, new JsonIndexConfig(),
        "Explicit-false config must NOT equal the all-null no-arg constructor");
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(3);
    config.setExcludeArray(true);
    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    config.setExcludeArray(true);
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

  private static void assertRoundTripsWithKey(JsonIndexConfig config, String key)
      throws Exception {
    JsonNode node = serializeToNode(config);
    assertEquals(node.size(), 1, "Only the toggled key should be emitted: " + node);
    assertTrue(node.has(key), "Missing key '" + key + "': " + node);
    assertRoundTripIdempotent(config);
  }

  private static void assertRoundTripIdempotent(JsonIndexConfig original)
      throws Exception {
    JsonNode firstNode = serializeToNode(original);
    JsonIndexConfig restored = JsonUtils.stringToObject(JsonUtils.objectToString(original), JsonIndexConfig.class);
    JsonNode secondNode = serializeToNode(restored);
    assertEquals(secondNode, firstNode, "Slim form must be byte-equivalent across a round-trip");
  }
}
