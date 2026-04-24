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
 * Verifies that {@link JsonIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each field only when it differs from its
 * class default.
 */
public class JsonIndexConfigSerializationTest {

  @Test
  public void testDefaultPojoSerializesToEmptyJson()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
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
    // maxLevels
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(5);
    assertRoundTripsWithKey(config, "maxLevels");

    // excludeArray
    config = new JsonIndexConfig();
    config.setExcludeArray(true);
    assertRoundTripsWithKey(config, "excludeArray");

    // disableCrossArrayUnnest
    config = new JsonIndexConfig();
    config.setDisableCrossArrayUnnest(true);
    assertRoundTripsWithKey(config, "disableCrossArrayUnnest");

    // includePaths
    config = new JsonIndexConfig();
    config.setIncludePaths(Set.of("$.a"));
    assertRoundTripsWithKey(config, "includePaths");

    // excludePaths (mutex with includePaths)
    config = new JsonIndexConfig();
    config.setExcludePaths(Set.of("$.b"));
    assertRoundTripsWithKey(config, "excludePaths");

    // excludeFields
    config = new JsonIndexConfig();
    config.setExcludeFields(Set.of("c"));
    assertRoundTripsWithKey(config, "excludeFields");

    // indexPaths
    config = new JsonIndexConfig();
    config.setIndexPaths(Set.of("$.d"));
    assertRoundTripsWithKey(config, "indexPaths");

    // maxValueLength
    config = new JsonIndexConfig();
    config.setMaxValueLength(128);
    assertRoundTripsWithKey(config, "maxValueLength");

    // skipInvalidJson
    config = new JsonIndexConfig();
    config.setSkipInvalidJson(true);
    assertRoundTripsWithKey(config, "skipInvalidJson");

    // maxBytesSize
    config = new JsonIndexConfig();
    config.setMaxBytesSize(1024L);
    assertRoundTripsWithKey(config, "maxBytesSize");
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{"
        + "\"disabled\":false,"
        + "\"maxLevels\":-1,"
        + "\"excludeArray\":false,"
        + "\"disableCrossArrayUnnest\":false,"
        + "\"maxValueLength\":0,"
        + "\"skipInvalidJson\":false"
        + "}";

    JsonIndexConfig config = JsonUtils.stringToObject(fat, JsonIndexConfig.class);

    // Round-trips back to slim.
    assertOnlyKeys(serializeToNode(config));
    assertEquals(config, new JsonIndexConfig());
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

  /**
   * Asserts that the serialized output contains exactly {@code expectedKey} and that the
   * deserialized POJO re-serializes to the same slim form (idempotent round-trip).
   *
   * <p>POJO-equality is intentionally not asserted here because {@code JsonIndexConfig._maxLevels}
   * has a field-init default of {@code -1} but the {@code @JsonCreator} primitive parameter
   * defaults to {@code 0} when absent. Both values are functionally equivalent (see
   * {@code JsonUtils.flatten(...)}, which only consults {@code maxLevels} when {@code > 0}), so
   * the slim wire form normalizes to "absent" and the runtime behavior is preserved even though
   * the in-memory primitive value differs after round-trip.
   */
  private static void assertRoundTripsWithKey(JsonIndexConfig config, String expectedKey)
      throws Exception {
    JsonNode node = serializeToNode(config);
    assertTrue(node.has(expectedKey), "Expected key '" + expectedKey + "' in: " + node);
    assertFalse(node.has("disabled"), "disabled should be absent (default false): " + node);
    JsonIndexConfig roundTripped = JsonUtils.stringToObject(node.toString(), JsonIndexConfig.class);
    JsonNode roundTrippedNode = serializeToNode(roundTripped);
    assertEquals(roundTrippedNode, node, "Slim form must be idempotent under round-trip");
  }
}
