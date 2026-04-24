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
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link RangeIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits {@code version} only when it differs from
 * its class default of {@code 2}.
 */
public class RangeIndexConfigSerializationTest {

  @Test
  public void testDefaultPojoSerializesToEmptyJson()
      throws Exception {
    JsonNode node = serializeToNode(RangeIndexConfig.DEFAULT);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    JsonNode node = serializeToNode(RangeIndexConfig.DISABLED);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testNonDefaultVersionEmitted()
      throws Exception {
    RangeIndexConfig config = new RangeIndexConfig(3);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "version");
    assertEquals(node.get("version").asInt(), 3);
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{\"disabled\":false,\"version\":2}";

    RangeIndexConfig config = JsonUtils.stringToObject(fat, RangeIndexConfig.class);

    assertOnlyKeys(serializeToNode(config));
    assertEquals(config.getVersion(), 2);
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
}
