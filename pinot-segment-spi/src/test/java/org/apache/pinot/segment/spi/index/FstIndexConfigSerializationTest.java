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
 * Verifies that {@link FstIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method. {@code FstIndexConfig} adds no fields beyond the base
 * {@code disabled} field, so the override delegates entirely to the base — but is still tested
 * here so the slim-form contract is locally enforced.
 */
public class FstIndexConfigSerializationTest {

  @Test
  public void testEnabledDefaultIsEmpty()
      throws Exception {
    FstIndexConfig config = new FstIndexConfig(false);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    FstIndexConfig config = FstIndexConfig.DISABLED;

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    FstIndexConfig config = JsonUtils.stringToObject("{\"disabled\":false}", FstIndexConfig.class);

    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    FstIndexConfig config = new FstIndexConfig(true);

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    FstIndexConfig config = new FstIndexConfig(true);

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
