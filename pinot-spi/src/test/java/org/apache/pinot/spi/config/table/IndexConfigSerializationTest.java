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
 * Verifies that {@link IndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each field only when it differs from its
 * class default. This is the base class for all {@code *IndexConfig} types — see also the
 * subclass-specific {@code *SerializationTest}s.
 */
public class IndexConfigSerializationTest {

  @Test
  public void testDefaultPojoSerializesToEmptyJson()
      throws Exception {
    IndexConfig config = new IndexConfig(false);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    IndexConfig config = new IndexConfig(true);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    // Fat = explicit "disabled": false. Pre-fix output looked like this.
    IndexConfig config = JsonUtils.stringToObject("{\"disabled\":false}", IndexConfig.class);

    assertFalse(config.isDisabled());
    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    IndexConfig enabled = new IndexConfig(false);
    IndexConfig disabled = new IndexConfig(true);

    assertEquals(serializeToNode(enabled), enabled.toJsonObject());
    assertEquals(serializeToNode(disabled), disabled.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    IndexConfig config = new IndexConfig(true);
    String json = new ObjectMapper().writeValueAsString(config);

    assertEquals(JsonUtils.stringToJsonNode(json), config.toJsonObject());
  }

  /**
   * Trivial subclass without an override should still emit only {@code disabled}.
   * Documents that subclasses are free to inherit the base serializer verbatim.
   */
  @Test
  public void testSubclassWithoutOverrideUsesBaseSerializer()
      throws Exception {
    BareSubclass enabled = new BareSubclass(false);
    BareSubclass disabled = new BareSubclass(true);

    assertOnlyKeys(serializeToNode(enabled));
    assertOnlyKeys(serializeToNode(disabled), "disabled");
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

  /** Test-only subclass with no extra fields and no serializer override. */
  private static final class BareSubclass extends IndexConfig {
    BareSubclass(Boolean disabled) {
      super(disabled);
    }
  }
}
