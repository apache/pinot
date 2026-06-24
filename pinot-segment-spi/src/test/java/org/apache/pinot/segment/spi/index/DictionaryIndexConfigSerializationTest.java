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
import org.apache.pinot.spi.config.table.Intern;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Slim-serialization contract for [DictionaryIndexConfig].
public class DictionaryIndexConfigSerializationTest {

  @Test
  public void testRoundTripAllNull()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig((Boolean) null, null, null, null);

    assertOnlyKeys(serializeToNode(config));
    assertRoundTripIdempotent(config);

    assertEquals(config.isOnHeap(), false);
    assertEquals(config.isUseVarLengthDictionary(), false);
    assertEquals(config.getIntern(), null);
  }

  @Test
  public void testRoundTripPartial()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, null);

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "onHeap");
    assertTrue(node.get("onHeap").asBoolean());
    assertRoundTripIdempotent(config);

    assertTrue(config.isOnHeap());
    assertEquals(config.isUseVarLengthDictionary(), false);
  }

  @Test
  public void testRoundTripAllSet()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(false, true, true, new Intern(1024));

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "onHeap", "useVarLengthDictionary", "intern");
    assertTrue(node.get("onHeap").asBoolean());
    assertTrue(node.get("useVarLengthDictionary").asBoolean());
    assertEquals(node.get("intern").get("capacity").asInt(), 1024);
    assertRoundTripIdempotent(config);
  }

  @Test
  public void testDefaultConstantSerializesEmpty()
      throws Exception {
    assertOnlyKeys(serializeToNode(DictionaryIndexConfig.DEFAULT));
  }

  @Test
  public void testDisabledConstantOnlyEmitsDisabled()
      throws Exception {
    JsonNode node = serializeToNode(DictionaryIndexConfig.DISABLED);
    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testUseVarLengthDictionaryEmittedWhenTrue()
      throws Exception {
    // Cast disambiguates the (Boolean, Boolean) ctor from (DictionaryIndexConfig, boolean).
    DictionaryIndexConfig config = new DictionaryIndexConfig((Boolean) null, true);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "useVarLengthDictionary");
    assertTrue(node.get("useVarLengthDictionary").asBoolean());
  }

  @Test
  public void testInternEmittedWhenSet()
      throws Exception {
    // Intern requires onHeap=true.
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, null, new Intern(1024));
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "onHeap", "intern");
    assertEquals(node.get("intern").get("capacity").asInt(), 1024);
  }

  @Test
  public void testExplicitFalsePreserved()
      throws Exception {
    // Explicit false is preserved (not swallowed). Wrapper-based contract distinguishes "user set
    // to false" (emit) from "user did not configure" (omit).
    String fat = "{\"onHeap\":false,\"useVarLengthDictionary\":false}";
    DictionaryIndexConfig config = JsonUtils.stringToObject(fat, DictionaryIndexConfig.class);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "onHeap", "useVarLengthDictionary");
    assertEquals(node.get("onHeap").asBoolean(), false);
    assertEquals(node.get("useVarLengthDictionary").asBoolean(), false);

    assertNotEquals(config, DictionaryIndexConfig.DEFAULT,
        "Explicit-false config must NOT equal the all-null DEFAULT constant");
  }

  // ---- Validation preserved ----

  @Test
  public void testInternRequiresOnHeap() {
    assertThrows(IllegalStateException.class,
        () -> new DictionaryIndexConfig((Boolean) null, null, null, new Intern(64)));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, true);
    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    DictionaryIndexConfig config = new DictionaryIndexConfig(true, null);
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

  private static void assertRoundTripIdempotent(DictionaryIndexConfig original)
      throws Exception {
    JsonNode firstNode = serializeToNode(original);
    DictionaryIndexConfig restored =
        JsonUtils.stringToObject(JsonUtils.objectToString(original), DictionaryIndexConfig.class);
    JsonNode secondNode = serializeToNode(restored);
    assertEquals(secondNode, firstNode, "Slim form must be byte-equivalent across a round-trip");
  }
}
