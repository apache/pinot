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
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link StarTreeIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method. {@code dimensionsSplitOrder} is required and always
 * emitted; the optional list fields appear only when non-null (the ctor coerces empty lists to
 * {@code null}).
 */
public class StarTreeIndexConfigSerializationTest {

  @Test
  public void testMinimalConfigOmitsOptionals()
      throws Exception {
    StarTreeIndexConfig config = new StarTreeIndexConfig(
        List.of("dim"), null, List.of("SUM__metric"), null, 0);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "dimensionsSplitOrder", "functionColumnPairs");
  }

  @Test
  public void testEmptyOptionalListsCoercedToNullAndOmitted()
      throws Exception {
    StarTreeIndexConfig config = new StarTreeIndexConfig(
        List.of("dim"), List.of(), List.of("SUM__metric"), List.of(), 0);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "dimensionsSplitOrder", "functionColumnPairs");
  }

  @Test
  public void testMaxLeafRecordsOmittedWhenZero()
      throws Exception {
    StarTreeIndexConfig config = new StarTreeIndexConfig(
        List.of("dim"), null, List.of("SUM__metric"), null, 0);

    JsonNode node = serializeToNode(config);

    assertTrue(!node.has("maxLeafRecords"));
  }

  @Test
  public void testMaxLeafRecordsEmittedWhenNonZero()
      throws Exception {
    StarTreeIndexConfig config = new StarTreeIndexConfig(
        List.of("dim"), null, List.of("SUM__metric"), null, 1000);

    JsonNode node = serializeToNode(config);

    assertEquals(node.get("maxLeafRecords").asInt(), 1000);
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{"
        + "\"dimensionsSplitOrder\":[\"dim\"],"
        + "\"skipStarNodeCreationForDimensions\":[],"
        + "\"functionColumnPairs\":[\"SUM__metric\"],"
        + "\"aggregationConfigs\":[],"
        + "\"maxLeafRecords\":0"
        + "}";

    StarTreeIndexConfig config = JsonUtils.stringToObject(fat, StarTreeIndexConfig.class);

    assertOnlyKeys(serializeToNode(config), "dimensionsSplitOrder", "functionColumnPairs");
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    StarTreeIndexConfig config = new StarTreeIndexConfig(
        List.of("dim"), List.of("dim2"), List.of("SUM__metric"), null, 100);

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    StarTreeIndexConfig config = new StarTreeIndexConfig(
        List.of("dim"), null, List.of("SUM__metric"), null, 0);

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
    assertTrue(node.has("dimensionsSplitOrder"),
        "dimensionsSplitOrder is required and must always be present: " + node);
  }
}
