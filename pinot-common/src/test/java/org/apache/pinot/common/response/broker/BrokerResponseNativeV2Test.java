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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class BrokerResponseNativeV2Test {
  @Test
  public void testEarlyTerminationReasonsMarkPartialResponse() {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    assertTrue(brokerResponse.getEarlyTerminationReasons().isEmpty());
    assertFalse(brokerResponse.isPartialResult());

    brokerResponse.addBrokerStats(new StatMap<>(BrokerResponseNativeV2.StatKey.class)
        .merge(BrokerResponseNativeV2.StatKey.EARLY_TERMINATION_REASONS,
            stringSet("DISTINCT_MAX_ROWS", "DISTINCT_MAX_ROWS_WITHOUT_CHANGE"))
        .merge(BrokerResponseNativeV2.StatKey.EARLY_TERMINATION_REASONS,
            stringSet("DISTINCT_MAX_ROWS", "DISTINCT_MAX_EXECUTION_TIME")));

    assertEquals(brokerResponse.getEarlyTerminationReasons(),
        List.of("DISTINCT_MAX_ROWS", "DISTINCT_MAX_ROWS_WITHOUT_CHANGE", "DISTINCT_MAX_EXECUTION_TIME"));
    assertTrue(brokerResponse.isPartialResult());
  }

  /// The remote access stats are serialized only when a remote access happened, and the direct ones only when a direct
  /// remote access happened.
  @Test
  public void testRemoteAccessStatsSerializedOnlyWhenNonZero() {
    BrokerResponseNativeV2 response = new BrokerResponseNativeV2();
    JsonNode json = JsonUtils.objectToJsonNode(response);
    assertFalse(json.has("numRemoteAccesses"));
    assertFalse(json.has("remoteAccessBytes"));
    assertFalse(json.has("numDirectRemoteAccesses"));
    assertFalse(json.has("directRemoteAccessBytes"));

    StatMap<BrokerResponseNativeV2.StatKey> stats = new StatMap<>(BrokerResponseNativeV2.StatKey.class);
    stats.merge(BrokerResponseNativeV2.StatKey.NUM_REMOTE_ACCESSES, 7L);
    stats.merge(BrokerResponseNativeV2.StatKey.REMOTE_ACCESS_BYTES, 700L);
    response.addBrokerStats(stats);
    json = JsonUtils.objectToJsonNode(response);
    assertEquals(json.get("numRemoteAccesses").asLong(), 7L);
    assertEquals(json.get("remoteAccessBytes").asLong(), 700L);
    assertFalse(json.has("numDirectRemoteAccesses"));
    assertFalse(json.has("directRemoteAccessBytes"));

    stats = new StatMap<>(BrokerResponseNativeV2.StatKey.class);
    stats.merge(BrokerResponseNativeV2.StatKey.NUM_DIRECT_REMOTE_ACCESSES, 2L);
    stats.merge(BrokerResponseNativeV2.StatKey.DIRECT_REMOTE_ACCESS_BYTES, 20L);
    response.addBrokerStats(stats);
    json = JsonUtils.objectToJsonNode(response);
    assertEquals(json.get("numDirectRemoteAccesses").asLong(), 2L);
    assertEquals(json.get("directRemoteAccessBytes").asLong(), 20L);
  }

  @Test
  public void testResponseMetadataSerialization()
      throws Exception {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    // Empty by default and omitted from JSON (NON_EMPTY).
    assertTrue(brokerResponse.getResponseMetadata().isEmpty());
    JsonNode emptyNode = JsonUtils.stringToJsonNode(brokerResponse.toJsonString());
    assertNull(emptyNode.get("responseMetadata"));

    // A boolean via the JsonNode overload, a string via the convenience overload, and a nested
    // object to exercise arbitrary (complex) JSON values.
    brokerResponse.putResponseMetadata("boolEntry", BooleanNode.getTrue());
    brokerResponse.putResponseMetadata("stringEntry", "hello");
    ObjectNode nested = JsonUtils.newObjectNode();
    nested.put("count", 2);
    nested.put("label", "example");
    brokerResponse.putResponseMetadata("objectEntry", nested);

    JsonNode node = JsonUtils.stringToJsonNode(brokerResponse.toJsonString()).get("responseMetadata");
    assertTrue(node.get("boolEntry").isBoolean());
    assertTrue(node.get("boolEntry").asBoolean());
    assertEquals(node.get("stringEntry").asText(), "hello");
    assertEquals(node.get("objectEntry").get("count").asInt(), 2);
    assertEquals(node.get("objectEntry").get("label").asText(), "example");
  }

  private static Set<String> stringSet(String... values) {
    return new LinkedHashSet<>(List.of(values));
  }
}
