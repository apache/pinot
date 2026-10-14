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
import java.io.IOException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class BrokerResponseNativeTest {
  @Test
  public void testEmptyResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.EMPTY_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    assertEquals(actual.getNumDocsScanned(), expected.getNumDocsScanned());
    assertEquals(actual.getTimeUsedMs(), expected.getTimeUsedMs());
    assertEquals(actual.getResultTable(), expected.getResultTable());
  }

  /// The remote access stats are serialized only when a remote access happened, and the direct ones only when a direct
  /// remote access happened.
  @Test
  public void testRemoteAccessStatsSerializedOnlyWhenNonZero()
      throws IOException {
    BrokerResponseNative response = new BrokerResponseNative();
    JsonNode json = JsonUtils.stringToJsonNode(response.toJsonString());
    assertFalse(json.has("numRemoteAccesses"));
    assertFalse(json.has("remoteAccessBytes"));
    assertFalse(json.has("numDirectRemoteAccesses"));
    assertFalse(json.has("directRemoteAccessBytes"));

    response.setNumRemoteAccesses(7L);
    response.setRemoteAccessBytes(700L);
    json = JsonUtils.stringToJsonNode(response.toJsonString());
    assertEquals(json.get("numRemoteAccesses").asLong(), 7L);
    assertEquals(json.get("remoteAccessBytes").asLong(), 700L);
    assertFalse(json.has("numDirectRemoteAccesses"));
    assertFalse(json.has("directRemoteAccessBytes"));

    response.setNumDirectRemoteAccesses(2L);
    response.setDirectRemoteAccessBytes(20L);
    json = JsonUtils.stringToJsonNode(response.toJsonString());
    assertEquals(json.get("numDirectRemoteAccesses").asLong(), 2L);
    assertEquals(json.get("directRemoteAccessBytes").asLong(), 20L);
    BrokerResponseNative deserialized = BrokerResponseNative.fromJsonString(response.toJsonString());
    assertEquals(deserialized.getNumDirectRemoteAccesses(), 2L);
    assertEquals(deserialized.getDirectRemoteAccessBytes(), 20L);
  }

  @Test
  public void testNullResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    assertEquals(actual.getExceptions().get(0).getErrorCode(), QueryErrorCode.BROKER_RESOURCE_MISSING.getId());
    assertEquals(actual.getExceptions().get(0).getMessage(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getDefaultMessage());
  }

  @Test
  public void testMultipleExceptionsResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String errorMsgStr = "Some random string!";
    expected.addException(new QueryProcessingException(400, errorMsgStr));
    String brokerString = expected.toJsonString();
    BrokerResponseNative newBrokerResponse = BrokerResponseNative.fromJsonString(brokerString);
    assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getId());
    assertEquals(newBrokerResponse.getExceptions().get(0).getMessage(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getDefaultMessage());
    assertEquals(newBrokerResponse.getExceptions().get(1).getErrorCode(), 400);
    assertEquals(newBrokerResponse.getExceptions().get(1).getMessage(), errorMsgStr);
  }

  @Test
  public void testMaterializedViewResponseFields()
      throws IOException {
    BrokerResponseNative expected = new BrokerResponseNative();
    expected.setMaterializedViewQueried("orders_by_day_OFFLINE");

    String brokerString = expected.toJsonString();
    assertTrue(brokerString.contains("\"materializedViewQueried\""));

    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    assertEquals(actual.getMaterializedViewQueried(), expected.getMaterializedViewQueried());
  }

  @Test
  public void testMaterializedViewQueriedAbsentWhenNull()
      throws IOException {
    BrokerResponseNative response = new BrokerResponseNative();
    String json = response.toJsonString();
    assertFalse(json.contains("materializedViewQueried"),
        "Null materializedViewQueried should be suppressed by @JsonInclude(NON_NULL)");
  }

  @Test
  public void testServerStatsRoundTrip()
      throws IOException {
    BrokerResponseNative expected = new BrokerResponseNative();
    String stats = "Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs,RequestSentDelayMs;"
        + "pinot-server-0_O=0,1,7571,0,0;pinot-server-1_O=0,1,7574,0,0";
    expected.setServerStats(stats);

    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(expected.toJsonString());
    assertEquals(actual.getServerStats(), stats);
  }

  @Test
  public void testServerStatsAbsentWhenNull()
      throws IOException {
    BrokerResponseNative response = new BrokerResponseNative();
    JsonNode tree = JsonUtils.stringToJsonNode(response.toJsonString());
    assertFalse(tree.has("serverStats"));
  }

  @Test
  public void testServerStatsDefaultsToNull() {
    assertNull(new BrokerResponseNative().getServerStats());
  }

  /// Regression test for backward-compatible deserialization: [BrokerResponseNative] does not
  /// override `getResponseMetadata()`, so the interface default (marked `@JsonIgnore`) must keep
  /// `responseMetadata` an unknown property. Otherwise Jackson would treat it as a known setterless
  /// Map property and try to populate the immutable `Map.of()` default via USE_GETTERS_AS_SETTERS,
  /// failing with an [UnsupportedOperationException] when a response produced by a newer broker
  /// (e.g. `BrokerResponseNativeV2`) carries a non-empty `responseMetadata`.
  @Test
  public void testResponseMetadataDeserializationCompatibility()
      throws IOException {
    String json = "{\"responseMetadata\":{\"note\":\"executed with fallback strategy\","
        + "\"details\":{\"count\":2}},\"numDocsScanned\":5}";
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(json);
    // The unknown field is ignored on the legacy impl; other fields still deserialize correctly.
    assertEquals(actual.getNumDocsScanned(), 5);
    assertTrue(actual.getResponseMetadata().isEmpty());
  }
}
