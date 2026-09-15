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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Round-trip tests for the wire format of [ConfigSuccessResponse]. The DTO gained a `deprecationWarnings` field
/// that must (a) be elided from the JSON when empty so older clients that strict-parse the response continue to
/// see the original shape, and (b) appear when non-empty so callers can surface controller-side warnings.
public class ConfigSuccessResponseTest {

  @Test
  public void testEmptyDeprecationWarningsAreOmittedFromJson() {
    /// The new `deprecationWarnings` field carries `@JsonInclude(NON_EMPTY)` on its getter so existing clients
    /// (including older Java client versions that strict-parse this DTO) keep observing the same JSON shape when no
    /// warnings fire.
    ConfigSuccessResponse response = new ConfigSuccessResponse("ok", Map.of());
    JsonNode json = JsonUtils.objectToJsonNode(response);
    assertFalse(json.has("deprecationWarnings"), "expected deprecationWarnings to be elided when empty: " + json);
  }

  @Test
  public void testNonEmptyDeprecationWarningsAppearInJson() {
    ConfigSuccessResponse response = new ConfigSuccessResponse("ok", Map.of(),
        List.of("'segmentsConfig.segmentPushType' is deprecated since 0.8.0. Use 'ingestionConfig...' instead."));
    JsonNode json = JsonUtils.objectToJsonNode(response);
    assertTrue(json.has("deprecationWarnings"));
    assertEquals(json.get("deprecationWarnings").size(), 1);
  }

  @Test
  public void testNullCollectionsNormalisedToEmpty() {
    ConfigSuccessResponse response = new ConfigSuccessResponse("ok", null, null);
    assertTrue(response.getUnrecognizedProperties().isEmpty());
    assertTrue(response.getDeprecationWarnings().isEmpty());
  }

  /// Locks the Jackson deserialization contract that downstream clients (admin SDK, integration tests, tooling)
  /// rely on. Without `@JsonCreator`, Jackson silently falls back to the `-parameters` compiler flag for argument
  /// names; this test fails if that fallback breaks.
  @Test
  public void testJsonRoundTripPreservesAllFields()
      throws Exception {
    ConfigSuccessResponse original = new ConfigSuccessResponse("ok", Map.of("unrecognised", "value"),
        List.of("warning-one", "warning-two"));
    String json = JsonUtils.objectToString(original);
    ConfigSuccessResponse parsed = JsonUtils.stringToObject(json, ConfigSuccessResponse.class);
    assertEquals(parsed.getStatus(), "ok");
    assertEquals(parsed.getUnrecognizedProperties(), Map.of("unrecognised", "value"));
    assertEquals(parsed.getDeprecationWarnings(), List.of("warning-one", "warning-two"));
  }

  /// Older controller payloads (pre-deprecationWarnings) omit the `deprecationWarnings` field entirely. Newer
  /// clients must accept that shape — this is the rolling-upgrade direction: new client + old controller.
  @Test
  public void testParsingResponseWithoutDeprecationWarningsField()
      throws Exception {
    String json = "{\"status\":\"ok\",\"unrecognizedProperties\":{}}";
    ConfigSuccessResponse parsed = JsonUtils.stringToObject(json, ConfigSuccessResponse.class);
    assertEquals(parsed.getStatus(), "ok");
    assertTrue(parsed.getDeprecationWarnings().isEmpty());
  }

  /// Future controller versions may add new wire fields. Strict-parsing older clients (with
  /// FAIL_ON_UNKNOWN_PROPERTIES=true) must still parse the response — this is the rolling-upgrade direction:
  /// old client + new controller emitting fields the client does not yet know about. Locked here so a future
  /// refactor that removes `@JsonIgnoreProperties(ignoreUnknown=true)` from ConfigSuccessResponse fails this
  /// test before it ships.
  @Test
  public void testStrictParserToleratesUnknownFutureField()
      throws Exception {
    String json = "{\"status\":\"ok\",\"unrecognizedProperties\":{},\"deprecationWarnings\":[\"w1\"],"
        + "\"futureField\":\"some value the old client does not know about\"}";
    com.fasterxml.jackson.databind.ObjectMapper strict = new com.fasterxml.jackson.databind.ObjectMapper()
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    ConfigSuccessResponse parsed = strict.readValue(json, ConfigSuccessResponse.class);
    assertEquals(parsed.getStatus(), "ok");
    assertEquals(parsed.getDeprecationWarnings(), List.of("w1"));
  }
}
