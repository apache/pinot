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
    // The new `deprecationWarnings` field carries `@JsonInclude(NON_EMPTY)` on its getter so existing clients
    // (including older Java client versions that strict-parse this DTO) keep observing the same JSON shape when no
    // warnings fire.
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
}
