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
import java.io.IOException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class QuotaConfigTest {

  @Test
  public void testStorageQuota()
      throws IOException {
    {
      String quotaConfigStr = "{\"storage\" : \"100gb\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getStorage(), "100G");
      assertEquals(quotaConfig.getStorageInBytes(), 100L * 1024 * 1024 * 1024);
    }
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertNull(quotaConfig.getStorage());
      assertEquals(quotaConfig.getStorageInBytes(), -1L);
    }
  }

  @Test
  public void testInvalidStorageQuota() {
    try {
      String quotaConfigStr = "{\"storage\" : \"124GB3GB\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      String quotaConfigStr = "{\"storage\":\"-1M\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testQPSQuota()
      throws IOException {
    {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"100\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getMaxQueriesPerSecond(), "100.0");
      assertEquals(quotaConfig.getMaxQPS(), 100.0);
    }
    {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"0.5\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getMaxQueriesPerSecond(), "0.5");
      assertEquals(quotaConfig.getMaxQPS(), 0.5);
    }
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertNull(quotaConfig.getMaxQueriesPerSecond());
      assertEquals(quotaConfig.getMaxQPS(), -1.0);
    }
  }

  @Test
  public void testInvalidQPSQuota() {
    try {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"InvalidQpsQuota\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"-1.0\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"1.0Test\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testInvalidStorageMessageAndCause() {
    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> new QuotaConfig("124GB3GB", null));
    String msg = e.getMessage();
    assertTrue(msg.contains("storage"), "Error message should reference the 'storage' field, got: " + msg);
    assertTrue(msg.contains("124GB3GB"), "Error message should include the offending value, got: " + msg);
    assertNotNull(e.getCause(), "Underlying parse exception should be preserved as the cause");
  }

  @Test
  public void testInvalidMaxQPSMessageReferencesCorrectField() {
    // Verify the field name AND the offending value are both reported correctly.
    // Regression: previously the message was "Invalid 'maxQueriesPerSecond': " + storage, i.e. it
    // interpolated the wrong variable and reported 'null' (or a valid 'storage' value) instead of
    // the actual bad maxQueriesPerSecond input.
    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> new QuotaConfig(null, "InvalidQpsQuota"));
    String msg = e.getMessage();
    assertTrue(msg.contains("maxQueriesPerSecond"),
        "Error message should reference the 'maxQueriesPerSecond' field, got: " + msg);
    assertTrue(msg.contains("InvalidQpsQuota"),
        "Error message should include the offending value, got: " + msg);
    assertNotNull(e.getCause(), "Underlying parse exception should be preserved as the cause");

    // Also verify the case where 'storage' is set alongside a bad maxQueriesPerSecond. The old code
    // used to render the storage value here, misleading the operator into thinking storage was invalid.
    IllegalArgumentException withStorage =
        expectThrows(IllegalArgumentException.class, () -> new QuotaConfig("100G", "InvalidQpsQuota"));
    String withStorageMsg = withStorage.getMessage();
    assertTrue(withStorageMsg.contains("InvalidQpsQuota"),
        "Error message should include the offending maxQueriesPerSecond value, got: " + withStorageMsg);
    assertFalse(withStorageMsg.contains("100G"),
        "Error message must not report the (valid) 'storage' value, got: " + withStorageMsg);
  }

  @Test
  public void testNonPositiveMaxQPSMessageIsInformative() {
    // Preconditions.checkArgument previously had no message, producing a null-message
    // IllegalArgumentException wrapped as "Invalid 'maxQueriesPerSecond': null".
    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> new QuotaConfig(null, "-1.0"));
    String msg = e.getMessage();
    assertTrue(msg.contains("maxQueriesPerSecond") && msg.contains("-1.0"),
        "Outer message should reference the field and offending value, got: " + msg);
    Throwable cause = e.getCause();
    assertNotNull(cause, "Preconditions.checkArgument failure should be preserved as the cause");
    assertTrue(cause instanceof IllegalArgumentException,
        "Cause should be the IllegalArgumentException thrown by Preconditions.checkArgument, got: " + cause);
    String causeMsg = cause.getMessage();
    assertNotNull(causeMsg, "Preconditions.checkArgument should carry a non-null message");
    assertTrue(causeMsg.contains("maxQueriesPerSecond"),
        "Cause message should reference the field, got: " + causeMsg);
    assertTrue(causeMsg.contains("-1.0"), "Cause message should include the offending value, got: " + causeMsg);
  }

  @Test
  public void testZeroMaxQPSRejected() {
    // Zero must be rejected by the '_maxQPS > 0' check.
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new QuotaConfig(null, "0"));
    assertTrue(e.getMessage().contains("maxQueriesPerSecond") && e.getMessage().contains("0"),
        "Error message should reference the field and offending value, got: " + e.getMessage());
  }

  @Test
  public void testNonFiniteMaxQPSRejected() {
    // 'Infinity' parses to Double.POSITIVE_INFINITY, which passes '> 0'. Without the isFinite guard
    // this becomes an undocumented back-door for unlimited QPS. Both Infinity and NaN must be rejected.
    for (String bad : new String[] {"Infinity", "-Infinity", "NaN"}) {
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> new QuotaConfig(null, bad));
      String msg = e.getMessage();
      assertTrue(msg.contains("maxQueriesPerSecond"),
          "Error message should reference the field for input '" + bad + "', got: " + msg);
      assertTrue(msg.contains(bad),
          "Error message should include the offending value '" + bad + "', got: " + msg);
    }
  }

  @Test
  public void testInvalidQuotaThroughJacksonSurfacesFieldAndValue()
      throws IOException {
    // Exercise the operator-facing path (JSON -> POJO). The IllegalArgumentException is wrapped by
    // Jackson; we walk the cause chain and assert the same invariants: field name + offending value.
    String json = "{\"maxQueriesPerSecond\" : \"InvalidQpsQuota\"}";
    Exception thrown = expectThrows(Exception.class, () -> JsonUtils.stringToObject(json, QuotaConfig.class));

    boolean foundField = false;
    boolean foundValue = false;
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      String tm = t.getMessage();
      if (tm == null) {
        continue;
      }
      if (tm.contains("maxQueriesPerSecond")) {
        foundField = true;
      }
      if (tm.contains("InvalidQpsQuota")) {
        foundValue = true;
      }
    }
    assertTrue(foundField && foundValue,
        "Deserialization error chain should mention 'maxQueriesPerSecond' and 'InvalidQpsQuota': " + thrown);
  }

  @Test
  public void testEnforcementModeSerDe()
      throws IOException {
    // Absent by default, and not serialized when null
    QuotaConfig quotaConfig = JsonUtils.stringToObject("{\"storage\":\"1G\"}", QuotaConfig.class);
    assertNull(quotaConfig.getStorageQuotaEnforcementMode());
    assertNull(quotaConfig.getQueryQuotaEnforcementMode());
    assertNull(quotaConfig.toJsonNode().get("storageQuotaEnforcementMode"));
    assertNull(quotaConfig.toJsonNode().get("queryQuotaEnforcementMode"));

    // Deserialize from JSON
    quotaConfig = JsonUtils.stringToObject(
        "{\"storage\":\"1G\",\"maxQueriesPerSecond\":\"5\",\"storageQuotaEnforcementMode\":\"SHADOW\","
            + "\"queryQuotaEnforcementMode\":\"DISABLED\"}", QuotaConfig.class);
    assertEquals(quotaConfig.getStorageQuotaEnforcementMode(), QuotaEnforcementMode.SHADOW);
    assertEquals(quotaConfig.getQueryQuotaEnforcementMode(), QuotaEnforcementMode.DISABLED);

    // Round trip, including equality (equals() is JSON based)
    QuotaConfig roundTripped = JsonUtils.stringToObject(quotaConfig.toJsonString(), QuotaConfig.class);
    assertEquals(roundTripped, quotaConfig);
    assertEquals(roundTripped.getStorageQuotaEnforcementMode(), QuotaEnforcementMode.SHADOW);
    assertEquals(roundTripped.getQueryQuotaEnforcementMode(), QuotaEnforcementMode.DISABLED);

    // Modes affect equality
    assertNotEquals(new QuotaConfig("1G", "5.0"), quotaConfig);
  }

  @Test
  public void testSerializedFormWithoutModesIsUnchanged()
      throws IOException {
    // Configs that do not set a mode must serialize exactly as before the modes existed, so that rolling out (or back)
    // does not rewrite them in ZooKeeper and binaries without these fields keep reading them
    assertEquals(new QuotaConfig("10G", "5").toJsonString(), "{\"storage\":\"10G\",\"maxQueriesPerSecond\":\"5.0\"}");

    // A config written before the modes existed deserializes with no modes and serializes back unchanged
    String legacyJson = "{\"storage\":\"10G\",\"maxQueriesPerSecond\":\"5.0\"}";
    QuotaConfig legacy = JsonUtils.stringToObject(legacyJson, QuotaConfig.class);
    assertNull(legacy.getStorageQuotaEnforcementMode());
    assertNull(legacy.getQueryQuotaEnforcementMode());
    assertEquals(legacy.toJsonString(), legacyJson);
    assertEquals(legacy, new QuotaConfig("10G", "5"));

    // Only the mode that was set is serialized
    QuotaConfig queryOnly = new QuotaConfig("10G", "5", null, QuotaEnforcementMode.SHADOW);
    assertEquals(queryOnly.toJsonString(),
        "{\"storage\":\"10G\",\"maxQueriesPerSecond\":\"5.0\",\"queryQuotaEnforcementMode\":\"SHADOW\"}");
  }

  @Test
  public void testInvalidEnforcementMode() {
    try {
      JsonUtils.stringToObject("{\"storageQuotaEnforcementMode\":\"SOFT\"}", QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testSerDe()
      throws IOException {
    QuotaConfig quotaConfig = new QuotaConfig("100G", "100.0");
    JsonNode quotaConfigJson = quotaConfig.toJsonNode();
    assertEquals(quotaConfigJson.get("storage").asText(), "100G");
    assertEquals(quotaConfigJson.get("maxQueriesPerSecond").asText(), "100.0");
    assertNull(quotaConfigJson.get("storageInBytes"));
    assertNull(quotaConfigJson.get("maxQPS"));

    assertEquals(JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class), quotaConfig);
    assertEquals(JsonUtils.stringToObject(quotaConfig.toJsonString(), QuotaConfig.class), quotaConfig);
  }
}
