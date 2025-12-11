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
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
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
  public void testValidQuotaDeserialization()
      throws IOException {
    // Test Case 1: An integer rate, specified per second with complete config.
    // This directly corresponds to the old "maxQueriesPerSecond": "100"
    {
      String quotaConfigStr = "{\"rateLimits\": 100.0, \"rateLimiterUnit\": \"SECONDS\", \"rateLimiterDuration\": 1.0}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), 100.0);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
    }

    // Test Case 2: A fractional rate, specified per minute with complete config.
    // This corresponds to the old "maxQueriesPerSecond": "0.5" logic.
    {
      String quotaConfigStr = "{\"rateLimits\": 0.5, \"rateLimiterUnit\": \"SECONDS\", \"rateLimiterDuration\": 1.0}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), 0.5);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
    }

    // Test Case 3: An empty config should result in default values.
    // This logic is preserved from the original test.
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), -1.0);
      assertNull(quotaConfig.getRateLimiterUnit());
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
    }

    // Test Case 4: Incomplete rate limiter config should result in default values
    // When only some parameters are provided, the system should not set rate limiting
    {
      String quotaConfigStr = "{\"rateLimits\": 100.0, \"rateLimiterUnit\": \"SECONDS\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), -1.0); // Should be default value since config is incomplete
      assertNull(quotaConfig.getRateLimiterUnit());
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
    }
  }

  @Test
  public void testInvalidQuotaConfig()
      throws IOException {
    // === Category 1: Individually Invalid Parameters ===

    // Test for a semantically invalid negative value for 'rateLimits' with complete config
    try {
      String quotaConfigStr = "{\"rateLimits\": -1.0, \"rateLimiterUnit\": \"SECONDS\", \"rateLimiterDuration\": 1.0}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Test for a semantically invalid negative value for 'rateLimiterDuration' with complete config
    try {
      String quotaConfigStr = "{\"rateLimits\": 10.0, \"rateLimiterUnit\": \"SECONDS\", \"rateLimiterDuration\": -1.0}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Test for an invalid enum value for 'rateLimiterUnit' with complete config
    try {
      String quotaConfigStr = "{\"rateLimits\": 10.0, \"rateLimiterUnit\": \"DECADES\", \"rateLimiterDuration\": 1.0}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // === Category 2: Incomplete Configurations (should not throw exceptions, but set default values) ===

    // Test for specifying rateLimits WITHOUT a time unit or duration
    // Current behavior: incomplete config results in default values, no exception thrown
    {
      String quotaConfigStr = "{\"rateLimits\": 100.0}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), -1.0); // Should be default value
      assertNull(quotaConfig.getRateLimiterUnit());
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
    }

    // Test for specifying a time unit WITHOUT rateLimits
    // Current behavior: incomplete config results in default values, no exception thrown
    {
      String quotaConfigStr = "{\"rateLimiterUnit\": \"MINUTES\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), -1.0); // Should be default value
      assertNull(quotaConfig.getRateLimiterUnit());
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
    }

    // Test for specifying a duration WITHOUT rateLimits
    // Current behavior: incomplete config results in default values, no exception thrown
    {
      String quotaConfigStr = "{\"rateLimiterDuration\": 5.0}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), -1.0); // Should be default value
      assertNull(quotaConfig.getRateLimiterUnit());
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
    }
  }

  @Test
  public void testFlexibleRateLimiterConfigurations()
      throws IOException {
    // Combined test for various rate limiter configurations
    Object[][] testCases = {
        // Seconds-based configurations
        {TimeUnit.SECONDS, 1d, 100d, "Basic seconds configuration"},
        {TimeUnit.SECONDS, 5d, 50d, "Extended seconds configuration"},
        {TimeUnit.SECONDS, 10d, 25d, "Long seconds configuration"},

        // Minutes-based configurations
        {TimeUnit.MINUTES, 1d, 60d, "Basic minutes configuration"},
        {TimeUnit.MINUTES, 5d, 300d, "Extended minutes configuration"},
        {TimeUnit.MINUTES, 10d, 600d, "Long minutes configuration"}
    };

    for (Object[] testCase : testCases) {
      TimeUnit unit = (TimeUnit) testCase[0];
      Double duration = (Double) testCase[1];
      Double rateLimits = (Double) testCase[2];
      String description = (String) testCase[3];

      QuotaConfig quotaConfig = new QuotaConfig(null, unit, duration, rateLimits);
      assertEquals(quotaConfig.getRateLimiterUnit(), unit, "Unit mismatch for: " + description);
      assertEquals(quotaConfig.getRateLimiterDuration(), duration, "Duration mismatch for: " + description);
      assertEquals(quotaConfig.getRateLimits(), rateLimits, "Rate limits mismatch for: " + description);
    }
  }

  @Test
  public void testFractionalRateLimits()
      throws IOException {
    // Test fractional rate limits - combined test for efficiency
    double[] fractionalRates = {0.25, 0.5, 0.1, 0.333};

    for (double rate : fractionalRates) {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, rate);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), rate, "Fractional rate " + rate + " not preserved");
    }
  }



  @Test
  public void testRateLimiterConfigSerialization()
      throws IOException {
    // Combined test for serialization and deserialization
    Object[][] testCases = {
        {"100G", TimeUnit.SECONDS, 5d, 25d, "Seconds with storage"},
        {"50G", TimeUnit.MINUTES, 10d, 120d, "Minutes with storage"},
        {null, TimeUnit.SECONDS, 1d, 0.25d, "Fractional rates"}
    };

    for (Object[] testCase : testCases) {
      String storage = (String) testCase[0];
      TimeUnit unit = (TimeUnit) testCase[1];
      Double duration = (Double) testCase[2];
      Double rateLimits = (Double) testCase[3];
      String description = (String) testCase[4];

      QuotaConfig quotaConfig = new QuotaConfig(storage, unit, duration, rateLimits);
      JsonNode quotaConfigJson = quotaConfig.toJsonNode();

      if (storage != null) {
        assertEquals(quotaConfigJson.get("storage").asText(), storage);
      }
      assertEquals(quotaConfigJson.get("rateLimiterUnit").asText(), unit.name());
      assertEquals(quotaConfigJson.get("rateLimiterDuration").asDouble(), duration);
      assertEquals(quotaConfigJson.get("rateLimits").asDouble(), rateLimits);

      QuotaConfig deserializedConfig = JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class);
      assertEquals(deserializedConfig, quotaConfig, "Serialization failed for: " + description);
    }
  }



  @Test
  public void testInvalidFlexibleRateLimiterConfig() {
    // Test invalid rate limiter configurations

    // Test negative duration
    try {
      new QuotaConfig(null, TimeUnit.SECONDS, -1d, 100d);
      fail("Expected exception for negative duration");
    } catch (Exception e) {
      // Expected
    }

    // Test zero duration
    try {
      new QuotaConfig(null, TimeUnit.SECONDS, 0d, 100d);
      fail("Expected exception for zero duration");
    } catch (Exception e) {
      // Expected
    }

    // Test with only some rate limiter parameters (should not set rate limiter)
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, null, 100d);
      assertEquals(quotaConfig.getRateLimiterUnit(), null);
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
      assertEquals(quotaConfig.getRateLimits(), -1.0);
    }

    {
      QuotaConfig quotaConfig = new QuotaConfig(null, null, 1d, 100d);
      assertEquals(quotaConfig.getRateLimiterUnit(), null);
      assertEquals(quotaConfig.getRateLimiterDuration(), -1.0);
      assertEquals(quotaConfig.getRateLimits(), -1.0);
    }
  }

  @Test
  public void testQuotaConfigSetMethod() {
    // Test the isQuotaConfigSet method with new flexible configurations

    // Config with all rate limiter parameters set should have quota config set
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 100d);
      assertEquals(quotaConfig.isQuotaConfigSet(), true); // Returns true when rate limits are NOT -1
    }

    // Config without rate limiter parameters should not have quota config set
    {
      QuotaConfig quotaConfig = new QuotaConfig("100G", null, null, null);
      assertEquals(quotaConfig.isQuotaConfigSet(), false); // Returns false when rate limits are -1
    }

    // Config with partial rate limiter parameters should not have quota config set
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, null, null);
      assertEquals(quotaConfig.isQuotaConfigSet(), false);
    }
  }

  @Test
  public void testSerDe()
      throws IOException {
    QuotaConfig quotaConfig = new QuotaConfig("100G", TimeUnit.SECONDS, 1d, 100d);
    JsonNode quotaConfigJson = quotaConfig.toJsonNode();
    assertEquals(quotaConfigJson.get("storage").asText(), "100G");
    assertEquals(quotaConfigJson.get("rateLimits").asDouble(), 100.0);
    assertNull(quotaConfigJson.get("storageInBytes"));

    assertEquals(JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class), quotaConfig);
    assertEquals(JsonUtils.stringToObject(quotaConfig.toJsonString(), QuotaConfig.class), quotaConfig);
  }
}
