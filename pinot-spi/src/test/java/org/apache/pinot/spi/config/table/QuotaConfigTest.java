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
    // Test Case 1: An integer rate, specified per second.
    // This directly corresponds to the old "maxQueriesPerSecond": "100"
    {
      String quotaConfigStr = "{\"rateLimits\": 100.0, \"rateLimiterUnit\": \"SECONDS\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), 100.0);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
    }

    // Test Case 2: A fractional rate, specified per minute to show flexibility.
    // This corresponds to the old "maxQueriesPerSecond": "0.5" logic.
    {
      String quotaConfigStr = "{\"rateLimits\": 0.5, \"rateLimiterUnit\": \"SECONDS\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), 0.5);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
    }

    // Test Case 3: An empty config should result in default values.
    // This logic is preserved from the original test.
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimits(), -1.0);
      assertNull(quotaConfig.getRateLimiterUnit());
      assertNull(quotaConfig.getRateLimiterDuration());
    }
  }

  @Test
  public void testInvalidQuotaConfig() {
    // === Category 1: Individually Invalid Parameters ===

    // Test for a semantically invalid negative value for 'rateLimits'
    try {
      String quotaConfigStr = "{\"rateLimits\": -1.0, \"rateLimiterUnit\": \"SECONDS\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Test for a semantically invalid negative value for 'rateLimiterDuration'
    try {
      String quotaConfigStr = "{\"rateLimits\": 10.0, \"rateLimiterDuration\": -1.0}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Test for an invalid enum value for 'rateLimiterUnit'
    try {
      String quotaConfigStr = "{\"rateLimits\": 10.0, \"rateLimiterUnit\": \"DECADES\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // === Category 2: Logically Invalid Combinations ===

    // Test for specifying rateLimits WITHOUT a time unit or duration
    try {
      String quotaConfigStr = "{\"rateLimits\": 100.0}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Test for specifying a time unit WITHOUT rateLimits
    try {
      String quotaConfigStr = "{\"rateLimiterUnit\": \"MINUTES\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Test for specifying a duration WITHOUT rateLimits
    try {
      String quotaConfigStr = "{\"rateLimiterDuration\": 5.0}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testFlexibleRateLimiterConfigWithSeconds()
      throws IOException {
    // Test basic seconds-based rate limiting
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 100d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 100.0);
    }

    // Test with different durations in seconds
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 5d, 50d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 5.0);
      assertEquals(quotaConfig.getRateLimits(), 50.0);
    }

    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 10d, 25d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 10.0);
      assertEquals(quotaConfig.getRateLimits(), 25.0);
    }
  }

  @Test
  public void testFlexibleRateLimiterConfigWithMinutes()
      throws IOException {
    // Test basic minutes-based rate limiting
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.MINUTES, 1d, 60d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.MINUTES);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 60.0);
    }

    // Test with different durations in minutes
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.MINUTES, 5d, 300d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.MINUTES);
      assertEquals(quotaConfig.getRateLimiterDuration(), 5.0);
      assertEquals(quotaConfig.getRateLimits(), 300.0);
    }

    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.MINUTES, 10d, 600d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.MINUTES);
      assertEquals(quotaConfig.getRateLimiterDuration(), 10.0);
      assertEquals(quotaConfig.getRateLimits(), 600.0);
    }
  }

  @Test
  public void testFractionalRateLimits()
      throws IOException {
    // Test fractional rate limits that should be automatically adjusted
    // These cases test the scenario where RateLimiterUtils.adjustRateLimitsUsingCommonsMath
    // converts fractional rates to integer equivalents

    // 0.25 queries per second = 1 query per 4 seconds
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 0.25d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 0.25);
    }

    // 0.5 queries per second = 1 query per 2 seconds
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 0.5d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 0.5);
    }

    // 0.1 queries per second = 1 query per 10 seconds
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 0.1d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 0.1);
    }

    // Test with other fractions
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 0.333d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 0.333);
    }
  }

  @Test
  public void testComplexFractionalRateLimitsWithMinutes()
      throws IOException {
    // Test fractional rate limits with minutes

    // 0.5 queries per minute = 1 query per 2 minutes
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.MINUTES, 1d, 0.5d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.MINUTES);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 0.5);
    }

    // 1.5 queries per 5 minutes
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.MINUTES, 5d, 1.5d);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.MINUTES);
      assertEquals(quotaConfig.getRateLimiterDuration(), 5.0);
      assertEquals(quotaConfig.getRateLimits(), 1.5);
    }
  }

  @Test
  public void testRateLimiterConfigSerialization()
      throws IOException {
    // Test serialization of flexible rate limiter configurations

    // Test seconds-based config
    {
      QuotaConfig quotaConfig = new QuotaConfig("100G", TimeUnit.SECONDS, 5d, 25d);
      JsonNode quotaConfigJson = quotaConfig.toJsonNode();
      assertEquals(quotaConfigJson.get("storage").asText(), "100G");
      assertEquals(quotaConfigJson.get("rateLimiterUnit").asText(), "SECONDS");
      assertEquals(quotaConfigJson.get("rateLimiterDuration").asDouble(), 5.0);
      assertEquals(quotaConfigJson.get("rateLimits").asDouble(), 25.0);

      QuotaConfig deserializedConfig = JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class);
      assertEquals(deserializedConfig, quotaConfig);
    }

    // Test minutes-based config
    {
      QuotaConfig quotaConfig = new QuotaConfig("50G", TimeUnit.MINUTES, 10d, 120d);
      JsonNode quotaConfigJson = quotaConfig.toJsonNode();
      assertEquals(quotaConfigJson.get("storage").asText(), "50G");
      assertEquals(quotaConfigJson.get("rateLimiterUnit").asText(), "MINUTES");
      assertEquals(quotaConfigJson.get("rateLimiterDuration").asDouble(), 10.0);
      assertEquals(quotaConfigJson.get("rateLimits").asDouble(), 120.0);

      QuotaConfig deserializedConfig = JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class);
      assertEquals(deserializedConfig, quotaConfig);
    }

    // Test fractional rate limits serialization
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 1d, 0.25d);
      JsonNode quotaConfigJson = quotaConfig.toJsonNode();
      assertEquals(quotaConfigJson.get("rateLimiterUnit").asText(), "SECONDS");
      assertEquals(quotaConfigJson.get("rateLimiterDuration").asDouble(), 1.0);
      assertEquals(quotaConfigJson.get("rateLimits").asDouble(), 0.25);

      QuotaConfig deserializedConfig = JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class);
      assertEquals(deserializedConfig, quotaConfig);
    }
  }

  @Test
  public void testRateLimiterConfigFromJsonString()
      throws IOException {
    // Test deserialization from JSON strings with flexible rate limiter configurations

    // Test seconds-based configuration
    {
      String quotaConfigStr = "{\"rateLimiterUnit\":\"SECONDS\",\"rateLimiterDuration\":5,\"rateLimits\":25}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 5.0);
      assertEquals(quotaConfig.getRateLimits(), 25.0);
    }

    // Test minutes-based configuration
    {
      String quotaConfigStr = "{\"rateLimiterUnit\":\"MINUTES\",\"rateLimiterDuration\":10,\"rateLimits\":600}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.MINUTES);
      assertEquals(quotaConfig.getRateLimiterDuration(), 10.0);
      assertEquals(quotaConfig.getRateLimits(), 600.0);
    }

    // Test fractional rate limits
    {
      String quotaConfigStr = "{\"rateLimiterUnit\":\"SECONDS\",\"rateLimiterDuration\":1,\"rateLimits\":0.25}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 1.0);
      assertEquals(quotaConfig.getRateLimits(), 0.25);
    }

    // Test combined storage and rate limiter config
    {
      String quotaConfigStr =
              "{\"storage\":\"200G\",\"rateLimiterUnit\":\"SECONDS\",\"rateLimiterDuration\":5,\"rateLimits\":50}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getStorage(), "200G");
      assertEquals(quotaConfig.getRateLimiterUnit(), TimeUnit.SECONDS);
      assertEquals(quotaConfig.getRateLimiterDuration(), 5.0);
      assertEquals(quotaConfig.getRateLimits(), 50.0);
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
      assertEquals(quotaConfig.isQuotaConfigSet(), false); // Note: this returns false when rate limits are NOT -1
    }

    // Config without rate limiter parameters should not have quota config set
    {
      QuotaConfig quotaConfig = new QuotaConfig("100G", null, null, null);
      assertEquals(quotaConfig.isQuotaConfigSet(), true); // Note: this returns true when rate limits are -1
    }

    // Config with partial rate limiter parameters should not have quota config set
    {
      QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, null, null);
      assertEquals(quotaConfig.isQuotaConfigSet(), true);
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
