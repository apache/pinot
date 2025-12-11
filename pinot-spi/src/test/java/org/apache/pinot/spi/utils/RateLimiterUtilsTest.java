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
package org.apache.pinot.spi.utils;

import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class RateLimiterUtilsTest {

  @Test
  public void testCreateDefaultSecondLevelRateLimiterConfig() {
    RateLimiterConfig config = RateLimiterUtils.createDefaultSecondLevelRateLimiterConfig(100);
    assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(1));
    assertEquals(config.getLimitForPeriod(), 100);
    assertEquals(config.getTimeoutDuration(), Duration.ZERO);
  }

  @Test
  public void testCreateRateLimiterConfigWithSeconds() {
    // Test basic seconds configuration
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 100.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(1));
      assertEquals(config.getLimitForPeriod(), 100);
    }

    // Test with different duration
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 5.0, 50.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(5));
      assertEquals(config.getLimitForPeriod(), 50);
    }
  }

  @Test
  public void testCreateRateLimiterConfigWithMinutes() {
    // Test basic minutes configuration
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 1.0, 60.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(1));
      assertEquals(config.getLimitForPeriod(), 60);
    }

    // Test with different duration
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 10.0, 600.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(10));
      assertEquals(config.getLimitForPeriod(), 600);
    }
  }

  @Test
  public void testFractionalRateAdjustments() {
    // Combined test for various fractional rate adjustments
    Object[][] testCases = {
        // Seconds-based fractional adjustments
        {TimeUnit.SECONDS, 1.0, 0.25, 4, 1, "0.25 qps -> 1 query per 4 seconds"},
        {TimeUnit.SECONDS, 1.0, 0.5, 2, 1, "0.5 qps -> 1 query per 2 seconds"},
        {TimeUnit.SECONDS, 1.0, 0.1, 10, 1, "0.1 qps -> 1 query per 10 seconds"},
        {TimeUnit.SECONDS, 1.0, 0.2, 5, 1, "0.2 qps -> 1 query per 5 seconds"},
        {TimeUnit.SECONDS, 1.0, 1.5, 2, 3, "1.5 qps -> 3 queries per 2 seconds"},
        {TimeUnit.SECONDS, 1.0, 2.5, 2, 5, "2.5 qps -> 5 queries per 2 seconds"},

        // Minutes-based fractional adjustments
        {TimeUnit.MINUTES, 1.0, 0.5, 2, 1, "0.5 qpm -> 1 query per 2 minutes"},
        {TimeUnit.MINUTES, 1.0, 0.25, 4, 1, "0.25 qpm -> 1 query per 4 minutes"},
        {TimeUnit.MINUTES, 1.0, 1.5, 2, 3, "1.5 qpm -> 3 queries per 2 minutes"}
    };

    for (Object[] testCase : testCases) {
      TimeUnit unit = (TimeUnit) testCase[0];
      Double duration = (Double) testCase[1];
      Double rate = (Double) testCase[2];
      int expectedPeriod = (Integer) testCase[3];
      int expectedLimit = (Integer) testCase[4];
      String description = (String) testCase[5];

      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(unit, duration, rate);

      if (unit == TimeUnit.SECONDS) {
        assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(expectedPeriod),
            "Period mismatch for: " + description);
      } else {
        assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(expectedPeriod),
            "Period mismatch for: " + description);
      }
      assertEquals(config.getLimitForPeriod(), expectedLimit, "Limit mismatch for: " + description);
    }
  }







  @Test
  public void testIntegerRatesNoAdjustment() {
    // Test that integer rates don't get adjusted

    // Integer rates should remain unchanged
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 10.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(1));
      assertEquals(config.getLimitForPeriod(), 10);
    }

    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 5.0, 100.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(5));
      assertEquals(config.getLimitForPeriod(), 100);
    }
  }



  @Test
  public void testDefaultTimeUnitFallback() {
    // Test unsupported time unit falls back to default
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.HOURS, 1.0, 10.0);
      // Should return default config - testing that it doesn't crash
      // Actual default timeout from RateLimiterConfig.ofDefaults()
      assertEquals(config.getTimeoutDuration(), Duration.ofSeconds(5));
    }
  }
}
