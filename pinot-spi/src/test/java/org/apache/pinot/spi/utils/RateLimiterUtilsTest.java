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
  public void testFractionalRateAdjustmentInSeconds() {
    // Test 0.25 queries per second - should be adjusted to 1 query per 4 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 0.25);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(4));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // Test 0.5 queries per second - should be adjusted to 1 query per 2 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 0.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(2));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // Test 0.1 queries per second - should be adjusted to 1 query per 10 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 0.1);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(10));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // Test 0.2 queries per second - should be adjusted to 1 query per 5 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 0.2);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(5));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // Test 1.5 queries per second - should be adjusted to 3 queries per 2 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 1.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(2));
      assertEquals(config.getLimitForPeriod(), 3);
    }

    // Test 2.5 queries per second - should be adjusted to 5 queries per 2 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 2.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(2));
      assertEquals(config.getLimitForPeriod(), 5);
    }
  }

  @Test
  public void testFractionalRateAdjustmentInMinutes() {
    // Test 0.5 queries per minute - should be adjusted to 1 query per 2 minutes
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 1.0, 0.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(2));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // Test 0.25 queries per minute - should be adjusted to 1 query per 4 minutes
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 1.0, 0.25);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(4));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // Test 1.5 queries per minute - should be adjusted to 3 queries per 2 minutes
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 1.0, 1.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(2));
      assertEquals(config.getLimitForPeriod(), 3);
    }
  }

  @Test
  public void testComplexFractionalRateAdjustmentWithDuration() {
    // Test fractional rates with non-unit durations

    // 0.5 queries per 5 seconds - should be adjusted to 1 query per 10 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 5.0, 0.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(10));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // 1.25 queries per 2 seconds - should be adjusted to 5 queries per 8 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 2.0, 1.25);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(8));
      assertEquals(config.getLimitForPeriod(), 5);
    }

    // 0.75 queries per 4 seconds - should be adjusted to 3 queries per 16 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 4.0, 0.75);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(16));
      assertEquals(config.getLimitForPeriod(), 3);
    }
  }

  @Test
  public void testComplexFractionalRateAdjustmentWithMinutesAndDuration() {
    // Test fractional rates with minutes and non-unit durations

    // 0.5 queries per 5 minutes - should be adjusted to 1 query per 10 minutes
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 5.0, 0.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(10));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // 1.5 queries per 2 minutes - should be adjusted to 3 queries per 4 minutes
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 2.0, 1.5);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(4));
      assertEquals(config.getLimitForPeriod(), 3);
    }

    // 0.25 queries per 10 minutes - should be adjusted to 1 query per 40 minutes
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.MINUTES, 10.0, 0.25);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofMinutes(40));
      assertEquals(config.getLimitForPeriod(), 1);
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
  public void testVerySmallFractionalRates() {
    // Test very small fractional rates

    // 0.01 queries per second - should be adjusted to 1 query per 100 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 0.01);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(100));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // 0.001 queries per second - should be adjusted to 1 query per 1000 seconds
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 0.001);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(1000));
      assertEquals(config.getLimitForPeriod(), 1);
    }
  }

  @Test
  public void testCommonFractionalScenarios() {
    // Test common real-world fractional scenarios

    // 1 query every 3 seconds (0.333... qps)
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 1.0/3.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(3));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // 1 query every 30 seconds (0.033... qps)
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 1.0/30.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(30));
      assertEquals(config.getLimitForPeriod(), 1);
    }

    // 2 queries every 3 seconds (0.666... qps)
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.SECONDS, 1.0, 2.0/3.0);
      assertEquals(config.getLimitRefreshPeriod(), Duration.ofSeconds(3));
      assertEquals(config.getLimitForPeriod(), 2);
    }
  }

  @Test
  public void testDefaultTimeUnitFallback() {
    // Test unsupported time unit falls back to default
    {
      RateLimiterConfig config = RateLimiterUtils.createRateLimiterConfig(TimeUnit.HOURS, 1.0, 10.0);
      // Should return default config - testing that it doesn't crash
      assertEquals(config.getTimeoutDuration(), Duration.ofMillis(500)); // Default timeout
    }
  }
}
