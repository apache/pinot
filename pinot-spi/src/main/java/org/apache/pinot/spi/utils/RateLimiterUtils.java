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
import org.apache.commons.math3.fraction.Fraction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RateLimiterUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimiterUtils.class);
  private static final int DEFAULT_SECOND_DURATION = 1;
  private static final double EPSILON = 0.000001;

  private RateLimiterUtils() {
  }

  public static RateLimiterConfig createDefaultSecondLevelRateLimiterConfig(int queries) {
    return RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofSeconds(DEFAULT_SECOND_DURATION))
        .limitForPeriod(queries)
        .timeoutDuration(Duration.ZERO)
        .build();
  }

  public static RateLimiterConfig createRateLimiterConfig(TimeUnit rateLimiterUnit,
      double rateLimiterDuration, double balancedRateLimits) {
    int[] adjusted = adjustRateLimitsUsingCommonsMath(rateLimiterDuration, balancedRateLimits);
    switch (rateLimiterUnit) {
      case MINUTES:
        return RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofMinutes(adjusted[0]))
            .limitForPeriod(adjusted[1])
            .timeoutDuration(Duration.ZERO)
            .build();
      case SECONDS:
        return RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(adjusted[0]))
            .limitForPeriod(adjusted[1])
            .timeoutDuration(Duration.ZERO)
            .build();
      default:
        return RateLimiterConfig.ofDefaults();
    }
  }

  /**
   * Calculates adjusted rate limiter duration and rate limits.
   * @return an int array where index 0 is adjustedRateLimiterDuration and index 1 is adjustedRateLimits.
   */
  private static int[] adjustRateLimitsUsingCommonsMath(double rateLimiterDuration, double balancedRateLimits) {
    if (balancedRateLimits > 0 && Math.abs(balancedRateLimits - Math.round(balancedRateLimits)) > EPSILON) {
      try {
        Fraction rateFraction = new Fraction(balancedRateLimits, EPSILON, 10000);
        if (rateFraction.getDenominator() != 1) {
          int multiplier = rateFraction.getDenominator();

          if (multiplier > 0) {
            rateLimiterDuration = rateLimiterDuration * multiplier;
            balancedRateLimits = Math.round(balancedRateLimits * multiplier);
          }
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("fail to converte " + rateLimiterDuration
            + " and " + balancedRateLimits + ": " + e.getMessage());
      }
    }
    return new int[]{(int) rateLimiterDuration, (int) balancedRateLimits};
  }
}
