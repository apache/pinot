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
package org.apache.pinot.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;


/**
 * Rate-limited exception logger that prevents log flooding while maintaining visibility into errors.
 *
 * <p><b>Key Features:</b>
 * <ul>
 * <li>Class-based fingerprinting: Each exception class gets its own rate limiter</li>
 * <li>Suppression tracking: Reports count of dropped logs when rate limit is lifted</li>
 * <li>Zero Dependencies: Uses a lightweight in-house Token Bucket implementation</li>
 * </ul>
 *
 * <p><b>Note:</b> This class is designed for single-threaded access per instance.
 */
@NotThreadSafe
public class ThrottledLogger {
  private final Logger _delegate;
  private final Map<Class<?>, RateLimiter> _rateLimiterMap = new HashMap<>();
  private final double _rateLimitPerMin;

  public ThrottledLogger(Logger delegate, @Nullable IngestionConfig ingestionConfig) {
    this(delegate, getRateLimitPerMin(ingestionConfig));
  }

  public ThrottledLogger(Logger delegate, double rateLimitPerMin) {
    _delegate = delegate;
    _rateLimitPerMin = rateLimitPerMin;
  }

  private static double getRateLimitPerMin(@Nullable IngestionConfig ingestionConfig) {
    if (ingestionConfig == null) {
      return CommonConstants.IngestionConfigs.DEFAULT_INGESTION_EXCEPTION_LOG_RATE_LIMIT_PER_MIN;
    }
    return ingestionConfig.getIngestionExceptionLogRateLimitPerMin();
  }

  public void warn(String msg, Throwable t) {
    logWithRateLimit(msg, t, _delegate::warn);
  }

  public void error(String msg, Throwable t) {
    logWithRateLimit(msg, t, _delegate::error);
  }

  private void logWithRateLimit(String msg, Throwable t, BiConsumer<String, Throwable> consumer) {
    if (_rateLimitPerMin <= 0) {
      _delegate.debug(msg, t);
      return;
    }

    Class<?> exceptionClass = t.getClass();
    RateLimiter limiter = _rateLimiterMap.computeIfAbsent(exceptionClass, k -> new RateLimiter(_rateLimitPerMin));

    // tryAcquire now automatically handles incrementing the drop count if it returns false
    if (limiter.tryAcquire()) {
      // If we successfully acquired, check if we had previously dropped logs
      long droppedCount = limiter.getAndResetDroppedCount();
      if (droppedCount > 0) {
        consumer.accept(String.format("Dropped %d occurrences of %s",
            droppedCount,
            exceptionClass.getSimpleName()), null);
      }
      consumer.accept(msg, t);
    } else {
      // If acquire failed, the limiter has already incremented its internal dropped counter.
      // We still debug log so the data isn't completely lost.
      _delegate.debug(msg, t);
    }
  }

  /**
   * A highly performant, non-thread-safe Token Bucket implementation.
   * It tracks its own dropped count to avoid wrapper object allocations.
   */
  @NotThreadSafe
  private static class RateLimiter {
    private final double _capacity;
    private final double _tokensPerNano;

    private double _tokens;
    private long _lastRefillNano;
    private long _droppedCount;

    RateLimiter(double rateLimitPerMin) {
      _capacity = rateLimitPerMin;
      _tokens = rateLimitPerMin;
      _tokensPerNano = rateLimitPerMin / 60_000_000_000.0;
      _lastRefillNano = System.nanoTime();
      _droppedCount = 0;
    }

    /**
     * Attempts to consume a token.
     * Refills tokens based on elapsed time.
     * @return true if token acquired, false if limit exceeded (and increments dropped count)
     */
    boolean tryAcquire() {
      long now = System.nanoTime();

      // 1. Refill
      long delta = now - _lastRefillNano;
      if (delta > 0) {
        double newTokens = delta * _tokensPerNano;
        _tokens = Math.min(_capacity, _tokens + newTokens);
        _lastRefillNano = now;
      }

      // 2. Acquire
      if (_tokens >= 1.0) {
        _tokens -= 1.0;
        return true;
      }

      // 3. Reject & Track
      _droppedCount++;
      return false;
    }

    /**
     * Returns the number of logs dropped since the last successful acquire,
     * and resets the counter to zero.
     */
    long getAndResetDroppedCount() {
      if (_droppedCount == 0) {
        return 0;
      }
      long count = _droppedCount;
      _droppedCount = 0;
      return count;
    }
  }
}
