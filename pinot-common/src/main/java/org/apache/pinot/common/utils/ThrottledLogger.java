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
      return 0;
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
}
