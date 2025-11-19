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

import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.slf4j.Logger;


/**
 * Rate-limited exception logger that prevents log flooding while maintaining visibility into errors.
 *
 * <p>This utility wraps an SLF4J logger and applies per-exception-class rate limiting. Unlike global rate limiting
 * which can suffer from the "Noisy Neighbor" problem (high-frequency errors consuming all log quota and starving
 * low-frequency critical errors), this implementation maintains independent rate limiters for each exception class.
 *
 * <p><b>Key Features:</b>
 * <ul>
 *   <li>Class-based fingerprinting: Each exception class gets its own rate limiter</li>
 *   <li>Suppression tracking: Reports count of dropped logs when rate limit is lifted</li>
 *   <li>Non-blocking: Uses atomic operations, no locks in hot path</li>
 *   <li>Thread-safe: ConcurrentHashMap ensures safe concurrent access</li>
 *   <li>Bounded memory: Exception classes are finite (~10-50 typical)</li>
 * </ul>
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * Logger logger = LoggerFactory.getLogger(MyClass.class);
 * PinotThrottledLogger throttled = new PinotThrottledLogger(logger, ingestionConfig, tableName);
 *
 * try {
 *   // some operation
 * } catch (Exception e) {
 *   throttled.warn("Operation failed for record: " + record, e);
 * }
 * </pre>
 *
 * <p><b>Backward Compatibility:</b>
 * When rate limit is 0 (default), falls back to DEBUG level logging to maintain backward compatible behavior.
 *
 * <p><b>Example Output:</b>
 * <pre>
 * WARN  [MyClass] Operation failed for record: {id=1}
 * java.lang.NumberFormatException: For input string: "abc"
 *     at java.lang.NumberFormatException.forInputString(...)
 *
 * [... 4 more similar logs within 1 minute ...]
 *
 * [After rate limit window passes and 10,001st exception occurs]
 * WARN  [MyClass] ... Suppressed 9995 occurrences of NumberFormatException ...
 * WARN  [MyClass] Operation failed for record: {id=10001}
 * java.lang.NumberFormatException: For input string: "xyz"
 * </pre>
 *
 * <p>Meanwhile, if a different exception type occurs (e.g., ConnectException), it logs immediately using its own
 * independent rate limiter, ensuring critical errors are never starved by high-frequency errors.
 *
 * @see org.apache.pinot.spi.config.table.ingestion.IngestionConfig#getIngestionExceptionLogRateLimitPerMin()
 */
public class PinotThrottledLogger {
  private final Logger _delegate;

  private final ConcurrentHashMap<Class<?>, RateLimiter> _rateLimiterMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Class<?>, AtomicLong> _droppedCountMap = new ConcurrentHashMap<>();
  private final double _permitsPerSecond;
  private final String _tableName;

  public PinotThrottledLogger(Logger delegate, @Nullable IngestionConfig ingestionConfig, @Nullable String tableName) {
    this(delegate, getPermitsPerSecond(ingestionConfig), tableName);
  }

  public PinotThrottledLogger(Logger delegate, double permitsPerSecond) {
    this(delegate, permitsPerSecond, null);
  }

  public PinotThrottledLogger(Logger delegate, double permitsPerSecond, @Nullable String tableName) {
    _delegate = delegate;
    _permitsPerSecond = permitsPerSecond;
    _tableName = tableName;
  }

  private static double getPermitsPerSecond(IngestionConfig ingestionConfig) {
    return Optional.ofNullable(ingestionConfig).orElse(new IngestionConfig())
        .getIngestionExceptionLogRateLimitPerMin() / 60.0;
  }

  public void warn(String msg, Throwable t) {
    logWithRateLimit(msg, t, _delegate::warn);
  }

  public void error(String msg, Throwable t) {
    logWithRateLimit(msg, t, _delegate::error);
  }

  private void logWithRateLimit(String msg, Throwable t, BiConsumer<String, Throwable> consumer) {
    if (_permitsPerSecond <= 0) {
      _delegate.debug(msg, t);
      return;
    }

    Class<?> exceptionClass = t.getClass();

    RateLimiter limiter = _rateLimiterMap.computeIfAbsent(exceptionClass, k -> RateLimiter.create(_permitsPerSecond));
    AtomicLong droppedCount = _droppedCountMap.computeIfAbsent(exceptionClass, k -> new AtomicLong(0));

    if (limiter.tryAcquire()) {
      long suppressed = droppedCount.getAndSet(0);
      if (suppressed > 0) {
        String suppressionMsg =
            String.format("... Suppressed %d occurrences of %s ...", suppressed, exceptionClass.getSimpleName());
        consumer.accept(suppressionMsg, null);
      }
      consumer.accept(msg, t);
    } else {
      droppedCount.incrementAndGet();
      if (_tableName != null) {
        ServerMetrics.get().addMeteredTableValue(_tableName, ServerMeter.LOGS_DROPPED_BY_THROTTLED_LOGGER, 1L);
      }
    }
  }
}
