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

import com.google.common.annotations.VisibleForTesting;
import java.util.function.LongSupplier;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * A highly performant, non-thread-safe Token Bucket implementation.
 * It tracks its own dropped count to avoid wrapper object allocations.
 */
@NotThreadSafe
class RateLimiter {
  private final double _capacity;
  private final double _tokensPerNano;
  private final LongSupplier _nanoTimeSupplier;

  private double _tokens;
  private long _lastRefillNano;
  private long _droppedCount;

  RateLimiter(double rateLimitPerMin) {
    this(rateLimitPerMin, System::nanoTime);
  }

  @VisibleForTesting
  RateLimiter(double rateLimitPerMin, LongSupplier nanoTimeSupplier) {
    _capacity = rateLimitPerMin;
    _tokens = rateLimitPerMin;
    _tokensPerNano = rateLimitPerMin / 60_000_000_000.0;
    _nanoTimeSupplier = nanoTimeSupplier;
    _lastRefillNano = nanoTimeSupplier.getAsLong();
    _droppedCount = 0;
  }

  /**
   * Attempts to consume a token.
   * Refills tokens based on elapsed time.
   * @return true if token acquired, false if limit exceeded (and increments dropped count)
   */
  boolean tryAcquire() {
    long now = _nanoTimeSupplier.getAsLong();

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
