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

import java.util.function.LongSupplier;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class RateLimiterTest {

  /**
   * Helper class to control time in tests for precise time-based assertions.
   */
  private static class ControllableTimeSource implements LongSupplier {
    private long _currentNanos;

    ControllableTimeSource(long initialNanos) {
      _currentNanos = initialNanos;
    }

    @Override
    public long getAsLong() {
      return _currentNanos;
    }

    public void advanceSeconds(long seconds) {
      _currentNanos += seconds * 1_000_000_000L;
    }
  }

  @Test
  public void testRateLimiterInitialCapacity() {
    ControllableTimeSource timeSource = new ControllableTimeSource(0);
    RateLimiter limiter = new RateLimiter(5.0, timeSource);

    boolean[] results = new boolean[6];
    for (int i = 0; i < 6; i++) {
      results[i] = limiter.tryAcquire();
    }

    assertThat(results[0]).describedAs("First acquire should succeed").isTrue();
    assertThat(results[1]).describedAs("Second acquire should succeed").isTrue();
    assertThat(results[2]).describedAs("Third acquire should succeed").isTrue();
    assertThat(results[3]).describedAs("Fourth acquire should succeed").isTrue();
    assertThat(results[4]).describedAs("Fifth acquire should succeed").isTrue();
    assertThat(results[5]).describedAs("Sixth acquire should fail").isFalse();
  }

  @Test
  public void testRateLimiterRejectIncrementsDropCount() {
    ControllableTimeSource timeSource = new ControllableTimeSource(0);
    RateLimiter limiter = new RateLimiter(2.0, timeSource);

    limiter.tryAcquire();
    limiter.tryAcquire();

    for (int i = 0; i < 10; i++) {
      boolean result = limiter.tryAcquire();
      assertThat(result).describedAs("Acquire should fail when tokens exhausted").isFalse();
    }

    long droppedCount = limiter.getAndResetDroppedCount();
    assertThat(droppedCount).isEqualTo(10);
  }

  @Test
  public void testRateLimiterDropCountResets() {
    ControllableTimeSource timeSource = new ControllableTimeSource(0);
    RateLimiter limiter = new RateLimiter(5.0, timeSource);

    for (int i = 0; i < 5; i++) {
      limiter.tryAcquire();
    }

    for (int i = 0; i < 100; i++) {
      assertThat(limiter.tryAcquire()).isFalse();
    }

    long droppedBeforeRefill = limiter.getAndResetDroppedCount();
    assertThat(droppedBeforeRefill).isEqualTo(100);

    long secondCall = limiter.getAndResetDroppedCount();
    assertThat(secondCall).isEqualTo(0);

    timeSource.advanceSeconds(60);
    for (int i = 0; i < 5; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
    }
    assertThat(limiter.tryAcquire()).isFalse();

    long droppedAfterRefill = limiter.getAndResetDroppedCount();
    assertThat(droppedAfterRefill).isEqualTo(1);
  }

  @Test
  public void testRateLimiterRefillsTokensOverTime() {
    ControllableTimeSource timeSource = new ControllableTimeSource(0);
    RateLimiter limiter = new RateLimiter(10.0, timeSource);

    for (int i = 0; i < 10; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
    }
    assertThat(limiter.tryAcquire()).isFalse();

    timeSource.advanceSeconds(6);
    assertThat(limiter.tryAcquire()).isTrue();

    timeSource.advanceSeconds(120);
    for (int i = 0; i < 10; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
    }
    assertThat(limiter.tryAcquire()).isFalse();
  }

  @Test
  public void testRateLimiterZeroTimeDelta() {
    ControllableTimeSource timeSource = new ControllableTimeSource(0);
    RateLimiter limiter = new RateLimiter(10.0, timeSource);

    int successCount = 0;
    for (int i = 0; i < 20; i++) {
      if (limiter.tryAcquire()) {
        successCount++;
      }
    }

    assertThat(successCount).isEqualTo(10);
    assertThat(limiter.getAndResetDroppedCount()).isEqualTo(10);
  }
}
