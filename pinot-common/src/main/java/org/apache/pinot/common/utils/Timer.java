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

import java.util.concurrent.TimeUnit;

/**
 * Utility class that works with a timeout in milliseconds and provides methods to check remaining time and expiration.
 */
public class Timer {
  private final long _startNs;
  private final long _deadlineNs;
  private final ClockNs _clock;

  public Timer(Long timeout, TimeUnit timeUnit) {
    this(System::nanoTime, timeout, timeUnit);
  }

  public Timer(ClockNs clock, Long timeout, TimeUnit timeUnit) {
    _clock = clock;
    _startNs = _clock.nanos();
    _deadlineNs = timeUnit.toNanos(timeout) + _clock.nanos();
  }

  /**
   * Returns the remaining time in milliseconds. If the timeout has expired, it returns 0.
   *
   * @return the remaining time in milliseconds
   */
  public long getRemainingTimeMs() {
    long remainingNs = _deadlineNs - _clock.nanos();
    return Math.max(remainingNs / 1_000_000, 0);
  }

  /**
   * Checks if the timer has expired.
   *
   * @return true if the timer has expired, false otherwise
   */
  public boolean hasExpired() {
    return getRemainingTimeMs() == 0;
  }

  public long timeElapsed(TimeUnit timeUnit) {
    return timeUnit.convert(_clock.nanos() - _startNs, TimeUnit.NANOSECONDS);
  }

  public interface ClockNs {
    long nanos();
  }
}
