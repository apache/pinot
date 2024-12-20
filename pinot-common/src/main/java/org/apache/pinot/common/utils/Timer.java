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

/**
 * Utility class that works with a timeout in milliseconds and provides methods to check remaining time and expiration.
 */
public class Timer {
  private final long _timeoutMillis;
  private final long _startTime;

  /**
   * Initializes the Timer with the specified timeout in milliseconds.
   *
   * @param timeoutMillis the timeout duration in milliseconds
   */
  public Timer(long timeoutMillis) {
    _timeoutMillis = timeoutMillis;
    _startTime = System.currentTimeMillis();
  }

  /**
   * Returns the remaining time in milliseconds. If the timeout has expired, it returns 0.
   *
   * @return the remaining time in milliseconds
   */
  public long getRemainingTime() {
    long elapsedTime = System.currentTimeMillis() - _startTime;
    long remainingTime = _timeoutMillis - elapsedTime;
    return Math.max(remainingTime, 0);
  }

  /**
   * Checks if the timer has expired.
   *
   * @return true if the timer has expired, false otherwise
   */
  public boolean hasExpired() {
    return getRemainingTime() == 0;
  }
}
