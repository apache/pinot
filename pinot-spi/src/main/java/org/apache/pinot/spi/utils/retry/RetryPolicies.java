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
package org.apache.pinot.spi.utils.retry;

/**
 * Factory for retry policies.
 */
public class RetryPolicies {
  private RetryPolicies() {
  }

  /**
   * Creates an {@link ExponentialBackoffRetryPolicy}.
   *
   * @param maxNumAttempts The maximum number of attempts to try
   * @param initialDelayMs The initial delay in milliseconds between attempts
   * @param delayScaleFactor The factor used for exponential scaling of delay
   * @return The retry policy
   */
  public static ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy(int maxNumAttempts, long initialDelayMs, double delayScaleFactor) {
    return new ExponentialBackoffRetryPolicy(maxNumAttempts, initialDelayMs, delayScaleFactor);
  }

  /**
   * Creates a {@link FixedDelayRetryPolicy}.
   *
   * @param maxNumAttempts The maximum number of attempts to try
   * @param delayMs The delay in milliseconds between attempts
   * @return The retry policy
   */
  public static FixedDelayRetryPolicy fixedDelayRetryPolicy(int maxNumAttempts, long delayMs) {
    return new FixedDelayRetryPolicy(maxNumAttempts, delayMs);
  }

  /**
   * Creates a {@link RandomDelayRetryPolicy}.
   *
   * @param maxNumAttempts The maximum number of attempts to try
   * @param minDelayMs The min delay in milliseconds between attempts (inclusive)
   * @param maxDelayMs The max delay in milliseconds between attempts (exclusive)
   * @return The retry policy
   */
  public static RandomDelayRetryPolicy randomDelayRetryPolicy(int maxNumAttempts, long minDelayMs, long maxDelayMs) {
    return new RandomDelayRetryPolicy(maxNumAttempts, minDelayMs, maxDelayMs);
  }

  /**
   * Creates a {@link NoDelayRetryPolicy}.
   *
   * @param maxNumAttempts The maximum number of attempts to try
   * @return The retry policy
   */
  public static NoDelayRetryPolicy noDelayRetryPolicy(int maxNumAttempts) {
    return new NoDelayRetryPolicy(maxNumAttempts);
  }
}
