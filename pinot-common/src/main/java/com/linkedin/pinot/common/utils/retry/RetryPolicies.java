/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils.retry;

/**
 * Factory for retry policies.
 */
public class RetryPolicies {
  private RetryPolicies() {
  }

  /**
   * Returns an exponential backoff retry policy. The time between attempts for the <em>i</em><sup>th</sup> retry is
   * between retryScaleFactor<sup>i - 1</sup> * minimumDelayMillis and retryScaleFactor<sup>i</sup> * minimumDelayMillis.
   *
   * @param maximumAttemptCount The maximum number of attempts to try
   * @param minimumDelayMillis The minimum of milliseconds between tries
   * @param retryScaleFactor The factor used for exponential scaling.
   * @return The retry policy
   */
  public static ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy(
      int maximumAttemptCount,
      long minimumDelayMillis,
      float retryScaleFactor
  ) {
    return new ExponentialBackoffRetryPolicy(maximumAttemptCount, minimumDelayMillis, retryScaleFactor);
  }

  /**
   * Returns a fixed delay retry policy, where the time between attempts is the same between each attempt.
   *
   * @param maximumAttemptCount The maximum number of attempts to try
   * @param delayMillis The time to wait between tries, in milliseconds
   * @return The retry policy
   */
  public static FixedDelayRetryPolicy fixedDelayRetryPolicy(
      int maximumAttemptCount,
      long delayMillis
  ) {
    return new FixedDelayRetryPolicy(maximumAttemptCount, delayMillis);
  }

  /**
   * Returns a no delay retry policy, where operations are retried without waiting.
   *
   * @param maximumAttemptCount The maximum number of attempts to try
   * @return The retry policy
   */
  public static NoDelayRetryPolicy noDelayRetryPolicy(
      int maximumAttemptCount
  ) {
    return new NoDelayRetryPolicy(maximumAttemptCount);
  }
}
