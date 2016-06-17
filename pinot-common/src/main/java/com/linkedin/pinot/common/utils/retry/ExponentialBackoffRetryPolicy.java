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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.Utils;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


/**
 * Exponential backoff retry policy, see {@link RetryPolicies#exponentialBackoffRetryPolicy(int, long, float)}.
 *
 * @author jfim
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
  private final int _maximumAttemptCount;
  private final long _minimumMilliseconds;
  private final float _retryScaleFactor;

  public ExponentialBackoffRetryPolicy(int maximumAttemptCount, long minimumMilliseconds, float retryScaleFactor) {
    _maximumAttemptCount = maximumAttemptCount;
    _minimumMilliseconds = minimumMilliseconds;
    _retryScaleFactor = retryScaleFactor;
  }

  @Override
  public boolean attempt(Callable<Boolean> operation) {
    try {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      int remainingAttempts = _maximumAttemptCount - 1;
      long minimumSleepTime = _minimumMilliseconds;
      long maximumSleepTime = (long) (minimumSleepTime * _retryScaleFactor);

      boolean result = operation.call();

      while ((!result) && (0 < remainingAttempts)) {
        long sleepTime = random.nextLong(minimumSleepTime, maximumSleepTime);

        Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);

        result = operation.call();

        remainingAttempts--;
        minimumSleepTime *= _retryScaleFactor;
        maximumSleepTime *= _retryScaleFactor;
      }

      return result;
    } catch (Exception e) {
      Utils.rethrowException(e);
      return false;
    }
  }
}
