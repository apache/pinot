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
import java.util.concurrent.TimeUnit;


/**
 * Fixed delay retry policy, see {@link RetryPolicies#fixedDelayRetryPolicy(int, long)}.
 */
public class FixedDelayRetryPolicy implements RetryPolicy {
  public FixedDelayRetryPolicy(int attemptCount, long sleepTimeMillis) {
    _attemptCount = attemptCount;
    _sleepTimeMillis = sleepTimeMillis;
  }

  private int _attemptCount;
  private long _sleepTimeMillis;

  @Override
  public boolean attempt(Callable<Boolean> operation) {
    try {
      int remainingAttempts = _attemptCount - 1;
      boolean result = operation.call();

      while (!result && 0 < remainingAttempts) {
        Uninterruptibles.sleepUninterruptibly(_sleepTimeMillis, TimeUnit.MILLISECONDS);

        result = operation.call();
        remainingAttempts--;
      }

      return result;
    } catch (Exception e) {
      Utils.rethrowException(e);
      return false;
    }
  }
}
