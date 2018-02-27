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

import java.util.concurrent.Callable;


public abstract class BaseRetryPolicy implements RetryPolicy {
  private final int _maxNumAttempts;

  protected BaseRetryPolicy(int maxNumAttempts) {
    _maxNumAttempts = maxNumAttempts;
  }

  /**
   * Get the delay in milliseconds before the next attempt.
   *
   * @return Delay in milliseconds
   */
  protected abstract long getNextDelayMs();

  @Override
  public void attempt(Callable<Boolean> operation) throws AttemptsExceededException, RetriableOperationException {
    int numAttempts = 0;
    while (numAttempts < _maxNumAttempts) {
      try {
        if (operation.call()) {
          // Succeeded
          return;
        } else {
          // Failed
          numAttempts++;
          Thread.sleep(getNextDelayMs());
        }
      } catch (Exception e) {
        throw new RetriableOperationException(e);
      }
    }
    throw new AttemptsExceededException("Operation failed after " + _maxNumAttempts + " attempts");
  }
}
