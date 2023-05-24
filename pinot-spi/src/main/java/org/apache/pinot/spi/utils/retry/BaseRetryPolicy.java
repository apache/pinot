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

import java.util.concurrent.Callable;


/**
 * The {@link BaseRetryPolicy} is the base class for all retry policies. To implement a new retry policy, extends
 * this class and implements the method {@link #getDelayMs(int)}.
 * <p>NOTE: All the retry policies should be stateless so that they can be cached and reused.
 */
public abstract class BaseRetryPolicy implements RetryPolicy {
  private final int _maxNumAttempts;

  protected BaseRetryPolicy(int maxNumAttempts) {
    _maxNumAttempts = maxNumAttempts;
  }

  /**
   * Gets the delay in milliseconds before the next attempt.
   *
   * @param currentAttempt Current attempt number
   * @return Delay in milliseconds
   */
  protected abstract long getDelayMs(int currentAttempt);

  @Override
  public int attempt(Callable<Boolean> operation)
      throws AttemptsExceededException, RetriableOperationException {
    int attempt = 0;
    try {
      while (attempt < _maxNumAttempts - 1) {
        if (Boolean.TRUE.equals(operation.call())) {
          // Succeeded
          return attempt;
        } else {
          // Failed
          Thread.sleep(getDelayMs(attempt++));
        }
      }
      if (_maxNumAttempts > 0 && Boolean.TRUE.equals(operation.call())) {
        // Succeeded
        return attempt;
      }
    } catch (Exception e) {
      throw new RetriableOperationException(e, attempt + 1);
    }
    throw new AttemptsExceededException("Operation failed after " + _maxNumAttempts + " attempts", _maxNumAttempts);
  }
}
