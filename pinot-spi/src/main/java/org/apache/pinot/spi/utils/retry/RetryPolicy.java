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
 * Retry policy, encapsulating the logic needed to retry an operation until it succeeds.
 */
public interface RetryPolicy {

  /**
   * Attempts to do the operation until it succeeds, aborting if an exception is thrown by the operation or number of
   * attempts exhausted.
   *
   * @param operation The operation to attempt, which returns true on success and false on failure.
   * @throws AttemptsExceededException
   * @throws RetriableOperationException
   * @return the number of attempts used for the operation. 0 means the first try was successful.
   */
  int attempt(Callable<Boolean> operation)
      throws AttemptsExceededException, RetriableOperationException;
}
