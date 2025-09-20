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
package org.apache.pinot.spi.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;


/**
 * An Executor that throttles task submission when the heap usage is critical.
 * Heap Usage level is obtained from {@link ThreadAccountant#throttleQuerySubmission()}.
 */
public class ThrottleOnCriticalHeapUsageExecutor extends DecoratorExecutorService {

  public ThrottleOnCriticalHeapUsageExecutor(ExecutorService executorService) {
    super(executorService);
  }

  protected void checkTaskAllowed() {
    if (QueryThreadContext.get().getAccountant().throttleQuerySubmission()) {
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException("Tasks throttled due to high heap usage.");
    }
  }

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      return task.call();
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      task.run();
    };
  }
}
