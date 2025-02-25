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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A delegator executor service that sets MDC context for the query.
 *
 * By using this executor, all tasks submitted to the executor will have the MDC context set to the query context.
 * This is easier and safer to apply than setting the MDC context manually for each task.
 * You can read more about MDC in <a href="https://www.baeldung.com/mdc-in-log4j-2-logback">this baeldung post</a>
 *
 * TODO: Convert this class and its usages into an Executor instead of an ExecutorService
 *
 */
public abstract class MdcExecutor extends DecoratorExecutorService {

  /**
   * The logger for the class.
   *
   * Notice this is using the name of the final class as the logger name.
   */
  private final Logger _logger = LoggerFactory.getLogger(this.getClass());

  public MdcExecutor(ExecutorService executorService) {
    super(executorService);
  }

  /**
   * Get the logger for the class
   */
  protected Logger getLogger() {
    return _logger;
  }

  /**
   * Check if the MDC context is already set.
   *
   * This indicates that the MDC context may have been leaked from a previous task, which would mean incorrect logging
   * while tasks are executed by this executor or after they finish. Therefore if this returns true a warning will be
   * logged and the MDC context will not be modified
   */
  protected abstract boolean alreadyRegistered();

  /**
   * Register the MDC context for the query.
   */
  protected abstract void registerInMdc();

  /**
   * Unregister the MDC context for the query.
   */
  protected abstract void unregisterFromMdc();

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    return () -> {
      if (alreadyRegistered()) {
        getLogger().warn("MDC context already set. This should not happen.");
        return task.call();
      }
      try {
        registerInMdc();
        return task.call();
      } finally {
        unregisterFromMdc();
      }
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    return () -> {
      if (alreadyRegistered()) {
        task.run();
        return;
      }
      try {
        registerInMdc();
        task.run();
      } finally {
        unregisterFromMdc();
      }
    };
  }
}
