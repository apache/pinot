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
package org.apache.pinot.core.query.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.executor.DecoratorExecutorService;

/**
 * An Executor that allows a maximum of tasks running at the same time, rejecting immediately any excess.
 */
public class MaxTasksExecutor extends DecoratorExecutorService {

  private final AtomicInteger _running;
  private final int _max;

  public MaxTasksExecutor(int max, ExecutorService executorService) {
    super(executorService);
    _running = new AtomicInteger(0);
    _max = max;
  }

  protected void checkTaskAllowed() {
    if (_running.get() >= _max) {
      throw new IllegalStateException("Exceeded maximum number of tasks");
    }
  }

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      try {
        _running.getAndIncrement();
        return task.call();
      } finally {
        _running.decrementAndGet();
      }
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      try {
        _running.getAndIncrement();
        task.run();
      } finally {
        _running.decrementAndGet();
      }
    };
  }
}
