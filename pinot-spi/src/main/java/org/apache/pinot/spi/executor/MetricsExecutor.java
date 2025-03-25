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

import com.google.common.base.Preconditions;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.spi.metrics.PinotMeter;


/**
 * An executor that keeps track of the number of tasks that have been started and finished
 */
public class MetricsExecutor extends DecoratorExecutorService {

  private final PinotMeter _startedTasks;
  private final PinotMeter _completedTasks;

  public MetricsExecutor(ExecutorService executorService, PinotMeter startedTasks, PinotMeter completedTasks) {
    super(executorService);
    // These tests are added to fail fast in case null meters are sent, which is normal in tests when using mocks.
    Preconditions.checkNotNull(startedTasks, "Started tasks meter should not be null");
    Preconditions.checkNotNull(completedTasks, "Completed tasks meter should not be null");
    _startedTasks = startedTasks;
    _completedTasks = completedTasks;
  }

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    return () -> {
      _startedTasks.mark();
      try {
        return task.call();
      } finally {
        _completedTasks.mark();
      }
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    return () -> {
      _startedTasks.mark();
      try {
        task.run();
      } finally {
        _completedTasks.mark();
      }
    };
  }
}
