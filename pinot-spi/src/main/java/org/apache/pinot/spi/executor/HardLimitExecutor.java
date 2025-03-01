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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * An Executor that allows a maximum of tasks running at the same time, rejecting immediately any excess.
 */
public class HardLimitExecutor extends DecoratorExecutorService {

  private final AtomicInteger _running;
  private final int _max;

  public HardLimitExecutor(int max, ExecutorService executorService) {
    super(executorService);
    _running = new AtomicInteger(0);
    _max = max;
  }

  /**
   * Returns the hard limit of the number of threads that can be used by the multi-stage executor.
   * @param config Pinot configuration
   * @return hard limit of the number of threads that can be used by the multi-stage executor (no hard limit if <= 0)
   */
  public static int getMultiStageExecutorHardLimit(PinotConfiguration config) {
    try {
      return Integer.parseInt(config.getProperty(
          CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS,
          CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS
      )) * Integer.parseInt(config.getProperty(
          CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR,
          CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR
      ));
    } catch (NumberFormatException e) {
      return Integer.parseInt(CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS);
    }
  }

  protected void checkTaskAllowed() {
    if (_running.get() >= _max) {
      throw new IllegalStateException("Tasks limit exceeded.");
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
