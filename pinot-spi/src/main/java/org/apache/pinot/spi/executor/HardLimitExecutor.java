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
import org.apache.pinot.spi.query.QueryThreadExceedStrategy;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Executor that allows a maximum of tasks running at the same time, rejecting immediately any excess.
 */
public class HardLimitExecutor extends DecoratorExecutorService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HardLimitExecutor.class);

  private final AtomicInteger _running;
  private final int _max;
  private final QueryThreadExceedStrategy _exceedStrategy;

  public HardLimitExecutor(int max, ExecutorService executorService, QueryThreadExceedStrategy exceedStrategy) {
    super(executorService);
    _running = new AtomicInteger(0);
    _max = max;
    _exceedStrategy = exceedStrategy;
  }

  public HardLimitExecutor(int max, ExecutorService executorService) {
    this(max, executorService, QueryThreadExceedStrategy.ERROR);
  }

  /**
   * Returns the hard limit of the number of threads that can be used by the multi-stage executor.
   * @param serverConf Pinot configuration
   * @return hard limit of the number of threads that can be used by the multi-stage executor (no hard limit if <= 0)
   */
  public static int getMultiStageExecutorHardLimit(PinotConfiguration serverConf) {
    try {
      int fixedLimit = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_MSE_MAX_EXECUTION_THREADS,
          CommonConstants.Server.DEFAULT_MSE_MAX_EXECUTION_THREADS);
      if (fixedLimit > 0) {
        return fixedLimit;
      }
      int factorBasedLimit = Integer.parseInt(
          serverConf.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS,
              CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS));
      int factor = Integer.parseInt(
          serverConf.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR,
              CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR));
      if (factorBasedLimit <= 0 || factor <= 0) {
        return -1;
      }
      return factorBasedLimit * factor;
    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to parse multi-stage executor hard limit from config. Hard limiting will be disabled.", e);
      return -1;
    }
  }

  protected void checkTaskAllowed() {
    if (_running.get() >= _max) {
      if (_exceedStrategy == QueryThreadExceedStrategy.LOG) {
        LOGGER.warn("Exceed strategy LOG: Tasks limit max: {} exceeded with running: {} tasks.",
            _max, _running.get());
      } else if (_exceedStrategy == QueryThreadExceedStrategy.ERROR) {
        throw new IllegalStateException("Tasks limit exceeded.");
      } else {
        throw new IllegalStateException(String.format(
            "%s is configured to an unsupported strategy.", this.getClass().getName()));
      }
    }
  }

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    checkTaskAllowed();
    return () -> {
      checkTaskAllowed();
      _running.getAndIncrement();
      try {
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
      _running.getAndIncrement();
      try {
        task.run();
      } finally {
        _running.decrementAndGet();
      }
    };
  }
}
