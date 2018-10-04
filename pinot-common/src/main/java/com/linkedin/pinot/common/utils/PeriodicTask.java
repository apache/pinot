/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class PeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTask.class);
  protected ScheduledExecutorService _executorService;
  private String _taskName;
  private long _intervalSeconds;
  private long _initialDelay;

  public PeriodicTask(String taskName) {
    this(taskName, 3600L);
  }

  public PeriodicTask(String taskName, long runFrequencyInSeconds) {
    this(taskName, runFrequencyInSeconds, 120L);
  }

  public PeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelay) {
    _taskName = taskName;
    _intervalSeconds = runFrequencyInSeconds;
    _initialDelay = initialDelay;
    _executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName(_taskName + "ExecutorService");
      return thread;
    });
  }

  public abstract void runTask();

  /**
   * Start the periodic task.
   */
  public void start() {
    LOGGER.info("Starting {}", _taskName);

    // Set up an executor that executes validation tasks periodically
    _executorService.scheduleWithFixedDelay(() -> {
      try {
        runTask();
      } catch (Throwable e) {
        // catch all errors to prevent subsequent exeuctions from being silently suppressed
        // Ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html#scheduleWithFixedDelay-java.lang.Runnable-long-long-java.util.concurrent.TimeUnit-
        LOGGER.warn("Caught exception while running {}", _taskName, e);
      }
    }, _initialDelay, _intervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Stop the periodic task.
   */
  public void stop() {
    LOGGER.info("Stopping {}", _taskName);
    if (_executorService == null) {
      return;
    }
    _executorService.shutdown();
    try {
      _executorService.awaitTermination(_initialDelay, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignored
    }
    _executorService = null;
  }

  protected void setIntervalSeconds(long intervalSeconds) {
    _intervalSeconds = intervalSeconds;
  }

  protected long getIntervalSeconds() {
    return _intervalSeconds;
  }
}
