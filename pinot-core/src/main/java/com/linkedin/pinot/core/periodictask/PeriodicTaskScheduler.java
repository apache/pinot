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
package com.linkedin.pinot.core.periodictask;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task scheduler will schedule a list of tasks based on their initial delay time and interval time.
 */
public class PeriodicTaskScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskScheduler.class);
  private static final int CORE_POOL_SIZE = 5;
  private final ScheduledExecutorService _executorService;

  public PeriodicTaskScheduler() {
    LOGGER.info("Initializing PeriodicTaskScheduler.");
    _executorService = Executors.newScheduledThreadPool(CORE_POOL_SIZE);
  }

  /**
   * Start scheduling periodic tasks.
   */
  public void start(List<PeriodicTask> periodicTasks) {
    if (periodicTasks == null || periodicTasks.isEmpty()) {
      LOGGER.warn("No periodic task assigned to scheduler!");
      return;
    }

    if (periodicTasks.size() > CORE_POOL_SIZE) {
      LOGGER.warn("The number of tasks:{} is more than the default number of threads:{}.", periodicTasks.size(),
          CORE_POOL_SIZE);
    }

    LOGGER.info("Starting PeriodicTaskScheduler.");
    // Set up an executor that executes tasks periodically
    for (PeriodicTask periodicTask : periodicTasks) {
      periodicTask.init();
      _executorService.scheduleWithFixedDelay(() -> {
        try {
          periodicTask.run();
        } catch (Throwable e) {
          // catch all errors to prevent subsequent executions from being silently suppressed
          // Ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html#scheduleWithFixedDelay-java.lang.Runnable-long-long-java.util.concurrent.TimeUnit-
          LOGGER.warn("Caught exception while running Task: {}", periodicTask.getTaskName(), e);
        }
      }, periodicTask.getInitialDelayInSeconds(), periodicTask.getIntervalInSeconds(), TimeUnit.SECONDS);
    }
  }

  public void stop() {
    LOGGER.info("Stopping PeriodicTaskScheduler");
    _executorService.shutdown();
  }
}
