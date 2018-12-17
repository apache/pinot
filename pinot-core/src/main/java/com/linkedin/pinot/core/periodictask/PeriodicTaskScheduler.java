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

import java.util.ArrayList;
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

  private ScheduledExecutorService _executorService;

  /**
   * Start scheduling periodic tasks.
   */
  public void start(List<PeriodicTask> periodicTasks) {
    if (_executorService != null) {
      LOGGER.warn("Periodic task scheduler already started");
    }

    List<PeriodicTask> tasksWithValidInterval = new ArrayList<>();
    for (PeriodicTask periodicTask : periodicTasks) {
      if (periodicTask.getIntervalInSeconds() > 0) {
        LOGGER.info("Adding periodic task: {}", periodicTask);
        tasksWithValidInterval.add(periodicTask);
      } else {
        LOGGER.info("Skipping periodic task: {}", periodicTask);
      }
    }

    if (tasksWithValidInterval.isEmpty()) {
      LOGGER.warn("No periodic task scheduled");
    } else {
      LOGGER.info("Starting periodic task scheduler with tasks: {}", tasksWithValidInterval);
      _executorService = Executors.newScheduledThreadPool(tasksWithValidInterval.size());
      for (PeriodicTask periodicTask : tasksWithValidInterval) {
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
  }

  public void stop() {
    if (_executorService != null) {
      LOGGER.info("Stopping periodic task scheduler");
      _executorService.shutdown();
      _executorService = null;
    }
  }
}
