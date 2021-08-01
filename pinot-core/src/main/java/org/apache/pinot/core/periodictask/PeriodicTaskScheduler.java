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
package org.apache.pinot.core.periodictask;

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
  private List<PeriodicTask> _tasksWithValidInterval;
  private volatile int _taskCount;

  /**
   * Initializes the periodic task scheduler with a list of periodic tasks.
   */
  public void init(List<PeriodicTask> periodicTasks) {
    _tasksWithValidInterval = new ArrayList<>();
    for (PeriodicTask periodicTask : periodicTasks) {
      if (periodicTask.getIntervalInSeconds() > 0) {
        LOGGER.info("Adding periodic task: {}", periodicTask);
        _tasksWithValidInterval.add(periodicTask);
      } else {
        LOGGER.info("Skipping periodic task: {}", periodicTask);
      }
    }

    _taskCount = _tasksWithValidInterval.size();
  }

  /**
   * Get number of tasks scheduled. Method is thread safe since task list is not modified after it is
   * initialized in {@link #init} method.
   * @return
   */
  public int getPeriodicTaskCount() {
    return _taskCount;
  }

  /**
   * Starts scheduling periodic tasks.
   */
  public synchronized void start() {
    if (_executorService != null) {
      LOGGER.warn("Periodic task scheduler already started");
    }

    if (_tasksWithValidInterval.isEmpty()) {
      LOGGER.warn("No periodic task scheduled");
    } else {
      LOGGER.info("Starting periodic task scheduler with tasks: {}", _tasksWithValidInterval);
      _executorService = Executors.newScheduledThreadPool(_tasksWithValidInterval.size());
      for (PeriodicTask periodicTask : _tasksWithValidInterval) {
        periodicTask.start();
        _executorService.scheduleWithFixedDelay(() -> {
          try {
            LOGGER.info("Starting {} with running frequency of {} seconds.", periodicTask.getTaskName(),
                periodicTask.getIntervalInSeconds());
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

  /**
   * Shuts down the executor service and stops the periodic tasks.
   */
  public synchronized void stop() {
    if (_executorService != null) {
      LOGGER.info("Stopping periodic task scheduler");
      _executorService.shutdown();
      _executorService = null;
    }

    if (_tasksWithValidInterval != null) {
      LOGGER.info("Stopping all periodic tasks: {}", _tasksWithValidInterval);
      _tasksWithValidInterval.parallelStream().forEach(PeriodicTask::stop);
    }
  }

  /** @return true if task with given name exists; otherwise, false. */
  public boolean hasTask(String periodicTaskName) {
    for (PeriodicTask task : _tasksWithValidInterval) {
      if (task.getTaskName().equals(periodicTaskName)) {
        return true;
      }
    }

    return false;
  }

  private PeriodicTask getPeriodicTask(String periodicTaskName) {
    for (PeriodicTask task : _tasksWithValidInterval) {
      if (task.getTaskName().equals(periodicTaskName)) {
        return task;
      }
    }
    return null;
  }

  /**
   * Execute specified {@link PeriodicTask} immediately. If the task is already running, wait for the running task
   * to finish before executing the task again.
   */
  public void execute(String periodicTaskName, String tableName) {
    PeriodicTask periodicTask = getPeriodicTask(periodicTaskName);
    LOGGER.info("Immediately executing periodic task {}", periodicTaskName);
    if (periodicTask == null) {
      throw new IllegalArgumentException("Unknown Periodic Task " + periodicTaskName);
    }

    _executorService.schedule(() -> {
      try {
        // To prevent thread conflict, this call will block if the same task is already running (see
        // BasePeriodicTask.run method).
        periodicTask.run();
      } catch (Throwable t) {
        // catch all errors to prevent subsequent executions from being silently suppressed
        // Ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html#scheduleWithFixedDelay-java.lang.Runnable-long-long-java.util.concurrent.TimeUnit-
        LOGGER.warn("Caught exception while attempting to execute named periodic task: {}", periodicTask.getTaskName(), t);
      }
    }, 0, TimeUnit.SECONDS);
  }
}
