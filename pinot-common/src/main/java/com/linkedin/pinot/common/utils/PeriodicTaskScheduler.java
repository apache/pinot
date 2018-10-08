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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PeriodicTaskScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskScheduler.class);
  private PriorityBlockingQueue<PeriodicTask> _periodicTasks;
  protected ScheduledExecutorService _executorService;
  private long _intervalSeconds;
  private long _initialDelaySeconds;

  public PeriodicTaskScheduler() {
    _executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName("PeriodicTaskSchedulerExecutorService");
      return thread;
    });
    _initialDelaySeconds = PeriodicTask.DEFAULT_INITIAL_DELAY_IN_SECOND;
    _intervalSeconds = PeriodicTask.DEFAULT_RUN_FREQUENCY_IN_SECOND;
    _periodicTasks = new PriorityBlockingQueue<>(10, (o1, o2) -> {
      if (o1.getExecutionTime() == o2.getExecutionTime()) {
        return 0;
      }
      return o1.getExecutionTime() < o2.getExecutionTime() ? -1 : 1;
    });
  }

  public void start() {
    LOGGER.info("Starting PeriodicTaskScheduler. Run frequency in seconds: {}", _intervalSeconds);

    // Set up an executor that executes tasks periodically
    _executorService.scheduleAtFixedRate(() -> {
      if (_periodicTasks.isEmpty()) {
        LOGGER.info("No task assigned to PeriodicTaskScheduler");
        return;
      }
      PeriodicTask task = _periodicTasks.peek();
      if (System.currentTimeMillis() < task.getExecutionTime()) {
        return;
      }
      _periodicTasks.poll();
      try {
        task.runTask();
      } catch (Throwable e) {
        // catch all errors to prevent subsequent exeuctions from being silently suppressed
        // Ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html#scheduleWithFixedDelay-java.lang.Runnable-long-long-java.util.concurrent.TimeUnit-
        LOGGER.warn("Caught exception while running PeriodicTaskScheduler", e);
      } finally {
        task.updateExecutionTime();
        _periodicTasks.offer(task);
      }
    }, _initialDelaySeconds, _intervalSeconds, TimeUnit.SECONDS);
  }

  public void stop() {
    LOGGER.info("Stopping PeriodicTaskScheduler");
    if (_executorService == null) {
      return;
    }
    _executorService.shutdown();
    try {
      _executorService.awaitTermination(_initialDelaySeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignored
    }
    _executorService = null;
  }

  /**
   * Add periodic task before starting the scheduler.
   */
  public void addPeriodicTask(PeriodicTask task) {
    if (task.getIntervalSeconds() > 0L) {
      LOGGER.info("Adding {} to PeriodicTaskScheduler, run frequency in seconds: {}", task.getTaskName(),
          task.getIntervalSeconds());
      task.initTask();
      _periodicTasks.offer(task);
      _intervalSeconds = Math.min(_intervalSeconds, task.getIntervalSeconds());
      _initialDelaySeconds = Math.min(_initialDelaySeconds, task.getInitialDelaySeconds());
    }
  }

  public void removePeriodicTask(PeriodicTask task) {
    _periodicTasks.remove(task);
  }
}
