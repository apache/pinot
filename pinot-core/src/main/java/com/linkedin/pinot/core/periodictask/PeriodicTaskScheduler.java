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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task scheduler will schedule a list of tasks based on their initial delay time and interval time.
 */
public class PeriodicTaskScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskScheduler.class);
  private final ScheduledExecutorService _executorService;
  private long _initialDelayInSeconds;
  private volatile boolean _running;

  private static class PeriodicTaskEntry implements Comparable<PeriodicTaskEntry> {
    private PeriodicTask _periodicTask;
    private long _executionTime;

    PeriodicTaskEntry(PeriodicTask periodicTask) {
      _periodicTask = periodicTask;
      _executionTime = System.currentTimeMillis() + _periodicTask.getInitialDelayInSeconds() * 1000L;
      if (_periodicTask.getIntervalInSeconds() > 0L) {
        _periodicTask.init();
      }
    }

    PeriodicTask getPeriodicTask() {
      return _periodicTask;
    }

    long getExecutionTime() {
      return _executionTime;
    }

    void updateExecutionTime() {
      _executionTime = System.currentTimeMillis() + _periodicTask.getIntervalInSeconds() * 1000L;
    }

    @Override
    public int compareTo(PeriodicTaskEntry o) {
      return Long.compare(this._executionTime, o._executionTime);
    }
  }

  public PeriodicTaskScheduler(long initialDelayInSeconds) {
    LOGGER.info("Initializing PeriodicTaskScheduler. Initial delay in seconds: {}", initialDelayInSeconds);
    _executorService = Executors.newScheduledThreadPool(1);
    _initialDelayInSeconds = initialDelayInSeconds;
    _running = true;
  }

  /**
   * Start scheduling periodic tasks.
   */
  public void start(List<PeriodicTask> periodicTasks) {
    if (periodicTasks == null || periodicTasks.isEmpty()) {
      LOGGER.warn("No periodic task assigned to scheduler!");
      return;
    }

    LOGGER.info("Starting PeriodicTaskScheduler.");
    PriorityBlockingQueue<PeriodicTaskEntry> queue = new PriorityBlockingQueue<>(periodicTasks.size());
    for (PeriodicTask task : periodicTasks) {
      queue.offer(new PeriodicTaskEntry(task));
    }

    // Set up an executor that executes tasks periodically
    _executorService.schedule(() -> {
      while (_running) {
        PeriodicTaskScheduler.PeriodicTaskEntry taskEntry = null;
        try {
          taskEntry = queue.take();
          long currentTime = System.currentTimeMillis();
          if (currentTime < taskEntry.getExecutionTime()) {
            Thread.sleep(taskEntry.getExecutionTime() - currentTime);
          }
          LOGGER.info("Running Task {}", taskEntry.getPeriodicTask().getTaskName());
          taskEntry.getPeriodicTask().run();
        } catch (InterruptedException ie) {
          LOGGER.warn("Interrupted when running periodic task scheduler", ie);
          return;
        } catch (Throwable e) {
          // catch all errors to prevent subsequent executions from being silently suppressed
          // Ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html#scheduleWithFixedDelay-java.lang.Runnable-long-long-java.util.concurrent.TimeUnit-
          LOGGER.warn("Caught exception while running PeriodicTaskScheduler", e);
        } finally {
          if (taskEntry != null) {
            taskEntry.updateExecutionTime();
            queue.offer(taskEntry);
          }
        }
      }
    }, _initialDelayInSeconds, TimeUnit.SECONDS);
  }

  public void stop() {
    LOGGER.info("Stopping PeriodicTaskScheduler");
    _running = false;
    _executorService.shutdown();
  }
}
