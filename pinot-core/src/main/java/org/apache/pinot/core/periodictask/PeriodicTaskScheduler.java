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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task scheduler will schedule a list of tasks based on their initial delay time and interval time. Tasks
 * can also scheduled of immediate execution by calling the scheduleNow() method.
 */
public class PeriodicTaskScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskScheduler.class);
  private static final int MAX_QUARTZ_THREADS_HARD_LIMIT = 20;
  //configure a thread limit for the quartz scheduler to use
  private static final int CONFIGURED_THREAD_COUNT = Math.min(
      MAX_QUARTZ_THREADS_HARD_LIMIT,
      Math.max(1, Runtime.getRuntime().availableProcessors())
  );
  private ScheduledExecutorService _executorService;
  private Scheduler _scheduler;
  private Map<String, PeriodicTask> _periodicTasks;

  /**
   * Initializes the periodic task scheduler with a list of periodic tasks.
   */
  public void init(List<PeriodicTask> periodicTasks) {
    _periodicTasks = Maps.newHashMapWithExpectedSize(periodicTasks.size());
    for (PeriodicTask periodicTask : periodicTasks) {
      LOGGER.info("Adding periodic task: {}", periodicTask);
      String periodicTaskName = periodicTask.getTaskName();
      Preconditions.checkState(_periodicTasks.put(periodicTaskName, periodicTask) == null,
          "Duplicate periodic task name: %s", periodicTaskName);
    }
  }

  /**
   * Starts scheduling periodic tasks.
   * It uses the cron expression if provided, if not, it falls back to the default fixed delay scheduling.
   *
   */
  public synchronized void start() {
    if (_executorService != null) {
      LOGGER.warn("Periodic task scheduler already started");
      return;
    }
    Preconditions.checkState(_periodicTasks != null,
        "Periodic task scheduler has not been initialized");

    if (_periodicTasks.isEmpty()) {
      LOGGER.warn("No periodic task scheduled");
      return;
    }

    Collection<PeriodicTask> periodicTasks = _periodicTasks.values();
    LOGGER.info("Starting periodic task scheduler with tasks: {}", periodicTasks);

    for (PeriodicTask task : periodicTasks) {
      String cron = task.getCronExpression();
      if (cron != null && !cron.trim().isEmpty() && !CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException(
            String.format("Invalid CRON expression '%s' for task '%s'. Halting controller startup.",
                cron, task.getTaskName())
        );
      }
    }

    boolean hasCronTasks = false;

    for (PeriodicTask task : periodicTasks) {
      String cron = task.getCronExpression();
      if (cron != null && !cron.trim().isEmpty()) {
        if (!CronExpression.isValidExpression(cron)) {
          throw new IllegalArgumentException(
              String.format("Invalid CRON expression '%s' for task '%s'. "
                      + "Halting controller startup.",
                  cron, task.getTaskName())
          );
        }
        hasCronTasks = true;
      }
    }

    if (hasCronTasks) {
      try {
        int periodicTaskCount = _periodicTasks.size();
        Properties quartzProperties = getQuartzProperties(periodicTaskCount);
        StdSchedulerFactory customSchedulerFactory = new StdSchedulerFactory(quartzProperties);
        _scheduler = customSchedulerFactory.getScheduler();
        _scheduler.start();
      } catch (SchedulerException e) {
        throw new RuntimeException("Failed to initialize Quartz scheduler. Halting controller startup.", e);
      }
    }

    _executorService = Executors.newScheduledThreadPool(periodicTasks.size());

    try {
      for (PeriodicTask periodicTask : periodicTasks) {
        periodicTask.start();

        String cronExpression = periodicTask.getCronExpression();
        String taskName = periodicTask.getTaskName();

        if (cronExpression != null && !cronExpression.trim().isEmpty()) {
          LOGGER.info("Scheduling periodic task {} with cron expression: {}", taskName, cronExpression);
          JobDetail jobDetail = JobBuilder.newJob(PeriodicTaskCronJob.class)
              .withIdentity(taskName)
              .build();
          jobDetail.getJobDataMap().put(PeriodicTaskCronJob.PERIODIC_TASK_KEY, periodicTask);

          CronTrigger trigger = TriggerBuilder.newTrigger()
              .withIdentity(taskName + "-CronTrigger")
              .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
              .build();

          _scheduler.scheduleJob(jobDetail, trigger);
        } else {
          // Legacy fallback for blank/unset crons
          long intervalInSeconds = periodicTask.getIntervalInSeconds();
          if (intervalInSeconds > 0) {
            _executorService.scheduleWithFixedDelay(() -> {
              try {
                periodicTask.run();
              } catch (Throwable e) {
                LOGGER.warn("Caught exception while running Task: {}", taskName, e);
              }
            }, periodicTask.getInitialDelayInSeconds(), intervalInSeconds, TimeUnit.SECONDS);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Fatal error scheduling periodic tasks. Cleaning up and halting startup.", e);
      this.stop();
      throw new RuntimeException("Controller startup failed due to periodic task scheduling error", e);
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

    if (_periodicTasks != null) {
      LOGGER.info("Stopping all periodic tasks: {}", _periodicTasks);
      _periodicTasks.values().parallelStream().forEach(PeriodicTask::stop);
    }

    if (_scheduler != null) {
      try {
        LOGGER.info("Stopping Quartz scheduler");
        _scheduler.shutdown(true);
        _scheduler = null;
      } catch (SchedulerException e) {
        LOGGER.error("Failed to shutdown Quartz scheduler", e);
      }
    }
  }

  /// Returns true if the task exists (regardless of whether it is scheduled to run periodically or not).
  public boolean hasTask(String periodicTaskName) {
    return _periodicTasks.containsKey(periodicTaskName);
  }

  /// Returns the list of all registered task names.
  public List<String> getTaskNames() {
    return new ArrayList<>(_periodicTasks.keySet());
  }

  /** Execute {@link PeriodicTask} immediately on the specified table. */
  public void scheduleNow(String periodicTaskName, Properties periodicTaskProperties) {
    //in case the executor service hasnt been initialized its better to log a warning than
    // throw a NPE
    if (_executorService == null) {
      LOGGER.warn("Cannot schedule task '{}' immediately: Scheduler is not running.", periodicTaskName);
      return;
    }
    // During controller deployment, each controller can have a slightly different list of periodic tasks if we add,
    // remove, or rename periodic task. To avoid this situation, we check again (besides the check at controller API
    // level) whether the periodic task exists.
    PeriodicTask periodicTask = _periodicTasks.get(periodicTaskName);
    if (periodicTask == null) {
      LOGGER.error("Unknown periodic task: {}", periodicTaskName);
      return;
    }

    String taskRequestId = periodicTaskProperties.get(PeriodicTask.PROPERTY_KEY_REQUEST_ID).toString();
    LOGGER.info(
        "[TaskRequestId: {}] Schedule task '{}' to run immediately. If the task is already running, this run will "
            + "wait until the current run finishes.",
        taskRequestId, periodicTaskName);
    _executorService.schedule(() -> {
      try {
        // Run the periodic task using the specified parameters. The call to run() method will block if another thread
        // (the periodic execution thread or another thread calling this method) is already in the process of
        // running the same task.
        periodicTask.run(periodicTaskProperties);
      } catch (Throwable t) {
        // catch all errors to prevent subsequent executions from being silently suppressed
        // Ref: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService
        // .html#scheduleWithFixedDelay-java.lang.Runnable-long-long-java.util.concurrent.TimeUnit-
        LOGGER.error("[TaskRequestId: {}] Caught exception while attempting to execute named periodic task: {}",
            taskRequestId, periodicTask.getTaskName(), t);
      }
    }, 0, TimeUnit.SECONDS);
  }

  private static Properties getQuartzProperties(int taskCount) {
    Properties quartzProperties = new Properties();
    //isolating from other scheduler instances by having a different scheduler instance name
    quartzProperties.put(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "ControllerPeriodicTaskScheduler");
    //final thread count that will be used.
    int threadCount = Math.min(taskCount, CONFIGURED_THREAD_COUNT);
    quartzProperties.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    quartzProperties.put("org.quartz.threadPool.threadCount", String.valueOf(threadCount));
    return quartzProperties;
  }
}
