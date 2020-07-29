/*
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

package org.apache.pinot.thirdeye.anomaly.task;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.slf4j.MDC;


public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);
  private static final Random RANDOM = new Random();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private ExecutorService taskExecutorService;
  private ExecutorService taskWatcherExecutorService;

  private final TaskManager taskDAO;
  private TaskContext taskContext;
  private long workerId;
  private final Set<TaskStatus> allowedOldTaskStatus = new HashSet<>();
  private TaskDriverConfiguration driverConfiguration;
  private boolean isOnline;

  private volatile boolean shutdown = false;

  public TaskDriver(ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration, boolean isOnline) {
    driverConfiguration = thirdEyeAnomalyConfiguration.getTaskDriverConfiguration();
    workerId = thirdEyeAnomalyConfiguration.getId();
    taskDAO = DAO_REGISTRY.getTaskDAO();
    String threadNamePrefix = isOnline ? "online-" : "";
    taskExecutorService = Executors.newFixedThreadPool(
            driverConfiguration.getMaxParallelTasks(),
            new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "task-executor-%d").build());
    taskWatcherExecutorService = Executors.newFixedThreadPool(
            driverConfiguration.getMaxParallelTasks(),
            new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "task-watcher-%d").setDaemon(true).build());
    taskContext = new TaskContext();
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeAnomalyConfiguration);
    allowedOldTaskStatus.add(TaskStatus.FAILED);
    allowedOldTaskStatus.add(TaskStatus.WAITING);
    this.isOnline = isOnline;
  }

  public void start() throws Exception {
    // Mark all assigned tasks with RUNNING as FAILED
    List<TaskDTO> leftoverTasks = DAO_REGISTRY.getTaskDAO().findByStatusAndWorkerId(workerId, TaskStatus.RUNNING);
    if (!leftoverTasks.isEmpty()) {
      LOG.info("Found {} RUNNING tasks with worker id {} at start", leftoverTasks.size(), workerId);
      for (TaskDTO task : leftoverTasks) {
        LOG.info("Update task {} from RUNNING to FAILED", task.getId());
        DAO_REGISTRY.getTaskDAO().updateStatusAndTaskEndTime(task.getId(), TaskStatus.RUNNING, TaskStatus.FAILED,
            System.currentTimeMillis(), "FAILED status updated by the worker at start");
      }
    }
    for (int i = 0; i < driverConfiguration.getMaxParallelTasks(); i++) {
      Runnable runnable = new Runnable() {
        @Override public void run() {
          while (!shutdown) {
            LOG.info("Finding next task to execute");

            // select a task to execute, and update it to RUNNING
            TaskDTO anomalyTaskSpec = TaskDriver.this.acquireTask();

            if (anomalyTaskSpec != null) { // a task has acquired and we must finish executing it before termination
              long tStart = System.nanoTime();
              if (TaskDriver.this.isOnline) {
                ThirdeyeMetricsUtil.onlineTaskCounter.inc();
              }
              ThirdeyeMetricsUtil.taskCounter.inc();

              try {
                MDC.put("job.name", anomalyTaskSpec.getJobName());
                LOG.info("Executing task {} {}", anomalyTaskSpec.getId(), anomalyTaskSpec.getTaskInfo());

                // execute the selected task
                TaskType taskType = anomalyTaskSpec.getTaskType();
                TaskRunner taskRunner = TaskRunnerFactory.getTaskRunnerFromTaskType(taskType);
                TaskInfo taskInfo = TaskInfoFactory.getTaskInfoFromTaskType(taskType, anomalyTaskSpec.getTaskInfo());
                Future<List<TaskResult>> future = taskExecutorService.submit(new Callable<List<TaskResult>>() {
                  @Override
                  public List<TaskResult> call() throws Exception {
                    return taskRunner.execute(taskInfo, taskContext);
                  }
                });
                try {
                  List<TaskResult> taskResults = future.get(driverConfiguration.getMaxTaskRunTimeMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                  LOG.error("Timeout on executing task", e);
                  future.cancel(true);
                  LOG.info("Executor thread gets cancelled successfully: {}", future.isCancelled());
                  updateStatusAndTaskEndTime(anomalyTaskSpec.getId(),
                      TaskStatus.RUNNING, TaskStatus.TIMEOUT, e.getMessage());
                  continue;
                }
                LOG.info("DONE Executing task {}", anomalyTaskSpec.getId());
                // update status to COMPLETED
                updateStatusAndTaskEndTime(anomalyTaskSpec.getId(), TaskStatus.RUNNING, TaskStatus.COMPLETED, "");
                ThirdeyeMetricsUtil.taskSuccessCounter.inc();
              } catch (Exception e) {
                ThirdeyeMetricsUtil.taskExceptionCounter.inc();
                LOG.error("Exception in electing and executing task", e);

                try {
                  // update task status failed
                  updateStatusAndTaskEndTime(anomalyTaskSpec.getId(), TaskStatus.RUNNING, TaskStatus.FAILED,
                      ExceptionUtils.getMessage(e) + "\n" + ExceptionUtils.getStackTrace(e));
                } catch (Exception e1) {
                  LOG.error("Error in updating failed status", e1);
                }

              } finally {
                long elapsedTime = System.nanoTime() - tStart;
                LOG.info("Task {} took {} nano seconds", anomalyTaskSpec.getId(), elapsedTime);
                MDC.clear();
                if (TaskDriver.this.isOnline) {
                  ThirdeyeMetricsUtil.onlineTaskDurationCounter.inc(elapsedTime);
                }
                ThirdeyeMetricsUtil.taskDurationCounter.inc(elapsedTime);
              }
            }
          }
          LOG.info("Thread safely quiting");
        }
      };
      taskWatcherExecutorService.submit(runnable);
      LOG.info("Starting task driver");
    }
  }

  public void shutdown() {
    shutdown = true;
    AnomalyUtils.safelyShutdownExecutionService(taskExecutorService, this.getClass());
    AnomalyUtils.safelyShutdownExecutionService(taskWatcherExecutorService, this.getClass());
  }

  /**
   * Returns a TaskDTO if a task is successfully acquired; returns null if system is shutting down.
   *
   * @return null if system is shutting down.
   */
  private TaskDTO acquireTask() {
    LOG.info("Trying to find a task to execute");
    while (!shutdown) {
      List<TaskDTO> anomalyTasks = new ArrayList<>();
      boolean hasFetchError = false;
      try {
        // randomize fetching head and tail to reduce synchronized patterns across threads (and hosts)
        boolean orderAscending = System.currentTimeMillis() % 2 == 0;

        // find by task type to separate online task from a normal task
        TaskType type = this.isOnline ? TaskType.DETECTION_ONLINE : TaskType.DETECTION;
        anomalyTasks = taskDAO
            .findByStatusAndTypeOrderByCreateTime(TaskStatus.WAITING, type, driverConfiguration.getTaskFetchSizeCap(),
                orderAscending);
      } catch (Exception e) {
        hasFetchError = true;
        anomalyTasks.clear();
        LOG.warn("Exception found in fetching new tasks", e);
      }

      if (CollectionUtils.isNotEmpty(anomalyTasks)) {
        LOG.info("Found {} tasks in waiting state", anomalyTasks.size());

        // shuffle candidate tasks to avoid synchronized patterns across threads (and hosts)
        Collections.shuffle(anomalyTasks);

        for (int i = 0; i < anomalyTasks.size() && !shutdown; i++) {
          TaskDTO anomalyTaskSpec = anomalyTasks.get(i);

          boolean success = false;
          try {
            success = taskDAO.updateStatusAndWorkerId(workerId, anomalyTaskSpec.getId(),
                    allowedOldTaskStatus, anomalyTaskSpec.getVersion());
            LOG.info("Trying to acquire task id [{}], success status: [{}] with version [{}]",
                anomalyTaskSpec.getId(), success, anomalyTaskSpec.getVersion());
          } catch (Exception e) {
            LOG.warn("Got exception when acquiring task. (Worker Id: {})", workerId, e);
          }
          if (success) {
            LOG.info("Acquired task: {}", anomalyTaskSpec);
            return anomalyTaskSpec;
          }
        }
      }

      if (shutdown) {
        return null;
      } else {
        long sleepTime = driverConfiguration.getTaskFailureDelayInMillis();
        if (!hasFetchError) {
          // sleep for few seconds if not tasks found - avoid cpu thrashing
          // also add some extra random number of milli seconds to allow threads to start at different times
          sleepTime = driverConfiguration.getNoTaskDelayInMillis() + RANDOM
              .nextInt(driverConfiguration.getRandomDelayCapInMillis());
        }
        try {
          LOG.debug("No tasks found to execute, sleeping for {} MS", sleepTime);
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          if (!shutdown) {
            LOG.warn(e.getMessage(), e);
          }
        }
      }
    }
    return null;
  }

  private void updateStatusAndTaskEndTime(long taskId, TaskStatus oldStatus, TaskStatus newStatus, String message) {
    LOG.info("Starting updateStatus for task id {}", taskId);
    try {
      taskDAO.updateStatusAndTaskEndTime(taskId, oldStatus, newStatus, System.currentTimeMillis(), message);
      LOG.info("Updated status {}", newStatus);
    } catch (Exception e) {
      LOG.error("Exception in updating status and task end time", e);
    }
  }
}
