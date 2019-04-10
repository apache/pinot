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

import org.apache.pinot.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;

import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);
  private static final Random RANDOM = new Random();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private ExecutorService taskExecutorService;

  private final TaskManager taskDAO;
  private TaskContext taskContext;
  private long workerId;
  private final Set<TaskStatus> allowedOldTaskStatus = new HashSet<>();
  private TaskDriverConfiguration driverConfiguration;

  private volatile boolean shutdown = false;

  public TaskDriver(ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration,
      AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory,
      AnomalyClassifierFactory anomalyClassifierFactory) {
    driverConfiguration = thirdEyeAnomalyConfiguration.getTaskDriverConfiguration();
    workerId = thirdEyeAnomalyConfiguration.getId();
    taskDAO = DAO_REGISTRY.getTaskDAO();
    taskExecutorService = Executors.newFixedThreadPool(driverConfiguration.getMaxParallelTasks());
    taskContext = new TaskContext();
    taskContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    taskContext.setThirdEyeAnomalyConfiguration(thirdEyeAnomalyConfiguration);
    taskContext.setAlertFilterFactory(alertFilterFactory);
    taskContext.setAnomalyClassifierFactory(anomalyClassifierFactory);
    allowedOldTaskStatus.add(TaskStatus.FAILED);
    allowedOldTaskStatus.add(TaskStatus.WAITING);
  }

  public void start() throws Exception {
    for (int i = 0; i < driverConfiguration.getMaxParallelTasks(); i++) {
      Callable callable = new Callable() {
        @Override public Object call() throws Exception {
          while (!shutdown) {
            LOG.info("Thread {} : Finding next task to execute.", Thread.currentThread().getId());

            // select a task to execute, and update it to RUNNING
            TaskDTO anomalyTaskSpec = TaskDriver.this.acquireTask();

            if (anomalyTaskSpec != null) { // a task has acquired and we must finish executing it before termination
              long tStart = System.nanoTime();
              ThirdeyeMetricsUtil.taskCounter.inc();

              try {
                LOG.info("Thread {} : Executing task: {} {}", Thread.currentThread().getId(), anomalyTaskSpec.getJobName(),
                    anomalyTaskSpec.getTaskInfo());

                // execute the selected task
                TaskType taskType = anomalyTaskSpec.getTaskType();
                TaskRunner taskRunner = TaskRunnerFactory.getTaskRunnerFromTaskType(taskType);
                TaskInfo taskInfo = TaskInfoFactory.getTaskInfoFromTaskType(taskType, anomalyTaskSpec.getTaskInfo());

                updateTaskStartTime(anomalyTaskSpec.getId());
                List<TaskResult> taskResults = taskRunner.execute(taskInfo, taskContext);
                LOG.info("Thread {} : DONE Executing task: {}", Thread.currentThread().getId(), anomalyTaskSpec.getId());
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
                LOG.info("Thread {} : Task {} took {} nano seconds", Thread.currentThread().getId(),
                    anomalyTaskSpec.getId(), elapsedTime);
                ThirdeyeMetricsUtil.taskDurationCounter.inc(elapsedTime);
              }
            }
          }
          return 0;
        }
      };
      taskExecutorService.submit(callable);
      LOG.info("Thread {} : Started task driver", Thread.currentThread().getId());
    }
  }

  public void shutdown() {
    shutdown = true;
    AnomalyUtils.safelyShutdownExecutionService(taskExecutorService, this.getClass());
  }

  /**
   * Returns a TaskDTO if a task is successfully acquired; returns null if system is shutting down.
   *
   * @return null if system is shutting down.
   */
  private TaskDTO acquireTask() {
    LOG.info("Thread {} : Trying to find a task to execute", Thread.currentThread().getId());
    while (!shutdown) {
      List<TaskDTO> anomalyTasks = new ArrayList<>();
      boolean hasFetchError = false;
      try {
        // randomize fetching head and tail to reduce synchronized patterns across threads (and hosts)
        boolean orderAscending = System.currentTimeMillis() % 2 == 0;
        anomalyTasks = taskDAO
            .findByStatusOrderByCreateTime(TaskStatus.WAITING, driverConfiguration.getTaskFetchSizeCap(),
                orderAscending);
      } catch (Exception e) {
        hasFetchError = true;
        anomalyTasks.clear();
        LOG.warn("Exception found in fetching new tasks", e);
      }

      if (CollectionUtils.isNotEmpty(anomalyTasks)) {
        LOG.info("Thread {} : Found {} tasks in waiting state", Thread.currentThread().getId(), anomalyTasks.size());

        // shuffle candidate tasks to avoid synchronized patterns across threads (and hosts)
        Collections.shuffle(anomalyTasks);

        for (int i = 0; i < anomalyTasks.size() && !shutdown; i++) {
          TaskDTO anomalyTaskSpec = anomalyTasks.get(i);

          boolean success = false;
          try {
            success = taskDAO
                .updateStatusAndWorkerId(workerId, anomalyTaskSpec.getId(), allowedOldTaskStatus,
                    TaskStatus.RUNNING, anomalyTaskSpec.getVersion());
            LOG.info("Thread {} : Trying to acquire task id [{}], success status: [{}] with version [{}]",
                Thread.currentThread().getId(), anomalyTaskSpec.getId(), success, anomalyTaskSpec.getVersion());
          } catch (Exception e) {
            LOG.warn("Thread {} : Got exception when acquiring task. (Worker Id: {})", Thread.currentThread().getId(),
                workerId, e);
          }
          if (success) {
            LOG.info("Thread {} has acquired task: {}", Thread.currentThread().getId(), anomalyTaskSpec);
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

  private void updateTaskStartTime(long taskId) {
    LOG.info("Thread {} : Starting updateTaskStartTime for task id {}", Thread.currentThread().getId(), taskId);
    try {
      long startTime = System.currentTimeMillis();
      taskDAO.updateTaskStartTime(taskId, startTime);
      LOG.info("Thread {} : Updated task start time {}", Thread.currentThread().getId(), startTime);
    } catch (Exception e) {
      LOG.error("Exception in updating task start time", e);
    }
  }

  private void updateStatusAndTaskEndTime(long taskId, TaskStatus oldStatus, TaskStatus newStatus, String message) {
    LOG.info("Thread {} : Starting updateStatus for task id {}", Thread.currentThread().getId(), taskId);
    try {
      taskDAO.updateStatusAndTaskEndTime(taskId, oldStatus, newStatus, System.currentTimeMillis(), message);
      LOG.info("Thread {} : Updated status {}", Thread.currentThread().getId(), newStatus);
    } catch (Exception e) {
      LOG.error("Exception in updating status and task end time", e);
    }
  }
}
