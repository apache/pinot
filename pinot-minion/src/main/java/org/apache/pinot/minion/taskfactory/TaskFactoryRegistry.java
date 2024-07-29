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
package org.apache.pinot.minion.taskfactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metrics.MinionGauge;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.MinionTimer;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.EventObserverFactoryRegistry;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObserverFactory;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.exception.FatalException;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.minion.executor.TaskExecutorFactoryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


/**
 * Registry for all {@link TaskFactory}.
 * <p>All {@link PinotTaskExecutor} in {@link TaskExecutorFactoryRegistry} will automatically be registered.
 */
public class TaskFactoryRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskFactoryRegistry.class);
  // we use 1000 as the limit for the following reasons:
  // 1. Task results reported (inlcudeing info messages) here will be saved into znode
  //    (<pinot cluster name>/PROPERTYSTORE/TaskRebalancer/<Helix job name>), which has a default limit of 1M.
  //    Since tasks belonging to the same helix job will be saved in the same znode, info messages cannot be too long,
  //    Otherwise, the znode may be too large, causing communication issue between zookeeper clients and zookeeper,
  //    which results in instance crash and znode not updatable.
  // 2. Info messages stored in znode are fetched by minion task APIs (/tasks/{taskType}/debug, /tasks/{taskType}/debug,
  //    /tasks/task/{taskName}/debug) for debugging purpose, so the info must contain enough info.
  // 1000 is a reasonable choice, because with this length, the info message will contain relatively rich info for
  // debugging. At the same time, with znode compressed, the helix job znode can hold ~1000 helix tasks (1M/1000=1000),
  // which is enough in most use cases.
  private static final int MAX_TASK_RESULT_INFO_LEN = 1000;

  private final Map<String, TaskFactory> _taskFactoryRegistry = new HashMap<>();

  public TaskFactoryRegistry(TaskExecutorFactoryRegistry taskExecutorFactoryRegistry,
      EventObserverFactoryRegistry eventObserverFactoryRegistry) {
    for (String taskType : taskExecutorFactoryRegistry.getAllTaskTypes()) {
      PinotTaskExecutorFactory taskExecutorFactory = taskExecutorFactoryRegistry.getTaskExecutorFactory(taskType);
      MinionEventObserverFactory eventObserverFactory = eventObserverFactoryRegistry.getEventObserverFactory(taskType);

      LOGGER.info("Registering {} with task executor factory: {}, event observer factory: {}", taskType,
          taskExecutorFactory.getClass().getSimpleName(), eventObserverFactory.getClass().getSimpleName());
      TaskFactory taskFactory = context -> {
        try {
          return new Task() {
            private final TaskConfig _taskConfig = context.getTaskConfig();
            private final PinotTaskExecutor _taskExecutor = taskExecutorFactory.create();
            private final MinionEventObserver _eventObserver = eventObserverFactory.create();
            private final MinionMetrics _minionMetrics = MinionMetrics.get();

            @Override
            public TaskResult run() {
              HelixManager helixManager = context.getManager();
              JobContext jobContext = TaskDriver.getJobContext(helixManager, context.getJobConfig().getJobId());
              // jobContext.getStartTime() return the time in milliseconds of job being put into helix queue.
              long jobInQueueTimeMs = jobContext.getStartTime();
              long jobDequeueTimeMs = System.currentTimeMillis();
              _minionMetrics
                  .addTimedValue(taskType, MinionTimer.TASK_QUEUEING, jobDequeueTimeMs - jobInQueueTimeMs,
                      TimeUnit.MILLISECONDS);
              String tableName = null;
              try {
                // Set taskId in MDC so that one may config logger to route task logs to separate file.
                MDC.put("taskId", _taskConfig.getId());
                PinotTaskConfig pinotTaskConfig = PinotTaskConfig.fromHelixTaskConfig(_taskConfig);
                tableName = pinotTaskConfig.getTableName();
                _minionMetrics.addValueToGlobalGauge(MinionGauge.NUMBER_OF_TASKS, 1L);
                _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS, 1L);
                if (tableName != null) {
                  _minionMetrics
                      .addTimedTableValue(tableName, taskType, MinionTimer.TASK_QUEUEING,
                          jobDequeueTimeMs - jobInQueueTimeMs, TimeUnit.MILLISECONDS);
                  _minionMetrics.addValueToTableGauge(tableName, MinionGauge.NUMBER_OF_TASKS, 1L);
                  _minionMetrics.addMeteredTableValue(tableName, taskType, MinionMeter.NUMBER_TASKS, 1L);
                }
                MinionEventObservers.getInstance().addMinionEventObserver(_taskConfig.getId(), _eventObserver);
                return runInternal(pinotTaskConfig);
              } finally {
                MinionEventObservers.getInstance().removeMinionEventObserver(_taskConfig.getId());
                _minionMetrics.addValueToGlobalGauge(MinionGauge.NUMBER_OF_TASKS, -1L);
                _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS, -1L);
                long executionTimeMs = System.currentTimeMillis() - jobDequeueTimeMs;
                _minionMetrics
                    .addTimedValue(taskType, MinionTimer.TASK_EXECUTION, executionTimeMs, TimeUnit.MILLISECONDS);
                if (tableName != null) {
                  _minionMetrics.addValueToTableGauge(tableName, MinionGauge.NUMBER_OF_TASKS, -1L);
                  _minionMetrics.addMeteredTableValue(tableName, taskType, MinionMeter.NUMBER_TASKS, -1L);
                  _minionMetrics
                      .addTimedTableValue(tableName, taskType, MinionTimer.TASK_EXECUTION,
                          executionTimeMs, TimeUnit.MILLISECONDS);
                }
                LOGGER.info("Task: {} completed in: {}ms", _taskConfig.getId(), executionTimeMs);
                // Clear taskId from MDC to reset it.
                MDC.remove("taskId");
              }
            }

            private TaskResult runInternal(PinotTaskConfig pinotTaskConfig) {
              if (StringUtils.isBlank(pinotTaskConfig.getConfigs().get(MinionConstants.AUTH_TOKEN))) {
                pinotTaskConfig.getConfigs().put(MinionConstants.AUTH_TOKEN,
                    AuthProviderUtils.toStaticToken(MinionContext.getInstance().getTaskAuthProvider()));
              }

              String tableName = pinotTaskConfig.getTableName();

              _eventObserver.notifyTaskStart(pinotTaskConfig);
              _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS_EXECUTED, 1L);
              if (tableName != null) {
                _minionMetrics.addMeteredTableValue(tableName, taskType,
                    MinionMeter.NUMBER_TASKS_EXECUTED, 1L);
              }
              LOGGER.info("Start running {}: {} with configs: {}", pinotTaskConfig.getTaskType(), _taskConfig.getId(),
                  pinotTaskConfig.getConfigs());

              try {
                Object executionResult = _taskExecutor.executeTask(pinotTaskConfig);
                _eventObserver.notifyTaskSuccess(pinotTaskConfig, executionResult);
                _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS_COMPLETED, 1L);
                if (tableName != null) {
                  _minionMetrics.addMeteredTableValue(tableName, taskType,
                      MinionMeter.NUMBER_TASKS_COMPLETED, 1L);
                }
                LOGGER.info("Task: {} succeeded", _taskConfig.getId());
                return new TaskResult(TaskResult.Status.COMPLETED, "Succeeded");
              } catch (TaskCancelledException e) {
                _eventObserver.notifyTaskCancelled(pinotTaskConfig);
                _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS_CANCELLED, 1L);
                if (tableName != null) {
                  _minionMetrics.addMeteredTableValue(tableName, taskType,
                      MinionMeter.NUMBER_TASKS_CANCELLED, 1L);
                }
                LOGGER.info("Task: {} got cancelled", _taskConfig.getId(), e);
                return new TaskResult(TaskResult.Status.CANCELED, extractAndTrimRootCauseMessage(e));
              } catch (FatalException e) {
                _eventObserver.notifyTaskError(pinotTaskConfig, e);
                _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS_FATAL_FAILED, 1L);
                if (tableName != null) {
                  _minionMetrics.addMeteredTableValue(tableName, taskType,
                      MinionMeter.NUMBER_TASKS_FATAL_FAILED, 1L);
                }
                LOGGER.error("Caught fatal exception while executing task: {}", _taskConfig.getId(), e);
                return new TaskResult(TaskResult.Status.FATAL_FAILED, extractAndTrimRootCauseMessage(e));
              } catch (Exception e) {
                _eventObserver.notifyTaskError(pinotTaskConfig, e);
                _minionMetrics.addMeteredValue(taskType, MinionMeter.NUMBER_TASKS_FAILED, 1L);
                if (tableName != null) {
                  _minionMetrics.addMeteredTableValue(tableName, taskType,
                      MinionMeter.NUMBER_TASKS_FAILED, 1L);
                }
                LOGGER.error("Caught exception while executing task: {}", _taskConfig.getId(), e);
                return new TaskResult(TaskResult.Status.FAILED, extractAndTrimRootCauseMessage(e));
              }
            }

            @Override
            public void cancel() {
              _taskExecutor.cancel();
            }
          };
        } catch (Exception e) {
          LOGGER.error("Caught exception while creating new task", e);
          throw new RuntimeException("Caught exception while creating new task", e);
        }
      };
      _taskFactoryRegistry.put(taskType, taskFactory);
    }
  }

  private static String extractAndTrimRootCauseMessage(Throwable th) {
    String rootCauseMessage = ExceptionUtils.getStackTrace(th);
    if (rootCauseMessage != null && rootCauseMessage.length() > MAX_TASK_RESULT_INFO_LEN) {
      return rootCauseMessage.substring(0, MAX_TASK_RESULT_INFO_LEN);
    }
    return rootCauseMessage;
  }

  /**
   * Returns the task factory registry.
   */
  public Map<String, TaskFactory> getTaskFactoryRegistry() {
    return _taskFactoryRegistry;
  }
}
