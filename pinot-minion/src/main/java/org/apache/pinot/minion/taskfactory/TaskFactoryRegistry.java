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
import org.apache.helix.HelixManager;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.EventObserverFactoryRegistry;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObserverFactory;
import org.apache.pinot.minion.exception.FatalException;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.minion.executor.TaskExecutorFactoryRegistry;
import org.apache.pinot.minion.metrics.MinionGauge;
import org.apache.pinot.minion.metrics.MinionMeter;
import org.apache.pinot.minion.metrics.MinionMetrics;
import org.apache.pinot.minion.metrics.MinionQueryPhase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for all {@link TaskFactory}.
 * <p>All {@link PinotTaskExecutor} in {@link TaskExecutorFactoryRegistry} will automatically be registered.
 */
public class TaskFactoryRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskFactoryRegistry.class);

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
            private final MinionMetrics _minionMetrics = MinionContext.getInstance().getMinionMetrics();

            @Override
            public TaskResult run() {
              HelixManager helixManager = context.getManager();
              JobContext jobContext = TaskDriver.getJobContext(helixManager, context.getJobConfig().getJobId());
              // jobContext.getStartTime() return the time in milliseconds of job being put into helix queue.
              long jobInQueueTime = jobContext.getStartTime();
              long jobDequeueTime = System.currentTimeMillis();
              _minionMetrics.addPhaseTiming(taskType, MinionQueryPhase.TASK_QUEUEING, jobDequeueTime - jobInQueueTime);
              _minionMetrics.addValueToGlobalGauge(MinionGauge.NUMBER_OF_TASKS, 1L);
              TaskResult result = runInternal();
              _minionMetrics.addValueToGlobalGauge(MinionGauge.NUMBER_OF_TASKS, -1L);
              return result;
            }

            private TaskResult runInternal() {
              PinotTaskConfig pinotTaskConfig = PinotTaskConfig.fromHelixTaskConfig(_taskConfig);
              if (StringUtils.isBlank(pinotTaskConfig.getConfigs().get(MinionConstants.AUTH_TOKEN))) {
                pinotTaskConfig.getConfigs()
                    .put(MinionConstants.AUTH_TOKEN, MinionContext.getInstance().getTaskAuthToken());
              }

              _eventObserver.notifyTaskStart(pinotTaskConfig);
              _minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_EXECUTED, 1L);
              LOGGER.info("Start running {}: {} with configs: {}", pinotTaskConfig.getTaskType(), _taskConfig.getId(),
                  pinotTaskConfig.getConfigs());

              try {
                long startTimeInNanos = System.nanoTime();
                Object executionResult = _taskExecutor.executeTask(pinotTaskConfig);
                long timeSpentInNanos = System.nanoTime() - startTimeInNanos;
                _eventObserver.notifyTaskSuccess(pinotTaskConfig, executionResult);
                _minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_COMPLETED, 1L);
                _minionMetrics.addPhaseTiming(taskType, MinionQueryPhase.TASK_EXECUTION, timeSpentInNanos);
                LOGGER.info("Task: {} completed in: {}ms", _taskConfig.getId(),
                    TimeUnit.NANOSECONDS.toMillis(timeSpentInNanos));
                return new TaskResult(TaskResult.Status.COMPLETED, "Succeeded");
              } catch (TaskCancelledException e) {
                _eventObserver.notifyTaskCancelled(pinotTaskConfig);
                _minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_CANCELLED, 1L);
                LOGGER.info("Task: {} got cancelled", _taskConfig.getId(), e);
                return new TaskResult(TaskResult.Status.CANCELED, e.toString());
              } catch (FatalException e) {
                _eventObserver.notifyTaskError(pinotTaskConfig, e);
                _minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_FATAL_FAILED, 1L);
                LOGGER.error("Caught fatal exception while executing task: {}", _taskConfig.getId(), e);
                return new TaskResult(TaskResult.Status.FATAL_FAILED, e.toString());
              } catch (Exception e) {
                _eventObserver.notifyTaskError(pinotTaskConfig, e);
                _minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_FAILED, 1L);
                LOGGER.error("Caught exception while executing task: {}", _taskConfig.getId(), e);
                return new TaskResult(TaskResult.Status.FAILED, e.toString());
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

  /**
   * Returns the task factory registry.
   */
  public Map<String, TaskFactory> getTaskFactoryRegistry() {
    return _taskFactoryRegistry;
  }
}
