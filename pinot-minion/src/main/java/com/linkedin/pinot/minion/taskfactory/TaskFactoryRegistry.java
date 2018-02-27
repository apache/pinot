/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.minion.taskfactory;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.minion.MinionContext;
import com.linkedin.pinot.minion.exception.FatalException;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.minion.executor.TaskExecutorRegistry;
import com.linkedin.pinot.minion.metrics.MinionMeter;
import com.linkedin.pinot.minion.metrics.MinionMetrics;
import com.linkedin.pinot.minion.metrics.MinionQueryPhase;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for all {@link TaskFactory}.
 * <p>All {@link PinotTaskExecutor} in {@link TaskExecutorRegistry} will automatically be registered.
 */
public class TaskFactoryRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskFactoryRegistry.class);

  private final Map<String, TaskFactory> _taskFactoryRegistry = new HashMap<>();

  public TaskFactoryRegistry(@Nonnull TaskExecutorRegistry taskExecutorRegistry) {
    for (final String taskType : taskExecutorRegistry.getAllTaskTypes()) {
      final Class<? extends PinotTaskExecutor> taskExecutorClass = taskExecutorRegistry.getTaskExecutorClass(taskType);
      Preconditions.checkNotNull(taskExecutorClass);

      LOGGER.info("Registering {} with task executor class: {}", taskType, taskExecutorClass.getName());
      TaskFactory taskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(final TaskCallbackContext taskCallbackContext) {
          try {
            return new Task() {
              private final TaskConfig _taskConfig = taskCallbackContext.getTaskConfig();
              private final PinotTaskExecutor _pinotTaskExecutor = taskExecutorClass.newInstance();

              @Override
              public TaskResult run() {
                long startTime = System.nanoTime();
                PinotTaskConfig pinotTaskConfig = PinotTaskConfig.fromHelixTaskConfig(_taskConfig);
                LOGGER.info("Start running {}: {} with configs: {}", pinotTaskConfig.getTaskType(), _taskConfig.getId(),
                    pinotTaskConfig.getConfigs());

                MinionMetrics minionMetrics = MinionContext.getInstance().getMinionMetrics();
                minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_EXECUTED, 1L);
                TaskResult taskResult;
                try {
                  _pinotTaskExecutor.executeTask(pinotTaskConfig);
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_COMPLETED, 1L);
                  taskResult = new TaskResult(TaskResult.Status.COMPLETED, "Succeeded");

                  long timeSpentInNanos = System.nanoTime() - startTime;
                  minionMetrics.addPhaseTiming(taskType, MinionQueryPhase.TASK_EXECUTION, timeSpentInNanos);
                  LOGGER.info("Task: {} completed in: {}ms", _taskConfig.getId(), timeSpentInNanos / 1000);
                } catch (TaskCancelledException e) {
                  taskResult = new TaskResult(TaskResult.Status.CANCELED, e.toString());
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_CANCELLED, 1L);

                  LOGGER.info("Task: {} got cancelled", _taskConfig.getId(), e);
                } catch (FatalException e) {
                  taskResult = new TaskResult(TaskResult.Status.FATAL_FAILED, e.toString());
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_FATAL_FAILED, 1L);

                  LOGGER.error("Caught fatal exception while executing task: {}", _taskConfig.getId(), e);
                } catch (Exception e) {
                  taskResult = new TaskResult(TaskResult.Status.FAILED, e.toString());
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_FAILED, 1L);

                  LOGGER.error("Caught exception while executing task: {}", _taskConfig.getId(), e);
                }
                return taskResult;
              }

              @Override
              public void cancel() {
                _pinotTaskExecutor.cancel();
              }
            };
          } catch (Exception e) {
            LOGGER.error("Caught exception while creating new task", e);
            throw new RuntimeException("Caught exception while creating new task", e);
          }
        }
      };
      _taskFactoryRegistry.put(taskType, taskFactory);
    }
  }

  /**
   * Get the task factory registry.
   *
   * @return Task factory registry
   */
  @Nonnull
  public Map<String, TaskFactory> getTaskFactoryRegistry() {
    return _taskFactoryRegistry;
  }
}
