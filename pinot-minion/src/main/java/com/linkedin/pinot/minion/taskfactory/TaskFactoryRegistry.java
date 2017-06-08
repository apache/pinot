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


/**
 * Registry for all {@link TaskFactory}.
 * <p>All {@link PinotTaskExecutor} in {@link TaskExecutorRegistry} will automatically be registered.
 */
public class TaskFactoryRegistry {
  private final Map<String, TaskFactory> _taskFactoryRegistry = new HashMap<>();

  public TaskFactoryRegistry(@Nonnull TaskExecutorRegistry taskExecutorRegistry,
      @Nonnull final MinionContext minionContext) {
    for (final String taskType : taskExecutorRegistry.getAllTaskTypes()) {
      final Class<? extends PinotTaskExecutor> taskExecutorClass = taskExecutorRegistry.getTaskExecutorClass(taskType);
      Preconditions.checkNotNull(taskExecutorClass);
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
                _pinotTaskExecutor.setMinionContext(minionContext);
                MinionMetrics minionMetrics = minionContext.getMinionMetrics();
                TaskResult taskResult;
                try {
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_EXECUTED, 1L);
                  _pinotTaskExecutor.executeTask(PinotTaskConfig.fromHelixTaskConfig(_taskConfig));
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_COMPLETED, 1L);
                  taskResult = new TaskResult(TaskResult.Status.COMPLETED, "Succeeded");
                } catch (TaskCancelledException e) {
                  taskResult = new TaskResult(TaskResult.Status.CANCELED, e.toString());
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_CANCELLED, 1L);
                } catch (FatalException e) {
                  taskResult = new TaskResult(TaskResult.Status.FATAL_FAILED, e.toString());
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_FATAL_FAILED, 1L);
                } catch (Exception e) {
                  taskResult = new TaskResult(TaskResult.Status.FAILED, e.toString());
                  minionMetrics.addMeteredTableValue(taskType, MinionMeter.NUMBER_TASKS_FAILED, 1L);
                }
                minionMetrics.addPhaseTiming(taskType, MinionQueryPhase.TASK_EXECUTION, System.nanoTime() - startTime);
                return taskResult;
              }

              @Override
              public void cancel() {
                _pinotTaskExecutor.cancel();
              }
            };
          } catch (Exception e) {
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
