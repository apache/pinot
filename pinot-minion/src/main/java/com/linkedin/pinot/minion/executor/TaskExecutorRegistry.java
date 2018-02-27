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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.core.common.MinionConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Registry for all {@link PinotTaskExecutor}.
 */
public class TaskExecutorRegistry {
  private final Map<String, Class<? extends PinotTaskExecutor>> _taskExecutorRegistry = new HashMap<>();

  public TaskExecutorRegistry() {
    registerTaskExecutorClass(MinionConstants.ConvertToRawIndexTask.TASK_TYPE, ConvertToRawIndexTaskExecutor.class);
    registerTaskExecutorClass(MinionConstants.PurgeTask.TASK_TYPE, PurgeTaskExecutor.class);
  }

  /**
   * Register a class of task executor.
   * <p>The reason why we register the class here is that for each new task we need to initialize a new task executor.
   *
   * @param taskType Task type
   * @param taskExecutorClass Class of task executor to be registered
   */
  public void registerTaskExecutorClass(@Nonnull String taskType,
      @Nonnull Class<? extends PinotTaskExecutor> taskExecutorClass) {
    _taskExecutorRegistry.put(taskType, taskExecutorClass);
  }

  /**
   * Get all registered task types.
   *
   * @return Set of all registered task types
   */
  @Nonnull
  public Set<String> getAllTaskTypes() {
    return _taskExecutorRegistry.keySet();
  }

  /**
   * Get the class of task executor for the given task type.
   *
   * @param taskType Task type
   * @return Class of task executor for the given task type
   */
  @Nullable
  public Class<? extends PinotTaskExecutor> getTaskExecutorClass(@Nonnull String taskType) {
    return _taskExecutorRegistry.get(taskType);
  }
}
