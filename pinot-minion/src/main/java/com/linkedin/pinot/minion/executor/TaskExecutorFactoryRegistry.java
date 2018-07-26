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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.core.common.MinionConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Registry for all {@link PinotTaskExecutorFactory}.
 */
public class TaskExecutorFactoryRegistry {
  private final Map<String, PinotTaskExecutorFactory> _taskExecutorFactoryRegistry = new HashMap<>();

  public TaskExecutorFactoryRegistry() {
    registerTaskExecutorFactory(MinionConstants.ConvertToRawIndexTask.TASK_TYPE,
        new ConvertToRawIndexTaskExecutorFactory());
    registerTaskExecutorFactory(MinionConstants.PurgeTask.TASK_TYPE, new PurgeTaskExecutorFactory());
  }

  /**
   * Registers a task executor factory.
   *
   * @param taskType Task type
   * @param taskExecutorFactory Task executor factory associated with the task type
   */
  public void registerTaskExecutorFactory(@Nonnull String taskType,
      @Nonnull PinotTaskExecutorFactory taskExecutorFactory) {
    _taskExecutorFactoryRegistry.put(taskType, taskExecutorFactory);
  }

  /**
   * Returns all registered task types.
   *
   * @return Set of all registered task types
   */
  public Set<String> getAllTaskTypes() {
    return _taskExecutorFactoryRegistry.keySet();
  }

  /**
   * Returns the task executor factory for the given task type.
   *
   * @param taskType Task type
   * @return Task executor factory associated with the given task type
   */
  public PinotTaskExecutorFactory getTaskExecutorFactory(@Nonnull String taskType) {
    return _taskExecutorFactoryRegistry.get(taskType);
  }
}
