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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskConfig;


public class PinotTaskConfig {
  public static final String TASK_COMMAND_KEY = "TASK_COMMAND";
  public static final String TASK_ID_KEY = "TASK_ID";

  private final String _taskType;
  private final Map<String, String> _configs;

  public PinotTaskConfig(@Nonnull String taskType, @Nonnull Map<String, String> configs) {
    _taskType = taskType;
    _configs = configs;
  }

  public String getTaskType() {
    return _taskType;
  }

  public Map<String, String> getConfigs() {
    return _configs;
  }

  @Nonnull
  public TaskConfig toHelixTaskConfig(@Nonnull String taskName) {
    return new TaskConfig(_taskType, _configs, taskName, null);
  }

  @Nonnull
  public static PinotTaskConfig fromHelixTaskConfig(@Nonnull TaskConfig helixTaskConfig) {
    Map<String, String> configs = helixTaskConfig.getConfigMap();

    // Inside Helix task configs, there are two extra fields: TASK_COMMAND, TASK_ID which need to be removed
    // TASK_COMMAND field stores the task type
    String taskType = configs.remove(TASK_COMMAND_KEY);
    configs.remove(TASK_ID_KEY);
    return new PinotTaskConfig(taskType, configs);
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    PinotTaskConfig that = (PinotTaskConfig) o;

    return EqualityUtils.isEqual(_taskType, that._taskType) && EqualityUtils.isEqual(_configs, that._configs);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_taskType);
    result = EqualityUtils.hashCodeOf(result, _configs);
    return result;
  }

  @Override
  public String toString() {
    return "Task Type: " + _taskType + ", Configs: " + _configs;
  }
}
