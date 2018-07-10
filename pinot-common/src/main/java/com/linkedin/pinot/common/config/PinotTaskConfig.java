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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskConfig;


public class PinotTaskConfig {
  private static final String TASK_ID_KEY = "TASK_ID";
  private static final String TASK_COMMAND_KEY = "TASK_COMMAND";
  private static final String TASK_TARGET_PARTITION_KEY = "TASK_TARGET_PARTITION";

  private final String _taskType;
  private final Map<String, String> _configs;

  public PinotTaskConfig(@Nonnull String taskType, @Nonnull Map<String, String> configs) {
    _taskType = taskType;
    _configs = configs;
  }

  @Nonnull
  public String getTaskType() {
    return _taskType;
  }

  @Nonnull
  public Map<String, String> getConfigs() {
    return _configs;
  }

  @Nonnull
  public TaskConfig toHelixTaskConfig(@Nonnull String taskName) {
    return new TaskConfig(_taskType, new HashMap<>(_configs), taskName, null);
  }

  @Nonnull
  public static PinotTaskConfig fromHelixTaskConfig(@Nonnull TaskConfig helixTaskConfig) {
    Map<String, String> configs = new HashMap<>(helixTaskConfig.getConfigMap());

    // Inside Helix task config map, there are 3 extra Helix properties: TASK_COMMAND, TASK_ID, TASK_TARGET_PARTITION
    configs.remove(TASK_ID_KEY);
    configs.remove(TASK_COMMAND_KEY);
    configs.remove(TASK_TARGET_PARTITION_KEY);

    return new PinotTaskConfig(helixTaskConfig.getCommand(), configs);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_taskType);
    result = EqualityUtils.hashCodeOf(result, _configs);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof PinotTaskConfig) {
      PinotTaskConfig that = (PinotTaskConfig) obj;
      return this._taskType.equals(that._taskType) && this._configs.equals(that._configs);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Task Type: " + _taskType + ", Configs: " + _configs;
  }
}
