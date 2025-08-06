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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.task.TableTaskTypeConfig;
import org.apache.pinot.spi.config.table.task.TaskType;


public class TableTaskConfig extends BaseJsonConfig {

  // A structured representation of task configs. It will contain few task types and their configs.
  // Eventually, this will replace _taskTypeConfigsMap.
  private final Map<TaskType, TableTaskTypeConfig> _taskTypeConfigs;

  // A map of task type to its configs.
  // This is primarily used for backward compatibility until all taskTypes use _taskTypeConfigs
  // This map will contain all configs for each task type, including those that are defined in _taskTypeConfigs.
  private final Map<String, Map<String, String>> _taskTypeConfigsMap;

  public final static String MINION_ALLOW_DOWNLOAD_FROM_SERVER = "allowDownloadFromServer";
  public final static boolean DEFAULT_MINION_ALLOW_DOWNLOAD_FROM_SERVER = false;

  @JsonCreator
  public TableTaskConfig(@JsonProperty(value = "taskTypeConfigsMap", required = true)
      Map<String, Map<String, String>> taskTypeConfigsMap) {
    Preconditions.checkArgument(taskTypeConfigsMap != null, "'taskTypeConfigsMap' must be configured");
    _taskTypeConfigsMap = taskTypeConfigsMap;

    // populate _taskTypeConfigs from _taskTypeConfigsMap if the key is a valid TaskType
    _taskTypeConfigs = taskTypeConfigsMap.entrySet().stream()
        .filter(entry -> TaskType.isValidTaskType(entry.getKey()))
        .collect(java.util.stream.Collectors.toMap(
            entry -> TaskType.valueOf(entry.getKey()),
            entry -> TaskType.createTaskTypeConfig(entry.getKey(), entry.getValue())));
  }

  @JsonProperty
  public Map<String, Map<String, String>> getTaskTypeConfigsMap() {
    return _taskTypeConfigsMap;
  }

  public boolean isTaskTypeEnabled(String taskType) {
    return _taskTypeConfigsMap.containsKey(taskType);
  }

  public Map<String, String> getConfigsForTaskType(String taskType) {
    return _taskTypeConfigsMap.get(taskType);
  }

  public TableTaskTypeConfig getTaskConfig(TaskType taskType) {
    return _taskTypeConfigs.get(taskType);
  }
}
