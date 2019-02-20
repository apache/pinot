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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import org.apache.pinot.common.utils.EqualityUtils;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableTaskConfig {

  @ConfigKey("taskConfig")
  @UseChildKeyHandler(TaskConfigMapChildKeyHandler.class)
  private Map<String, Map<String, String>> _taskTypeConfigsMap;

  public void setTaskTypeConfigsMap(Map<String, Map<String, String>> taskTypeConfigsMap) {
    _taskTypeConfigsMap = taskTypeConfigsMap;
  }

  public Map<String, Map<String, String>> getTaskTypeConfigsMap() {
    return _taskTypeConfigsMap;
  }

  @JsonIgnore
  public boolean isTaskTypeEnabled(String taskType) {
    return _taskTypeConfigsMap.containsKey(taskType);
  }

  @JsonIgnore
  public Map<String, String> getConfigsForTaskType(String taskType) {
    return _taskTypeConfigsMap.get(taskType);
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    TableTaskConfig that = (TableTaskConfig) o;

    return EqualityUtils.isEqual(_taskTypeConfigsMap, that._taskTypeConfigsMap);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_taskTypeConfigsMap);
    return result;
  }
}
