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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.json.JSONException;
import org.json.JSONObject;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableTaskConfig {
  private static final String TASK_TYPE_CONFIGS_MAP_KEY = "taskTypeConfigsMap";

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
  public String toString() {
    JSONObject jsonTaskConfigsMap = new JSONObject();
    try {
      jsonTaskConfigsMap.put(TASK_TYPE_CONFIGS_MAP_KEY, _taskTypeConfigsMap);
      return jsonTaskConfigsMap.toString(2);
    } catch (JSONException e) {
      return e.toString();
    }
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
