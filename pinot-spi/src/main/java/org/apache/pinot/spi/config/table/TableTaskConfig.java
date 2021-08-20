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


public class TableTaskConfig extends BaseJsonConfig {
  private final Map<String, Map<String, String>> _taskTypeConfigsMap;

  @JsonCreator
  public TableTaskConfig(@JsonProperty(value = "taskTypeConfigsMap", required = true) Map<String, Map<String, String>> taskTypeConfigsMap) {
    Preconditions.checkArgument(taskTypeConfigsMap != null, "'taskTypeConfigsMap' must be configured");
    _taskTypeConfigsMap = taskTypeConfigsMap;
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
}
