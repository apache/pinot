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
package org.apache.pinot.common.minion;

import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * MinionRequestURLBuilder is the helper class to generate URLs for task APIs.
 */
public class MinionRequestURLBuilder {

  private final String _baseUrl;

  private MinionRequestURLBuilder(String baseUrl) {
    _baseUrl = StringUtils.removeEnd(baseUrl, "/");
  }

  public static MinionRequestURLBuilder baseUrl(String baseUrl) {
    return new MinionRequestURLBuilder(baseUrl);
  }

  public String forTaskSchedule(@Nullable String taskType, @Nullable String tableNameWithType) {
    String url = StringUtil.join("/", _baseUrl, "tasks/schedule");
    if (taskType != null && tableNameWithType != null) {
      return url + "?taskType=" + taskType + "&tableName=" + tableNameWithType;
    } else if (taskType != null) {
      return url + "?taskType=" + taskType;
    } else if (tableNameWithType != null) {
      return url + "?tableName=" + tableNameWithType;
    } else {
      return url;
    }
  }

  public String forListAllTasks(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType, "tasks");
  }

  public String forListAllTaskTypes() {
    return StringUtil.join("/", _baseUrl, "tasks/tasktypes");
  }

  public String forTaskTypeState(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType, "state");
  }

  public String forTasksStates(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType, "taskstates");
  }

  public String forTaskState(String taskName) {
    return StringUtil.join("/", _baseUrl, "tasks/task", taskName, "state");
  }

  public String forTaskConfig(String taskName) {
    return StringUtil.join("/", _baseUrl, "tasks/task", taskName, "config");
  }

  public String forTaskTypeCleanup(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType, "clenaup");
  }

  public String forTaskTypeStop(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType, "stop");
  }

  public String forTaskTypeResume(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType, "resume");
  }

  public String forTaskTypeDelete(String taskType) {
    return StringUtil.join("/", _baseUrl, "tasks", taskType);
  }

  public String forTaskExecute() {
    return StringUtil.join("/", _baseUrl, "tasks/execute");
  }
}
