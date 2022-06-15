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
package org.apache.pinot.common.metadata.task;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants;


public class TaskZKMetadata implements ZKMetadata {
  private final ZNRecord _znRecord;
  private final Map<String, String> _taskProperties;

  public TaskZKMetadata(ZNRecord znRecord) {
    _znRecord = znRecord;
    _taskProperties = _znRecord.getSimpleFields();
  }

  public TaskZKMetadata(String taskId, TaskType taskType) {
    _znRecord = new ZNRecord(taskId);
    _taskProperties = new HashMap<>();
    _taskProperties.put(CommonConstants.Task.TASK_ID, taskId);
    _taskProperties.put(CommonConstants.Task.TASK_TYPE, taskType.toString());
    _taskProperties.put(CommonConstants.Task.TASK_SUBMISSION_TIME, Long.toString(System.currentTimeMillis()));
  }

  public void setSimpleField(String key, String value) {
    _taskProperties.put(key, value);
  }

  public String getSimpleField(String key) {
    return _taskProperties.get(key);
  }

  public Map<String, String> getTaskProperties() {
    return _taskProperties;
  }

  @Override
  public ZNRecord toZNRecord() {
    _znRecord.setSimpleFields(_taskProperties);
    return _znRecord;
  }
}
