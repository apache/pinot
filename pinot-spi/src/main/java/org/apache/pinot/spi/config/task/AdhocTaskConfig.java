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
package org.apache.pinot.spi.config.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * AdhocTaskConfig configuration.
 * <pre>
 * Example:
 * {
 *   "taskType": "SegmentGenerationAndPushTask",
 *   "tableName": "myTable",
 *   "taskName": "myTask-0",
 *   "taskConfigs": {
 *     "inputDirURI": "s3://my-bucket/my-file.json",
 *     "input.fs.className": "org.apache.pinot.plugin.filesystem.S3PinotFS",
 *     "input.fs.prop.accessKey": "<aws-access-key>",
 *     "input.fs.prop.secretKey": "<aws-secret-key>",
 *     "input.fs.prop.region": "us-west-2"
 *   }
 * }
 * </pre>
 */
public class AdhocTaskConfig extends BaseJsonConfig {
  private String _taskType;
  private String _tableName;
  private String _taskName;
  private Map<String, String> _taskConfigs;

  @JsonCreator
  public AdhocTaskConfig(@JsonProperty(value = "taskType", required = true) String taskType,
      @JsonProperty(value = "tableName", required = true) String tableName,
      @JsonProperty(value = "taskName") @Nullable String taskName,
      @JsonProperty("taskConfigs") @Nullable Map<String, String> taskConfigs) {
    Preconditions.checkArgument(taskType != null, "'taskType' must be configured");
    Preconditions.checkArgument(tableName != null, "'tableName' must be configured");
    Preconditions.checkArgument(taskName == null || !taskName.contains("/"),
        "'taskName' must not contain path separator '/'");
    _taskType = taskType;
    _tableName = tableName;
    _taskName = taskName;
    _taskConfigs = taskConfigs;
  }

  public String getTaskType() {
    return _taskType;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getTaskName() {
    return _taskName;
  }

  public Map<String, String> getTaskConfigs() {
    return _taskConfigs;
  }
}
