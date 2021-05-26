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
package org.apache.pinot.core.periodictask;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


/**
 * Execution status of a task upon trying to enqueue the task with a task execution engine.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskExecutionResult {
  private final Status _status;
  private final String _description;
  private final String _taskName;

  @JsonCreator
  public TaskExecutionResult(@JsonProperty(value = "status", required = true) Status status,
      @JsonProperty(value = "description", required = true) String description,
      @JsonProperty(value = "taskName", required = true) String taskName) {
    _status = status;
    _description = description;
    _taskName = taskName;
  }

  @JsonProperty
  public Status getStatus() {
    return _status;
  }

  @JsonProperty
  public String getDescription() {
    return _description;
  }

  @JsonProperty
  public String getTaskName() {
    return _taskName;
  }

  public enum Status {
    NO_OP, DONE, FAILED, IN_PROGRESS
  }
}
