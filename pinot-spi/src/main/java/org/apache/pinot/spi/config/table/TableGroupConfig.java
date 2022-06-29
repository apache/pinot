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
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;


public class TableGroupConfig {

  @JsonPropertyDescription("Name of the table-group")
  private final String _groupName;

  @JsonPropertyDescription("Instance assignment config for the table-group")
  private InstanceAssignmentConfig _instanceAssignmentConfig;

  @JsonCreator
  public TableGroupConfig(
      @JsonProperty(value = "groupName", required = true) String groupName,
      @JsonProperty(value = "instanceAssignmentConfig", required = true)
          InstanceAssignmentConfig instanceAssignmentConfig) {
    Preconditions.checkArgument(instanceAssignmentConfig != null, "instanceAssignmentConfig should not be null");
    _groupName = groupName;
    _instanceAssignmentConfig = instanceAssignmentConfig;
  }

  public InstanceAssignmentConfig getInstanceAssignmentConfig() {
    return _instanceAssignmentConfig;
  }

  public String getGroupName() {
    return _groupName;
  }

  public void setInstanceAssignmentConfig(InstanceAssignmentConfig instanceAssignmentConfig) {
    _instanceAssignmentConfig = instanceAssignmentConfig;
  }
}
