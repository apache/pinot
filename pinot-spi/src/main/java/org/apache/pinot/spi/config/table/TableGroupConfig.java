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
import java.util.Objects;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;


/**
 * TableGroupConfig can be used to set the configs for a table-group, which mainly includes
 * the InstanceAssignmentConfig that specifies properties like number of replica-groups,
 * number of replica-group partitions, etc. The config for a table-group can be viewed/edited
 * in the UI at /groups/{groupName}.
 */
public class TableGroupConfig {
  private static final String GROUP_NAME_CONFIG_KEY = "groupName";
  public static final String ASSIGNMENT_CONFIG_KEY = "instanceAssignmentConfig";

  @JsonPropertyDescription("Name of the table-group")
  private final String _groupName;

  @JsonPropertyDescription("Instance assignment config for the table-group")
  private InstanceAssignmentConfig _instanceAssignmentConfig;

  @JsonCreator
  public TableGroupConfig(
      @JsonProperty(value = GROUP_NAME_CONFIG_KEY, required = true) String groupName,
      @JsonProperty(value = ASSIGNMENT_CONFIG_KEY, required = true)
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableGroupConfig that = (TableGroupConfig) o;
    return _groupName.equals(that._groupName) && _instanceAssignmentConfig.equals(that._instanceAssignmentConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_groupName, _instanceAssignmentConfig);
  }
}
