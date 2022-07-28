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
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


/**
 * TableGroupConfig can be used to set the configs for a table-group, which mainly includes
 * the InstanceAssignmentConfig that specifies properties like number of replica-groups,
 * number of replica-group partitions, etc. The config for a table-group can be viewed/edited
 * in the UI at /groups/{groupName}.
 */
public class TableGroupConfig {
  private static final String GROUP_NAME_CONFIG_KEY = "groupName";
  public static final String IN_SYNC_CONFIG_KEY = "keepInstancePartitionsInSync";
  public static final String ASSIGNMENT_CONFIG_KEY = "instanceAssignmentConfigMap";

  @JsonPropertyDescription("Name of the table-group")
  private final String _groupName;

  @JsonPropertyDescription("If set to true, will ensure instance partitions are same for all instance-partitions type")
  private final boolean _keepInstancePartitionsInSync;

  @JsonPropertyDescription("Instance assignment config for the table-group")
  private Map<InstancePartitionsType, InstanceAssignmentConfig> _instanceAssignmentConfigMap;

  @JsonCreator
  public TableGroupConfig(
      @JsonProperty(value = GROUP_NAME_CONFIG_KEY, required = true) String groupName,
      @JsonProperty(value = IN_SYNC_CONFIG_KEY, required = true) boolean keepInstancePartitionsInSync,
      @JsonProperty(value = ASSIGNMENT_CONFIG_KEY, required = true) Map<InstancePartitionsType,
      InstanceAssignmentConfig> instanceAssignmentConfigMap) {
    Preconditions.checkArgument(_instanceAssignmentConfigMap.size() > 0, "instance assignment config map should "
        + "contain config for at-least 1 instance-partitions type (OFFLINE/CONSUMING/COMPLETED)");
    _groupName = groupName;
    _keepInstancePartitionsInSync = keepInstancePartitionsInSync;
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
  }

  public Map<InstancePartitionsType, InstanceAssignmentConfig> getInstanceAssignmentConfigMap() {
    return _instanceAssignmentConfigMap;
  }

  public boolean getKeepInstancePartitionsInSync() {
    return _keepInstancePartitionsInSync;
  }

  public String getGroupName() {
    return _groupName;
  }

  public void setInstanceAssignmentConfigMap(
      Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap) {
    _instanceAssignmentConfigMap = instanceAssignmentConfigMap;
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
    return _keepInstancePartitionsInSync == that._keepInstancePartitionsInSync && _groupName.equals(that._groupName)
        && _instanceAssignmentConfigMap.equals(that._instanceAssignmentConfigMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_groupName, _keepInstancePartitionsInSync, _instanceAssignmentConfigMap);
  }
}
