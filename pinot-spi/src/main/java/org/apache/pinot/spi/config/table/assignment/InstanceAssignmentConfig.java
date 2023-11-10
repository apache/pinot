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
package org.apache.pinot.spi.config.table.assignment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class InstanceAssignmentConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Configuration for the strategy to assign instances to partitions")
  private final PartitionSelector _partitionSelector;
  @JsonPropertyDescription("Configuration for the instance tag and pool of the instance assignment (mandatory)")
  private final InstanceTagPoolConfig _tagPoolConfig;

  @JsonPropertyDescription("Configuration for the instance constraints of the instance assignment,"
      + " which filters out unqualified instances and sorts instances for picking priority")
  private final InstanceConstraintConfig _constraintConfig;

  @JsonPropertyDescription(
      "Configuration for the instance replica-group and partition of the instance assignment (mandatory)")
  private final InstanceReplicaGroupPartitionConfig _replicaGroupPartitionConfig;

  @JsonCreator
  public InstanceAssignmentConfig(
      @JsonProperty(value = "tagPoolConfig", required = true) InstanceTagPoolConfig tagPoolConfig,
      @JsonProperty("constraintConfig") @Nullable InstanceConstraintConfig constraintConfig,
      @JsonProperty(value = "replicaGroupPartitionConfig", required = true)
          InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      @JsonProperty("partitionSelector") @Nullable String partitionSelector) {
    Preconditions.checkArgument(tagPoolConfig != null, "'tagPoolConfig' must be configured");
    Preconditions
        .checkArgument(replicaGroupPartitionConfig != null, "'replicaGroupPartitionConfig' must be configured");
    _tagPoolConfig = tagPoolConfig;
    _constraintConfig = constraintConfig;
    _replicaGroupPartitionConfig = replicaGroupPartitionConfig;
    _partitionSelector =
        partitionSelector == null ? PartitionSelector.INSTANCE_REPLICA_GROUP_PARTITION_SELECTOR
            : PartitionSelector.valueOf(partitionSelector);
  }

  public InstanceAssignmentConfig(InstanceTagPoolConfig tagPoolConfig, InstanceConstraintConfig constraintConfig,
      InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig) {
    this(tagPoolConfig, constraintConfig, replicaGroupPartitionConfig, null);
  }

  public PartitionSelector getPartitionSelector() {
    return _partitionSelector;
  }

  public InstanceTagPoolConfig getTagPoolConfig() {
    return _tagPoolConfig;
  }

  @Nullable
  public InstanceConstraintConfig getConstraintConfig() {
    return _constraintConfig;
  }

  public InstanceReplicaGroupPartitionConfig getReplicaGroupPartitionConfig() {
    return _replicaGroupPartitionConfig;
  }

  public enum PartitionSelector {
    FD_AWARE_INSTANCE_PARTITION_SELECTOR, INSTANCE_REPLICA_GROUP_PARTITION_SELECTOR,
    MIRROR_SERVER_SET_PARTITION_SELECTOR
  }
}
