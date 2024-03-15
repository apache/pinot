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
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class InstanceReplicaGroupPartitionConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Whether to use replica-group based selection, false by default")
  private final boolean _replicaGroupBased;

  @JsonPropertyDescription(
      "Number of instances to select for non-replica-group based selection, select all instances if not specified")
  private final int _numInstances;

  @JsonPropertyDescription("Number of replica-groups for replica-group based selection")
  private final int _numReplicaGroups;

  @JsonPropertyDescription(
      "Number of instances per replica-group for replica-group based selection, select as many instances as possible"
          + " if not specified")
  private final int _numInstancesPerReplicaGroup;

  @JsonPropertyDescription(
      "Number of partitions for replica-group based selection, do not partition the replica-group (1 partition) if "
          + "not specified")
  private final int _numPartitions;

  @JsonPropertyDescription(
      "Number of instances per partition (within a replica-group) for replica-group based selection, select all "
          + "instances if not " + "specified")
  private final int _numInstancesPerPartition;

  @JsonPropertyDescription(
      "Name of the column used for partition, if not provided table level replica group will be used")
  private final String _partitionColumn;

  // TODO: remove this config in the next official release
  @Deprecated
  private final boolean _minimizeDataMovement;

  @JsonCreator
  public InstanceReplicaGroupPartitionConfig(@JsonProperty("replicaGroupBased") boolean replicaGroupBased,
      @JsonProperty("numInstances") int numInstances, @JsonProperty("numReplicaGroups") int numReplicaGroups,
      @JsonProperty("numInstancesPerReplicaGroup") int numInstancesPerReplicaGroup,
      @JsonProperty("numPartitions") int numPartitions,
      @JsonProperty("numInstancesPerPartition") int numInstancesPerPartition,
      @JsonProperty("minimizeDataMovement") boolean minimizeDataMovement,
      @Nullable @JsonProperty("partitionColumn") String partitionColumn) {
    _replicaGroupBased = replicaGroupBased;
    _numInstances = numInstances;
    _numReplicaGroups = numReplicaGroups;
    _numInstancesPerReplicaGroup = numInstancesPerReplicaGroup;
    _numPartitions = numPartitions;
    _numInstancesPerPartition = numInstancesPerPartition;
    _minimizeDataMovement = minimizeDataMovement;
    _partitionColumn = partitionColumn;
  }

  public boolean isReplicaGroupBased() {
    return _replicaGroupBased;
  }

  public int getNumInstances() {
    return _numInstances;
  }

  public int getNumReplicaGroups() {
    return _numReplicaGroups;
  }

  public int getNumInstancesPerReplicaGroup() {
    return _numInstancesPerReplicaGroup;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public int getNumInstancesPerPartition() {
    return _numInstancesPerPartition;
  }

  public boolean isMinimizeDataMovement() {
    return _minimizeDataMovement;
  }

  @Nullable
  public String getPartitionColumn() {
    return _partitionColumn;
  }
}
