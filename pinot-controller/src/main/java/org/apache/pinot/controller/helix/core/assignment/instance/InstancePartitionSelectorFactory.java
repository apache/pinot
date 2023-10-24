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
package org.apache.pinot.controller.helix.core.assignment.instance;

import java.util.Arrays;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;


public class InstancePartitionSelectorFactory {

  private InstancePartitionSelectorFactory() {
  }

  public static InstancePartitionSelector getInstance(InstanceAssignmentConfig.PartitionSelector partitionSelector,
      InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig, String tableNameWithType,
      InstancePartitions existingInstancePartitions) {
    return getInstance(partitionSelector, instanceReplicaGroupPartitionConfig, tableNameWithType,
        existingInstancePartitions, null);
  }

  public static InstancePartitionSelector getInstance(InstanceAssignmentConfig.PartitionSelector partitionSelector,
      InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig, String tableNameWithType,
      InstancePartitions existingInstancePartitions, InstancePartitions preConfiguredInstancePartitions
  ) {
    switch (partitionSelector) {
      case FD_AWARE_INSTANCE_PARTITION_SELECTOR:
        return new FDAwareInstancePartitionSelector(instanceReplicaGroupPartitionConfig, tableNameWithType,
            existingInstancePartitions);
      case INSTANCE_REPLICA_GROUP_PARTITION_SELECTOR:
        return new InstanceReplicaGroupPartitionSelector(instanceReplicaGroupPartitionConfig, tableNameWithType,
            existingInstancePartitions);
      case MIRROR_SERVER_SET_PARTITION_SELECTOR:
        return new MirrorServerSetInstancePartitionSelector(instanceReplicaGroupPartitionConfig, tableNameWithType,
            existingInstancePartitions, preConfiguredInstancePartitions);
      default:
        throw new IllegalStateException("Unexpected PartitionSelector: " + partitionSelector + ", should be from"
            + Arrays.toString(InstanceAssignmentConfig.PartitionSelector.values()));
    }
  }
}
