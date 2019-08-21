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
package org.apache.pinot.common.config.instance;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.common.assignment.InstancePartitionsType;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Segment.AssignmentStrategy;


public class InstanceAssignmentConfigUtils {
  private InstanceAssignmentConfigUtils() {
  }

  /**
   * Returns whether the instance assignment is allowed for the given table config.
   */
  public static boolean allowInstanceAssignment(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    TableType tableType = tableConfig.getTableType();
    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        tableConfig.getInstanceAssignmentConfigMap();
    switch (instancePartitionsType) {
      // Allow OFFLINE instance assignment if the offline table has it configured or (for backward-compatibility) is
      // using replica-group segment assignment
      case OFFLINE:
        return tableType == TableType.OFFLINE && ((instanceAssignmentConfigMap != null && instanceAssignmentConfigMap
            .containsKey(InstancePartitionsType.OFFLINE))
            || AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY
            .equalsIgnoreCase(tableConfig.getValidationConfig().getSegmentAssignmentStrategy()));
      // Allow CONSUMING/COMPLETED instance assignment if the real-time table has it configured
      case CONSUMING:
      case COMPLETED:
        return tableType == TableType.REALTIME && (instanceAssignmentConfigMap != null && instanceAssignmentConfigMap
            .containsKey(instancePartitionsType));
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Extracts or generates default instance assignment config from the given table config.
   */
  public static InstanceAssignmentConfig getInstanceAssignmentConfig(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    Preconditions.checkState(allowInstanceAssignment(tableConfig, instancePartitionsType),
        "Instance assignment is not allowed for the given table config");

    // Use the instance assignment config from the table config if it exists
    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        tableConfig.getInstanceAssignmentConfigMap();
    if (instanceAssignmentConfigMap != null) {
      if (instanceAssignmentConfigMap.containsKey(instancePartitionsType)) {
        return instanceAssignmentConfigMap.get(instancePartitionsType);
      }
    }

    // Generate default instance assignment config if it does not exist
    // Only allow default config for offline table with replica-group segment assignment for backward-compatibility
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig();

    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig();
    tagPoolConfig.setTag(TagNameUtils.getOfflineTagForTenant(tableConfig.getTenantConfig().getServer()));
    instanceAssignmentConfig.setTagPoolConfig(tagPoolConfig);

    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig();
    replicaGroupPartitionConfig.setReplicaGroupBased(true);
    SegmentsValidationAndRetentionConfig segmentConfig = tableConfig.getValidationConfig();
    replicaGroupPartitionConfig.setNumReplicaGroups(segmentConfig.getReplicationNumber());
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = segmentConfig.getReplicaGroupStrategyConfig();
    Preconditions.checkState(replicaGroupStrategyConfig != null, "Failed to find the replica-group strategy config");
    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();
    if (partitionColumn != null) {
      int numPartitions = tableConfig.getIndexingConfig().getSegmentPartitionConfig().getNumPartitions(partitionColumn);
      Preconditions.checkState(numPartitions > 0, "Number of partitions for column: %s is not properly configured",
          partitionColumn);
      replicaGroupPartitionConfig.setNumPartitions(numPartitions);
      replicaGroupPartitionConfig.setNumInstancesPerPartition(replicaGroupStrategyConfig.getNumInstancesPerPartition());
    } else {
      // If partition column is not configured, use replicaGroupStrategyConfig.getNumInstancesPerPartition() as
      // number of instances per replica-group for backward-compatibility
      replicaGroupPartitionConfig
          .setNumInstancesPerReplicaGroup(replicaGroupStrategyConfig.getNumInstancesPerPartition());
    }
    instanceAssignmentConfig.setReplicaGroupPartitionConfig(replicaGroupPartitionConfig);

    return instanceAssignmentConfig;
  }
}
