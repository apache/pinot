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
package org.apache.pinot.common.assignment;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;


public class InstanceAssignmentConfigUtils {
  private InstanceAssignmentConfigUtils() {
  }

  /**
   * Returns whether the COMPLETED segments should be relocated (offloaded from CONSUMING instances to COMPLETED
   * instances) for a LLC real-time table based on the given table config.
   * <p>COMPLETED segments should be relocated iff the COMPLETED instance assignment is configured or (for
   * backward-compatibility) COMPLETED server tag is overridden to be different from the CONSUMING server tag.
   */
  public static boolean shouldRelocateCompletedSegments(TableConfig tableConfig) {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    return (instanceAssignmentConfigMap != null
        && instanceAssignmentConfigMap.get(InstancePartitionsType.COMPLETED.toString()) != null)
        || TagNameUtils.isRelocateCompletedSegments(tableConfig.getTenantConfig());
  }

  /**
   * Returns whether the instance assignment is allowed for the given table config and instance partitions type.
   */
  public static boolean allowInstanceAssignment(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    if (TableConfigUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType)) {
      return true;
    }
    TableType tableType = tableConfig.getTableType();
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    switch (instancePartitionsType) {
      // Allow OFFLINE instance assignment if the offline table has it configured or (for backward-compatibility) is
      // using replica-group segment assignment
      case OFFLINE:
        return tableType == TableType.OFFLINE && ((instanceAssignmentConfigMap != null
            && instanceAssignmentConfigMap.get(InstancePartitionsType.OFFLINE.toString()) != null)
            || AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY
            .equalsIgnoreCase(tableConfig.getValidationConfig().getSegmentAssignmentStrategy()));
      // Allow CONSUMING/COMPLETED instance assignment if the real-time table has it configured
      case CONSUMING:
      case COMPLETED:
        return tableType == TableType.REALTIME && (instanceAssignmentConfigMap != null
            && instanceAssignmentConfigMap.get(instancePartitionsType.toString()) != null);
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
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    if (instanceAssignmentConfigMap != null) {
      InstanceAssignmentConfig instanceAssignmentConfig =
          instanceAssignmentConfigMap.get(instancePartitionsType.toString());
      if (instanceAssignmentConfig != null) {
        return instanceAssignmentConfig;
      }
    }

    // Generate default instance assignment config if it does not exist
    // Only allow default config for offline table with replica-group segment assignment for backward-compatibility

    InstanceTagPoolConfig tagPoolConfig =
        new InstanceTagPoolConfig(TagNameUtils.extractOfflineServerTag(tableConfig.getTenantConfig()), false, 0, null);

    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig;
    SegmentsValidationAndRetentionConfig segmentConfig = tableConfig.getValidationConfig();
    int numReplicaGroups = tableConfig.getReplication();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = segmentConfig.getReplicaGroupStrategyConfig();
    Preconditions.checkState(replicaGroupStrategyConfig != null, "Failed to find the replica-group strategy config");
    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();
    boolean minimizeDataMovement = segmentConfig.isMinimizeDataMovement();
    if (partitionColumn != null) {
      int numPartitions = tableConfig.getIndexingConfig().getSegmentPartitionConfig().getNumPartitions(partitionColumn);
      Preconditions.checkState(numPartitions > 0, "Number of partitions for column: %s is not properly configured",
          partitionColumn);
      replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups, 0, numPartitions,
          replicaGroupStrategyConfig.getNumInstancesPerPartition(), minimizeDataMovement, partitionColumn);
    } else {
      // If partition column is not configured, use replicaGroupStrategyConfig.getNumInstancesPerPartition() as
      // number of instances per replica-group for backward-compatibility
      replicaGroupPartitionConfig = new InstanceReplicaGroupPartitionConfig(true, 0, numReplicaGroups,
          replicaGroupStrategyConfig.getNumInstancesPerPartition(), 0, 0, minimizeDataMovement, null);
    }

    return new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig, null, minimizeDataMovement);
  }

  public static boolean isMirrorServerSetAssignment(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    // If the instance assignment config is not null and the partition selector is
    // MIRROR_SERVER_SET_PARTITION_SELECTOR,
    return tableConfig.getInstanceAssignmentConfigMap() != null
        && tableConfig.getInstanceAssignmentConfigMap().get(instancePartitionsType.toString()) != null
        && InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, instancePartitionsType)
        .getPartitionSelector()
        == InstanceAssignmentConfig.PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR;
  }
}
