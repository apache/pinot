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
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.common.utils.InstancePartitionsType;


public class InstanceAssignmentConfigUtils {
  private InstanceAssignmentConfigUtils() {
  }

  public static InstanceAssignmentConfig getInstanceAssignmentConfig(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    // Use the instance assignment config from the table config if it exists
    Map<InstancePartitionsType, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        tableConfig.getInstanceAssignmentConfigMap();
    if (instanceAssignmentConfigMap != null) {
      if (instanceAssignmentConfigMap.containsKey(instancePartitionsType)) {
        return instanceAssignmentConfigMap.get(instancePartitionsType);
      }
      // Use the CONSUMING config for COMPLETED segments if COMPLETED config does not exist
      if (instancePartitionsType == InstancePartitionsType.COMPLETED && instanceAssignmentConfigMap
          .containsKey(InstancePartitionsType.CONSUMING)) {
        return instanceAssignmentConfigMap.get(InstancePartitionsType.CONSUMING);
      }
    }

    // Generate default instance assignment config if it does not exist
    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig();

    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig();
    String serverTag =
        TagNameUtils.getServerTagFromTableConfigAndInstancePartitionsType(tableConfig, instancePartitionsType);
    tagPoolConfig.setTag(serverTag);
    instanceAssignmentConfig.setTagPoolConfig(tagPoolConfig);

    InstanceReplicaPartitionConfig replicaPartitionConfig = new InstanceReplicaPartitionConfig();
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY
        .equalsIgnoreCase(validationConfig.getSegmentAssignmentStrategy())) {
      replicaPartitionConfig.setReplicaGroupBased(true);
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        replicaPartitionConfig.setNumReplicas(validationConfig.getReplicationNumber());
        ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
            tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
        Preconditions
            .checkState(replicaGroupStrategyConfig != null, "Failed to find the replica-group strategy config");
        String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();
        if (partitionColumn != null) {
          int numPartitions =
              tableConfig.getIndexingConfig().getSegmentPartitionConfig().getNumPartitions(partitionColumn);
          Preconditions.checkState(numPartitions > 0, "Number of partitions for column: %s is not properly configured",
              partitionColumn);
          replicaPartitionConfig.setNumPartitions(numPartitions);
          replicaPartitionConfig.setNumServersPerPartition(replicaGroupStrategyConfig.getNumInstancesPerPartition());
        } else {
          // If partition column is not configured, use replicaGroupStrategyConfig.getNumInstancesPerPartition() as
          // number of servers per replica for backward-compatibility
          replicaPartitionConfig.setNumServersPerReplica(replicaGroupStrategyConfig.getNumInstancesPerPartition());
        }
      } else {
        replicaPartitionConfig.setNumReplicas(validationConfig.getReplicasPerPartitionNumber());
      }
    }
    instanceAssignmentConfig.setReplicaPartitionConfig(replicaPartitionConfig);

    return instanceAssignmentConfig;
  }
}
