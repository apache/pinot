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
package org.apache.pinot.controller.helix.core.assignment.segment;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment for LLC real-time table.
 * <ul>
 *   <li>
 *     For the CONSUMING segments, it is very similar to replica-group based segment assignment with the following
 *     differences:
 *     <ul>
 *       <li>
 *         1. Within a replica, all segments of the same partition (steam partition) are always assigned to exactly one
 *         server, and because of that we can directly assign or rebalance the CONSUMING segments to the servers based
 *         on the partition id
 *       </li>
 *       <li>
 *         2. Partition id for an instance is derived from the index of the instance (within the replica-group for
 *         replica-group based assignment), instead of explicitly stored in the instance partitions
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     For the COMPLETED segments, rebalance segments the same way as OfflineSegmentAssignment.
 *   </li>
 * </ul>
 */
public class RealtimeSegmentAssignment implements SegmentAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentAssignment.class);

  private String _realtimeTableName;
  private int _replication;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _realtimeTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    LOGGER.info("Initialized RealtimeSegmentAssignment with replication: {} for table: {}", _replication,
        _realtimeTableName);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
        _realtimeTableName);
    Preconditions
        .checkState(instancePartitions.getNumPartitions() == 1, "Instance partitions: %s should contain 1 partition",
            instancePartitions.getName());
    LOGGER.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _realtimeTableName);

    List<String> instancesAssigned = assignSegment(segmentName, instancePartitions);
    LOGGER.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _realtimeTableName);
    return instancesAssigned;
  }

  /**
   * Helper method to assign instances based on the segment partition id and instance partitions.
   */
  private List<String> assignSegment(String segmentName, InstancePartitions instancePartitions) {
    int partitionId = new LLCSegmentName(segmentName).getPartitionId();

    if (instancePartitions.getNumReplicas() == 1) {
      // Non-replica-group based assignment:
      // Uniformly spray the partitions and replicas across the instances.
      // E.g. (6 servers, 3 partitions, 4 replicas)
      // "0_0": [i0,  i1,  i2,  i3,  i4,  i5  ]
      //         p0r0 p0r1 p0r2 p1r3 p1r0 p1r1
      //         p1r2 p1r3 p2r0 p2r1 p2r2 p2r3

      List<String> instances =
          SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
      int numInstances = instances.size();
      List<String> instancesAssigned = new ArrayList<>(_replication);
      for (int replicaId = 0; replicaId < _replication; replicaId++) {
        instancesAssigned.add(instances.get((partitionId * _replication + replicaId) % numInstances));
      }
      return instancesAssigned;
    } else {
      // Replica-group based assignment:
      // Within a replica, uniformly spray the partitions across the instances.
      // E.g. (within a replica, 3 servers, 6 partitions)
      // "0_0": [i0, i1, i2]
      //         p0  p1  p2
      //         p3  p4  p5

      int numReplicas = instancePartitions.getNumReplicas();
      if (numReplicas != _replication) {
        LOGGER.warn(
            "Number of replicas in instance partitions {}: {} does not match replication in table config: {} for table: {}, use: {}",
            instancePartitions.getName(), numReplicas, _replication, _realtimeTableName, numReplicas);
      }

      List<String> instancesAssigned = new ArrayList<>(numReplicas);
      for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
        List<String> instances = instancePartitions.getInstances(0, replicaId);
        instancesAssigned.add(instances.get(partitionId % instances.size()));
      }
      return instancesAssigned;
    }
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, Configuration config) {
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment completedConsumingOfflineSegmentAssignment =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(currentAssignment);

    // Rebalance COMPLETED segments first
    Map<String, Map<String, String>> completedSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getCompletedSegmentAssignment();
    InstancePartitions completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    Preconditions
        .checkState(completedInstancePartitions != null, "Failed to find COMPLETED instance partitions for table: %s",
            _realtimeTableName);
    LOGGER.info("Rebalancing COMPLETED segments for table: {} with instance partitions: {}", _realtimeTableName,
        completedInstancePartitions);

    Map<String, Map<String, String>> newAssignment;
    if (completedInstancePartitions.getNumReplicas() == 1) {
      // Non-replica-group based assignment

      List<String> instances = SegmentAssignmentUtils
          .getInstancesForNonReplicaGroupBasedAssignment(completedInstancePartitions, _replication);
      newAssignment = SegmentAssignmentUtils
          .rebalanceTableWithHelixAutoRebalanceStrategy(completedSegmentAssignment, instances, _replication);
    } else {
      // Replica-group based assignment

      int numReplicas = completedInstancePartitions.getNumReplicas();
      if (numReplicas != _replication) {
        LOGGER.warn(
            "Number of replicas in instance partitions {}: {} does not match replication in table config: {} for table: {}, use: {}",
            completedInstancePartitions.getName(), numReplicas, _replication, _realtimeTableName, numReplicas);
      }

      Map<Integer, Set<String>> partitionIdToSegmentsMap = new HashMap<>();
      for (String segmentName : completedSegmentAssignment.keySet()) {
        int partitionId = new LLCSegmentName(segmentName).getPartitionId();
        partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new HashSet<>()).add(segmentName);
      }
      newAssignment = SegmentAssignmentUtils
          .rebalanceReplicaGroupBasedTable(completedSegmentAssignment, completedInstancePartitions,
              partitionIdToSegmentsMap);
    }

    // Rebalance CONSUMING segments if configured
    Map<String, Map<String, String>> consumingSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getConsumingSegmentAssignment();
    if (config.getBoolean(RebalanceUserConfigConstants.INCLUDE_CONSUMING,
        RebalanceUserConfigConstants.DEFAULT_INCLUDE_CONSUMING)) {
      InstancePartitions consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
      Preconditions
          .checkState(consumingInstancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
              _realtimeTableName);
      Preconditions.checkState(consumingInstancePartitions.getNumPartitions() == 1,
          "Instance partitions: %s should contain 1 partition", consumingInstancePartitions.getName());
      LOGGER.info("Rebalancing CONSUMING segments for table: {} with instance partitions: {}", _realtimeTableName,
          consumingInstancePartitions);

      for (String segmentName : consumingSegmentAssignment.keySet()) {
        List<String> instancesAssigned = assignSegment(segmentName, consumingInstancePartitions);
        Map<String, String> instanceStateMap = SegmentAssignmentUtils
            .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        newAssignment.put(segmentName, instanceStateMap);
      }
    } else {
      newAssignment.putAll(consumingSegmentAssignment);
    }

    // Keep the OFFLINE segments not moved, and RealtimeSegmentValidationManager will periodically detect the OFFLINE
    // segments and re-assign them
    newAssignment.putAll(completedConsumingOfflineSegmentAssignment.getOfflineSegmentAssignment());

    LOGGER.info("Rebalanced table: {}, number of segments to be moved to each instance: {}", _realtimeTableName,
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }
}
