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
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitionsUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy for LLC real-time segments (both consuming and completed).
 * <p>It is very similar to replica-group based segment assignment with the following differences:
 * <ul>
 *   <li>1. Inside one replica, each partition (stream partition) always contains one server</li>
 *   <li>
 *     2. Partition id for an instance is derived from the index of the instance in the replica group, instead of
 *     explicitly stored in the instance partitions
 *   </li>
 *   <li>3. In addition to the ONLINE segments, there are also CONSUMING segments to be assigned</li>
 * </ul>
 * <p>
 *   Since each partition contains only one server (in one replica), we can directly assign or rebalance segments to the
 *   servers based on the partition id.
 * <p>
 *   The real-time segment assignment does not minimize segment moves because the server is fixed for each partition in
 *   each replica. The instance assignment is responsible for keeping minimum changes to the instance partitions to
 *   reduce the number of segments need to be moved.
 */
public class RealtimeReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeReplicaGroupSegmentAssignmentStrategy.class);

  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private String _tableNameWithType;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();

    LOGGER.info("Initialized RealtimeReplicaGroupSegmentAssignmentStrategy for table: {}", _tableNameWithType);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment) {
    InstancePartitions instancePartitions = InstancePartitionsUtils
        .fetchOrComputeInstancePartitions(_helixManager, _tableConfig, InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
        "The instance partitions: %s should contain only 1 partition", instancePartitions.getName());

    int partitionId = new LLCSegmentName(segmentName).getPartitionId();
    List<String> instancesAssigned = getInstances(instancePartitions, partitionId);
    LOGGER.info("Assigned segment: {} with partition id: {} to instances: {} for table: {}", segmentName, partitionId,
        instancesAssigned, _tableNameWithType);
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Configuration config) {
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment completedConsumingOfflineSegmentAssignment =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(currentAssignment);

    // Rebalance COMPLETED segments first
    Map<String, Map<String, String>> completedSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getCompletedSegmentAssignment();
    InstancePartitions instancePartitionsForCompletedSegments = InstancePartitionsUtils
        .fetchOrComputeInstancePartitions(_helixManager, _tableConfig, InstancePartitionsType.COMPLETED);
    Map<Integer, Set<String>> partitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : completedSegmentAssignment.keySet()) {
      int partitionId = new LLCSegmentName(segmentName).getPartitionId();
      partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new HashSet<>()).add(segmentName);
    }
    Map<String, Map<String, String>> newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(completedSegmentAssignment, instancePartitionsForCompletedSegments,
            partitionIdToSegmentsMap);

    // Rebalance CONSUMING segments if needed
    Map<String, Map<String, String>> consumingSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getConsumingSegmentAssignment();
    if (config.getBoolean(RebalanceUserConfigConstants.INCLUDE_CONSUMING,
        RebalanceUserConfigConstants.DEFAULT_INCLUDE_CONSUMING)) {
      InstancePartitions instancePartitionsForConsumingSegments = InstancePartitionsUtils
          .fetchOrComputeInstancePartitions(_helixManager, _tableConfig, InstancePartitionsType.CONSUMING);
      for (String segmentName : consumingSegmentAssignment.keySet()) {
        int partitionId = new LLCSegmentName(segmentName).getPartitionId();
        List<String> instancesAssigned = getInstances(instancePartitionsForConsumingSegments, partitionId);
        Map<String, String> instanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned,
            CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        newAssignment.put(segmentName, instanceStateMap);
      }
      LOGGER.info(
          "Rebalanced {} COMPLETED segments with instance partitions: {} and {} CONSUMING segments with instance partitions: {} for table: {}, number of segments to be moved to each instances: {}",
          completedSegmentAssignment.size(), instancePartitionsForCompletedSegments.getPartitionToInstancesMap(),
          consumingSegmentAssignment.size(), instancePartitionsForConsumingSegments.getPartitionToInstancesMap(),
          _tableNameWithType,
          SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
    } else {
      LOGGER.info(
          "Rebalanced {} COMPLETED segments with instance partitions: {} for table: {}, number of segments to be moved to each instance: {}",
          completedSegmentAssignment.size(), instancePartitionsForCompletedSegments.getPartitionToInstancesMap(),
          _tableNameWithType,
          SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(completedSegmentAssignment, newAssignment));
      newAssignment.putAll(consumingSegmentAssignment);
    }

    // Keep the OFFLINE segments not moved, and RealtimeSegmentValidationManager will periodically detect the OFFLINE
    // segments and re-assign them
    newAssignment.putAll(completedConsumingOfflineSegmentAssignment.getOfflineSegmentAssignment());

    return newAssignment;
  }

  /**
   * Returns the instances for the given partition id for CONSUMING segments.
   * <p>Within a replica, uniformly spray the partitions across the instances.
   * <p>E.g. (within a replica, 3 servers, 6 partitions)
   * <pre>
   *   "0_0": [i0, i1, i2]
   *           p0  p1  p2
   *           p3  p4  p5
   * </pre>
   */
  private List<String> getInstances(InstancePartitions instancePartitions, int partitionId) {
    int numReplicas = instancePartitions.getNumReplicas();
    List<String> instancesAssigned = new ArrayList<>(numReplicas);
    for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
      List<String> instances = instancePartitions.getInstances(0, replicaId);
      instancesAssigned.add(instances.get(partitionId % instances.size()));
    }
    return instancesAssigned;
  }
}
