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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategyFactory;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;


/**
 * Segment assignment for LLC real-time table.
 * <ul>
 *   <li>
 *     For the CONSUMING segments, it is very similar to replica-group based segment assignment with the following
 *     differences:
 *     <ul>
 *       <li>
 *         1. Within a replica-group, all segments of the same stream partition are always assigned to the same exactly
 *         one instance, and because of that we can directly assign or rebalance the CONSUMING segments to the instances
 *         based on the partition id
 *       </li>
 *       <li>
 *         2. If no explicit partition is configured in the instance partitions, partition id for an instance is derived
 *         from the index of the instance (within the replica-group for replica-group based assignment)
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     For the COMPLETED segments:
 *     <ul>
 *       <li>
 *         If COMPLETED instance partitions are provided, reassign COMPLETED segments the same way as
 *         {@link OfflineSegmentAssignment} to relocate COMPLETED segments and offload them from CONSUMING instances to
 *         COMPLETED instances
 *       </li>
 *       <li>
 *         If COMPLETED instance partitions are not provided, reassign COMPLETED segments the same way as CONSUMING
 *         segments with CONSUMING instance partitions to ensure COMPLETED segments are served by the correct instances
 *         when instances for the table has been changed.
 *       </li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class RealtimeSegmentAssignment extends BaseSegmentAssignment {

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    Map.Entry<InstancePartitionsType, InstancePartitions> typeToInstancePartitions =
        instancePartitionsMap.entrySet().iterator().next();
    InstancePartitionsType instancePartitionsType = typeToInstancePartitions.getKey();
    InstancePartitions instancePartitions = typeToInstancePartitions.getValue();
    _logger.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _tableNameWithType);

    // TODO: remove this check after we also refactor consuming segments assignment strategy
    // See https://github.com/apache/pinot/issues/9047
    List<String> instancesAssigned;
    if (instancePartitionsType == InstancePartitionsType.COMPLETED) {
      // Gets Segment assignment strategy for instance partitions
      SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory
          .getSegmentAssignmentStrategy(_helixManager, _tableConfig, instancePartitionsType.toString(),
              instancePartitions);
      instancesAssigned = segmentAssignmentStrategy
          .assignSegment(segmentName, currentAssignment, instancePartitions, InstancePartitionsType.COMPLETED);
    } else {
      instancesAssigned = assignConsumingSegment(segmentName, instancePartitions);
    }
    _logger.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }

  /**
   * Helper method to assign instances for CONSUMING segment based on the segment partition id and instance partitions.
   */
  private List<String> assignConsumingSegment(String segmentName, InstancePartitions instancePartitions) {
    int segmentPartitionId = SegmentAssignmentUtils
        .getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, _partitionColumn);
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    int numPartitions = instancePartitions.getNumPartitions();

    if (numReplicaGroups == 1 && numPartitions == 1) {
      // Non-replica-group based assignment:
      // Uniformly spray the partitions and replicas across the instances.
      // E.g. (6 instances, 3 partitions, 4 replicas)
      // "0_0": [i0,  i1,  i2,  i3,  i4,  i5  ]
      //         p0r0 p0r1 p0r2 p1r3 p1r0 p1r1
      //         p1r2 p1r3 p2r0 p2r1 p2r2 p2r3

      List<String> instances =
          SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
      int numInstances = instances.size();
      List<String> instancesAssigned = new ArrayList<>(_replication);
      for (int replicaId = 0; replicaId < _replication; replicaId++) {
        int instanceIndex = (segmentPartitionId * _replication + replicaId) % numInstances;
        instancesAssigned.add(instances.get(instanceIndex));
      }
      return instancesAssigned;
    } else {
      // Replica-group based assignment
      // TODO: Refactor check replication this for segment assignment strategy in follow up PR
      // See https://github.com/apache/pinot/issues/9047
      if (numReplicaGroups != _replication) {
        _logger.warn(
            "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for "
                + "table: {}, using: {}", instancePartitions.getInstancePartitionsName(), numReplicaGroups,
            _replication, _tableNameWithType, numReplicaGroups);
      }
      List<String> instancesAssigned = new ArrayList<>(numReplicaGroups);

      if (numPartitions == 1) {
        // Implicit partition:
        // Within a replica-group, uniformly spray the partitions across the instances.
        // E.g. (within a replica-group, 3 instances, 6 partitions)
        // "0_0": [i0, i1, i2]
        //         p0  p1  p2
        //         p3  p4  p5

        for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
          List<String> instances = instancePartitions.getInstances(0, replicaGroupId);
          instancesAssigned.add(instances.get(segmentPartitionId % instances.size()));
        }
      } else {
        // Explicit partition:
        // Assign segment to the first instance within the partition.

        for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
          int partitionId = segmentPartitionId % numPartitions;
          instancesAssigned.add(instancePartitions.getInstances(partitionId, replicaGroupId).get(0));
        }
      }

      return instancesAssigned;
    }
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, Configuration config) {
    InstancePartitions completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    InstancePartitions consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions
        .checkState(consumingInstancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
            _tableNameWithType);
    boolean includeConsuming = config
        .getBoolean(RebalanceConfigConstants.INCLUDE_CONSUMING, RebalanceConfigConstants.DEFAULT_INCLUDE_CONSUMING);
    boolean bootstrap =
        config.getBoolean(RebalanceConfigConstants.BOOTSTRAP, RebalanceConfigConstants.DEFAULT_BOOTSTRAP);

    // Rebalance tiers first
    Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> pair =
        rebalanceTiers(currentAssignment, sortedTiers, tierInstancePartitionsMap, bootstrap,
            InstancePartitionsType.COMPLETED);

    List<Map<String, Map<String, String>>> newTierAssignments = pair.getLeft();
    Map<String, Map<String, String>> nonTierAssignment = pair.getRight();

    _logger.info("Rebalancing table: {} with COMPLETED instance partitions: {}, CONSUMING instance partitions: {}, "
            + "includeConsuming: {}, bootstrap: {}", _tableNameWithType, completedInstancePartitions,
        consumingInstancePartitions, includeConsuming, bootstrap);

    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment completedConsumingOfflineSegmentAssignment =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(nonTierAssignment);
    Map<String, Map<String, String>> newAssignment;

    // Reassign COMPLETED segments first
    Map<String, Map<String, String>> completedSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getCompletedSegmentAssignment();
    if (completedInstancePartitions != null) {
      // When COMPLETED instance partitions are provided, reassign COMPLETED segments in a balanced way (relocate
      // COMPLETED segments to offload them from CONSUMING instances to COMPLETED instances)
      SegmentAssignmentStrategy segmentAssignmentStrategy =
          SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(_helixManager, _tableConfig,
              InstancePartitionsType.COMPLETED.toString(), completedInstancePartitions);
      _logger.info("Reassigning COMPLETED segments with COMPLETED instance partitions for table: {}",
          _tableNameWithType);
      newAssignment = reassignSegments(InstancePartitionsType.COMPLETED.toString(), completedSegmentAssignment,
          completedInstancePartitions, bootstrap, segmentAssignmentStrategy, InstancePartitionsType.COMPLETED);
    } else {
      // When COMPLETED instance partitions are not provided, reassign COMPLETED segments the same way as CONSUMING
      // segments with CONSUMING instance partitions (ensure COMPLETED segments are served by the correct instances when
      // instances for the table has been changed)
      _logger.info(
          "No COMPLETED instance partitions found, reassigning COMPLETED segments the same way as CONSUMING segments "
              + "with CONSUMING instance partitions for table: {}", _tableNameWithType);

      newAssignment = new TreeMap<>();
      for (String segmentName : completedSegmentAssignment.keySet()) {
        List<String> instancesAssigned = assignConsumingSegment(segmentName, consumingInstancePartitions);
        Map<String, String> instanceStateMap =
            SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE);
        newAssignment.put(segmentName, instanceStateMap);
      }
    }

    // Reassign CONSUMING segments if configured
    Map<String, Map<String, String>> consumingSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getConsumingSegmentAssignment();
    if (includeConsuming) {
      _logger
          .info("Reassigning CONSUMING segments with CONSUMING instance partitions for table: {}", _tableNameWithType);

      for (String segmentName : consumingSegmentAssignment.keySet()) {
        List<String> instancesAssigned = assignConsumingSegment(segmentName, consumingInstancePartitions);
        Map<String, String> instanceStateMap =
            SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.CONSUMING);
        newAssignment.put(segmentName, instanceStateMap);
      }
    } else {
      newAssignment.putAll(consumingSegmentAssignment);
    }

    // Keep the OFFLINE segments not moved, and RealtimeSegmentValidationManager will periodically detect the OFFLINE
    // segments and re-assign them
    newAssignment.putAll(completedConsumingOfflineSegmentAssignment.getOfflineSegmentAssignment());

    // Add tier assignments, if available
    if (CollectionUtils.isNotEmpty(newTierAssignments)) {
      newTierAssignments.forEach(newAssignment::putAll);
    }

    _logger.info("Rebalanced table: {}, number of segments to be moved to each instance: {}", _tableNameWithType,
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }
}
