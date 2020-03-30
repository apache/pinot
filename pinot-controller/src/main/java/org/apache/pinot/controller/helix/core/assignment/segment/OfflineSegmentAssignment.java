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
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.spi.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.TableConfig;
import org.apache.pinot.spi.config.assignment.InstancePartitionsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment for offline table.
 * <ul>
 *   <li>
 *     Non-replica-group based assignment (only 1 replica-group in instance partitions):
 *     <p>Assign the segment to the instance with the least number of segments. In case of a tie, assign the segment to
 *     the instance with the smallest index in the list. Use Helix AutoRebalanceStrategy to rebalance the table.
 *   </li>
 *   <li>
 *     Replica-group based assignment (more than 1 replica-groups in instance partitions):
 *     <p>Among replica-groups, always mirror the assignment (pick the same index of the instance).
 *     <p>Within each partition, assign the segment to the instances with the least segments already assigned. In case
 *     of a tie, assign to the instance with the smallest index in the list. Do this for one replica-group and mirror
 *     the assignment to other replica-groups.
 *     <p>To rebalance a table, within each partition, first calculate the number of segments on each instance, loop
 *     over all the segments and keep the assignment if number of segments for the instance has not been reached and
 *     track the not assigned segments, then assign the left-over segments to the instances with the least segments, or
 *     the smallest index if there is a tie. Repeat the process for all the partitions in one replica-group, and mirror
 *     the assignment to other replica-groups. With this greedy algorithm, the result is deterministic and with minimum
 *     segment moves.
 *   </li>
 * </ul>
 */
public class OfflineSegmentAssignment implements SegmentAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineSegmentAssignment.class);

  private HelixManager _helixManager;
  private String _offlineTableName;
  private int _replication;
  private String _partitionColumn;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _offlineTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;

    if (_partitionColumn == null) {
      LOGGER.info("Initialized OfflineSegmentAssignment with replication: {} without partition column for table: {} ",
          _replication, _offlineTableName);
    } else {
      LOGGER.info("Initialized OfflineSegmentAssignment with replication: {} and partition column: {} for table: {}",
          _replication, _partitionColumn, _offlineTableName);
    }
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    Preconditions.checkState(instancePartitions != null, "Failed to find OFFLINE instance partitions for table: %s",
        _offlineTableName);
    LOGGER.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _offlineTableName);

    List<String> instancesAssigned;
    if (instancePartitions.getNumReplicaGroups() == 1) {
      // Non-replica-group based assignment

      // Assign the segment to the instance with the least segments, or the smallest id if there is a tie
      List<String> instances =
          SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
      int[] numSegmentsAssignedPerInstance =
          SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);
      int numInstances = numSegmentsAssignedPerInstance.length;
      PriorityQueue<Pairs.IntPair> heap = new PriorityQueue<>(numInstances, Pairs.intPairComparator());
      for (int instanceId = 0; instanceId < numInstances; instanceId++) {
        heap.add(new Pairs.IntPair(numSegmentsAssignedPerInstance[instanceId], instanceId));
      }
      instancesAssigned = new ArrayList<>(_replication);
      for (int i = 0; i < _replication; i++) {
        instancesAssigned.add(instances.get(heap.remove().getRight()));
      }
    } else {
      // Replica-group based assignment

      int numReplicaGroups = instancePartitions.getNumReplicaGroups();
      if (numReplicaGroups != _replication) {
        LOGGER.warn(
            "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for table: {}, use: {}",
            instancePartitions.getInstancePartitionsName(), numReplicaGroups, _replication, _offlineTableName,
            numReplicaGroups);
      }

      // Fetch partition id from segment ZK metadata if partition column is configured
      int partitionId;
      if (_partitionColumn == null) {
        LOGGER.info("Assigning segment: {} without partition column for table: {}", segmentName, _offlineTableName);

        Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
            "Instance partitions: %s should contain 1 partition without partition column",
            instancePartitions.getInstancePartitionsName());
        partitionId = 0;
      } else {
        LOGGER.info("Assigning segment: {} with partition column: {} for table: {}", segmentName, _partitionColumn,
            _offlineTableName);

        OfflineSegmentZKMetadata segmentZKMetadata = ZKMetadataProvider
            .getOfflineSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _offlineTableName, segmentName);
        Preconditions
            .checkState(segmentZKMetadata != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
                segmentName, _offlineTableName);
        int segmentPartitionId = getPartitionId(segmentZKMetadata);

        // Uniformly spray the segment partitions over the instance partitions
        int numPartitions = instancePartitions.getNumPartitions();
        partitionId = segmentPartitionId % numPartitions;
        LOGGER.info("Assigning segment: {} with partition id: {} to partition: {}/{} for table: {}", segmentName,
            segmentPartitionId, partitionId, numPartitions, _offlineTableName);
      }

      // First assign the segment to replica-group 0
      List<String> instances = instancePartitions.getInstances(partitionId, 0);
      int[] numSegmentsAssignedPerInstance =
          SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);
      int minNumSegmentsAssigned = numSegmentsAssignedPerInstance[0];
      int instanceIdWithLeastSegmentsAssigned = 0;
      int numInstances = numSegmentsAssignedPerInstance.length;
      for (int instanceId = 1; instanceId < numInstances; instanceId++) {
        if (numSegmentsAssignedPerInstance[instanceId] < minNumSegmentsAssigned) {
          minNumSegmentsAssigned = numSegmentsAssignedPerInstance[instanceId];
          instanceIdWithLeastSegmentsAssigned = instanceId;
        }
      }

      // Mirror the assignment to all replica-groups
      instancesAssigned = new ArrayList<>(numReplicaGroups);
      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
        instancesAssigned
            .add(instancePartitions.getInstances(partitionId, replicaGroupId).get(instanceIdWithLeastSegmentsAssigned));
      }
    }

    LOGGER
        .info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned, _offlineTableName);
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, Configuration config) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    Preconditions.checkState(instancePartitions != null, "Failed to find OFFLINE instance partitions for table: %s",
        _offlineTableName);
    LOGGER.info("Rebalancing table: {} with instance partitions: {}", _offlineTableName, instancePartitions);

    Map<String, Map<String, String>> newAssignment;
    if (instancePartitions.getNumReplicaGroups() == 1) {
      // Non-replica-group based assignment

      List<String> instances =
          SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
      newAssignment = SegmentAssignmentUtils
          .rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, instances, _replication);
    } else {
      // Replica-group based assignment

      int numReplicaGroups = instancePartitions.getNumReplicaGroups();
      if (numReplicaGroups != _replication) {
        LOGGER.warn(
            "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for table: {}, use: {}",
            instancePartitions.getInstancePartitionsName(), numReplicaGroups, _replication, _offlineTableName,
            numReplicaGroups);
      }

      if (_partitionColumn == null) {
        LOGGER.info("Rebalancing table: {} without partition column", _offlineTableName);
        Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
            "Instance partitions: %s should contain 1 partition without partition column",
            instancePartitions.getInstancePartitionsName());
        newAssignment = new TreeMap<>();
        SegmentAssignmentUtils
            .rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, 0, currentAssignment.keySet(),
                newAssignment);
      } else {
        LOGGER.info("Rebalancing table: {} with partition column: {}", _offlineTableName, _partitionColumn);
        newAssignment = rebalanceTableWithPartition(currentAssignment, instancePartitions);
      }
    }

    LOGGER.info("Rebalanced table: {}, number of segments to be moved to each instance: {}", _offlineTableName,
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }

  private Map<String, Map<String, String>> rebalanceTableWithPartition(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions) {
    // Fetch partition id from segment ZK metadata
    List<OfflineSegmentZKMetadata> segmentZKMetadataList = ZKMetadataProvider
        .getOfflineSegmentZKMetadataListForTable(_helixManager.getHelixPropertyStore(), _offlineTableName);
    Map<String, OfflineSegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (OfflineSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      segmentZKMetadataMap.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata);
    }
    Map<Integer, Set<String>> partitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int partitionId = getPartitionId(segmentZKMetadataMap.get(segmentName));
      partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new HashSet<>()).add(segmentName);
    }

    return SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions, partitionIdToSegmentsMap);
  }

  private int getPartitionId(OfflineSegmentZKMetadata segmentZKMetadata) {
    String segmentName = segmentZKMetadata.getSegmentName();
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(_partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata for column: %s",
        segmentName, _offlineTableName, _partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for segment: %s of table: %s contains multiple partitions for column: %s", segmentName,
        _offlineTableName, _partitionColumn);
    return partitions.iterator().next();
  }
}
