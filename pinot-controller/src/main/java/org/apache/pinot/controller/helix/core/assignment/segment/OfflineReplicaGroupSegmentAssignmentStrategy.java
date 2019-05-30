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
import java.util.TreeMap;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitionsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy for offline segments that assigns segment to the instance in the replica with the least
 * number of segments.
 * <p>Among multiple replicas, always mirror the assignment (pick the same index of the instance).
 * <p>Inside each partition, assign the segment to the servers with the least segments already assigned. In case of a
 * tie, assign to the server with the smallest index in the list. Do this for one replica and mirror the assignment to
 * other replicas.
 * <p>To rebalance a table, inside each partition, first calculate the number of segments on each server, loop over all
 * the segments and keep the assignment if number of segments for the server has not been reached and track the not
 * assigned segments, then assign the left-over segments to the servers with the least segments, or the smallest index
 * if there is a tie. Repeat the process for all the partitions in one replica, and mirror the assignment to other
 * replicas. With this greedy algorithm, the result is deterministic and with minimum segment moves.
 */
public class OfflineReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineReplicaGroupSegmentAssignmentStrategy.class);

  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private String _tableNameWithType;
  private String _partitionColumn;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();
    ReplicaGroupStrategyConfig strategyConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = strategyConfig != null ? strategyConfig.getPartitionColumn() : null;

    if (_partitionColumn == null) {
      LOGGER.info("Initialized OfflineReplicaGroupSegmentAssignmentStrategy for table: {} without partition column",
          _tableNameWithType);
    } else {
      LOGGER.info("Initialized OfflineReplicaGroupSegmentAssignmentStrategy for table: {} with partition column: {}",
          _tableNameWithType, _partitionColumn);
    }
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment) {
    InstancePartitions instancePartitions = InstancePartitionsUtils
        .fetchOrComputeInstancePartitions(_helixManager, _tableConfig, InstancePartitionsType.OFFLINE);

    // Fetch partition id from segment ZK metadata if partition column is configured
    int partitionId = 0;
    if (_partitionColumn == null) {
      Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
          "The instance partitions: %s should contain only 1 partition", instancePartitions.getName());
    } else {
      OfflineSegmentZKMetadata segmentZKMetadata = ZKMetadataProvider
          .getOfflineSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _tableNameWithType, segmentName);
      Preconditions
          .checkState(segmentZKMetadata != null, "Failed to fetch segment ZK metadata for table: %s, segment: %s",
              _tableNameWithType, segmentName);
      // Uniformly spray the segment partitions over the instance partitions
      partitionId = getPartitionId(segmentZKMetadata) % instancePartitions.getNumPartitions();
    }

    // First assign the segment to replica 0
    List<String> instances = instancePartitions.getInstances(partitionId, 0);
    int[] numSegmentsAssigned = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, instances);
    int minNumSegmentsAssigned = numSegmentsAssigned[0];
    int instanceIdWithLeastSegmentsAssigned = 0;
    int numInstances = numSegmentsAssigned.length;
    for (int instanceId = 1; instanceId < numInstances; instanceId++) {
      if (numSegmentsAssigned[instanceId] < minNumSegmentsAssigned) {
        minNumSegmentsAssigned = numSegmentsAssigned[instanceId];
        instanceIdWithLeastSegmentsAssigned = instanceId;
      }
    }

    // Mirror the assignment to all replicas
    int numReplicas = instancePartitions.getNumReplicas();
    List<String> instancesAssigned = new ArrayList<>(numReplicas);
    for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
      instancesAssigned
          .add(instancePartitions.getInstances(partitionId, replicaId).get(instanceIdWithLeastSegmentsAssigned));
    }

    if (_partitionColumn == null) {
      LOGGER.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
          _tableNameWithType);
    } else {
      LOGGER.info("Assigned segment: {} with partition id: {} to instances: {} for table: {}", segmentName, partitionId,
          instancesAssigned, _tableNameWithType);
    }
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Configuration config) {
    InstancePartitions instancePartitions = InstancePartitionsUtils
        .fetchOrComputeInstancePartitions(_helixManager, _tableConfig, InstancePartitionsType.OFFLINE);
    if (_partitionColumn == null) {
      return rebalanceTableWithoutPartition(currentAssignment, instancePartitions);
    } else {
      return rebalanceTableWithPartition(currentAssignment, instancePartitions);
    }
  }

  private Map<String, Map<String, String>> rebalanceTableWithoutPartition(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions) {
    Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
        "The instance partitions: %s should contain only 1 partition", instancePartitions.getName());

    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, 0, currentAssignment.keySet(),
            newAssignment);

    LOGGER.info(
        "Rebalanced {} segments with instance partitions: {} for table: {} without partition column, number of segments to be moved to each instance: {}",
        currentAssignment.size(), instancePartitions.getPartitionToInstancesMap(), _tableNameWithType,
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    return newAssignment;
  }

  private Map<String, Map<String, String>> rebalanceTableWithPartition(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions) {
    // Fetch partition id from segment ZK metadata
    List<OfflineSegmentZKMetadata> segmentZKMetadataList = ZKMetadataProvider
        .getOfflineSegmentZKMetadataListForTable(_helixManager.getHelixPropertyStore(), _tableNameWithType);
    Map<String, OfflineSegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (OfflineSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      segmentZKMetadataMap.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata);
    }
    Map<Integer, Set<String>> partitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int partitionId = getPartitionId(segmentZKMetadataMap.get(segmentName));
      partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new HashSet<>()).add(segmentName);
    }

    Map<String, Map<String, String>> newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions, partitionIdToSegmentsMap);

    LOGGER.info(
        "Rebalanced {} segments with instance partitions: {} for table: {} with partition column: {}, number of segments to be moved to each instance: {}",
        currentAssignment.size(), instancePartitions.getPartitionToInstancesMap(), _tableNameWithType, _partitionColumn,
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    return newAssignment;
  }

  private int getPartitionId(OfflineSegmentZKMetadata segmentZKMetadata) {
    String segmentName = segmentZKMetadata.getSegmentName();
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(_partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for table: %s, segment: %s does not contain partition metadata for column: %s",
        _tableNameWithType, segmentName, _partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for table: %s, segment: %s contains multiple partitions for column: %s",
        _tableNameWithType, segmentName, _partitionColumn);
    return partitions.iterator().next();
  }
}
