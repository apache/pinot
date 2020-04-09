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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfigConstants;
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
    checkReplication(instancePartitions);

    List<String> instancesAssigned = assignSegment(segmentName, currentAssignment, instancePartitions);

    LOGGER
        .info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned, _offlineTableName);
    return instancesAssigned;
  }

  /**
   * Helper method to check whether the number of replica-groups matches the table replication for replica-group based
   * instance partitions. Log a warning if they do not match.
   */
  private void checkReplication(InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    if (numReplicaGroups != 1 && numReplicaGroups != _replication) {
      LOGGER.warn(
          "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for table: {}, use: {}",
          instancePartitions.getInstancePartitionsName(), numReplicaGroups, _replication, _offlineTableName,
          numReplicaGroups);
    }
  }

  /**
   * Helper method to assign instances based on the current assignment and instance partitions.
   */
  private List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    if (numReplicaGroups == 1) {
      // Non-replica-group based assignment

      return SegmentAssignmentUtils
          .assignSegmentWithoutReplicaGroup(currentAssignment, instancePartitions, _replication);
    } else {
      // Replica-group based assignment

      // Fetch partition id from segment ZK metadata if partition column is configured
      int partitionId;
      if (_partitionColumn == null) {
        partitionId = 0;
      } else {
        OfflineSegmentZKMetadata segmentZKMetadata = ZKMetadataProvider
            .getOfflineSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _offlineTableName, segmentName);
        Preconditions
            .checkState(segmentZKMetadata != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
                segmentName, _offlineTableName);
        int segmentPartitionId = getPartitionId(segmentZKMetadata);

        // Uniformly spray the segment partitions over the instance partitions
        int numPartitions = instancePartitions.getNumPartitions();
        partitionId = segmentPartitionId % numPartitions;
      }

      return SegmentAssignmentUtils.assignSegmentWithReplicaGroup(currentAssignment, instancePartitions, partitionId);
    }
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, Configuration config) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    Preconditions.checkState(instancePartitions != null, "Failed to find OFFLINE instance partitions for table: %s",
        _offlineTableName);
    boolean bootstrap =
        config.getBoolean(RebalanceConfigConstants.BOOTSTRAP, RebalanceConfigConstants.DEFAULT_BOOTSTRAP);
    LOGGER.info("Rebalancing table: {} with instance partitions: {}, bootstrap: {}", _offlineTableName,
        instancePartitions, bootstrap);
    checkReplication(instancePartitions);

    Map<String, Map<String, String>> newAssignment;
    if (bootstrap) {
      LOGGER.info("Bootstrapping the table: {}", _offlineTableName);

      // When bootstrap is enabled, start with an empty assignment and reassign all segments
      newAssignment = new TreeMap<>();
      for (String segment : currentAssignment.keySet()) {
        List<String> assignedInstances = assignSegment(segment, newAssignment, instancePartitions);
        newAssignment.put(segment,
            SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentOnlineOfflineStateModel.ONLINE));
      }
    } else {
      int numReplicaGroups = instancePartitions.getNumReplicaGroups();
      if (numReplicaGroups == 1) {
        // Non-replica-group based assignment

        List<String> instances =
            SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
        newAssignment = SegmentAssignmentUtils
            .rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, instances, _replication);
      } else {
        // Replica-group based assignment

        if (_partitionColumn == null) {
          // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
          //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the
          //       table name hash as the random seed for the shuffle so that the result is deterministic.
          List<String> segments = new ArrayList<>(currentAssignment.keySet());
          Collections.shuffle(segments, new Random(_offlineTableName.hashCode()));

          newAssignment = new TreeMap<>();
          SegmentAssignmentUtils
              .rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, 0, segments, newAssignment);
        } else {
          newAssignment = rebalanceTableWithPartition(currentAssignment, instancePartitions);
        }
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
    Map<Integer, List<String>> partitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int partitionId = getPartitionId(segmentZKMetadataMap.get(segmentName));
      partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(segmentName);
    }

    // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
    //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the table
    //       name hash as the random seed for the shuffle so that the result is deterministic.
    Random random = new Random(_offlineTableName.hashCode());
    for (List<String> segments : partitionIdToSegmentsMap.values()) {
      Collections.shuffle(segments, random);
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
