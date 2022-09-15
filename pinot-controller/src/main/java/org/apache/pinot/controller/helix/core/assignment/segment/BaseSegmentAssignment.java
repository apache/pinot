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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base segment assignment which contains the common assignment strategies.
 * <ul>
 *   <li>
 *     Non-replica-group based assignment (1 replica-group and 1 partition in instance partitions):
 *     <p>Assign the segment to the instance with the least number of segments. In case of a tie, assign the segment to
 *     the instance with the smallest index in the list. Use Helix AutoRebalanceStrategy to rebalance the table.
 *   </li>
 *   <li>
 *     Replica-group based assignment (multiple replica-groups or partitions in instance partitions):
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
public abstract class BaseSegmentAssignment implements SegmentAssignment {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  protected HelixManager _helixManager;
  protected String _tableNameWithType;
  protected int _replication;
  protected String _partitionColumn;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableNameWithType = tableConfig.getTableName();
    _replication = getReplication(tableConfig);
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;

    if (_partitionColumn == null) {
      _logger.info("Initialized with replication: {} without partition column for table: {} ", _replication,
          _tableNameWithType);
    } else {
      _logger.info("Initialized with replication: {} and partition column: {} for table: {}", _replication,
          _partitionColumn, _tableNameWithType);
    }
  }

  /**
   * Returns the replication of the table.
   */
  protected abstract int getReplication(TableConfig tableConfig);

  /**
   * Helper method to check whether the number of replica-groups matches the table replication for replica-group based
   * instance partitions. Log a warning if they do not match and use the one inside the instance partitions. The
   * mismatch can happen when table is not configured correctly (table replication and numReplicaGroups does not match
   * or replication changed without reassigning instances).
   */
  protected void checkReplication(InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    if (numReplicaGroups != _replication) {
      _logger.warn(
          "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for "
              + "table: {}, using: {}", instancePartitions.getInstancePartitionsName(), numReplicaGroups, _replication,
          _tableNameWithType, numReplicaGroups);
    }
  }

  /**
   * Helper method to assign instances based on the current assignment and instance partitions.
   */
  protected List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    int numPartitions = instancePartitions.getNumPartitions();

    if (numReplicaGroups == 1 && numPartitions == 1) {
      // Non-replica-group based assignment

      return SegmentAssignmentUtils.assignSegmentWithoutReplicaGroup(currentAssignment, instancePartitions,
          _replication);
    } else {
      // Replica-group based assignment

      checkReplication(instancePartitions);

      int partitionId;
      if (_partitionColumn == null || numPartitions == 1) {
        partitionId = 0;
      } else {
        // Uniformly spray the segment partitions over the instance partitions
        partitionId = getSegmentPartitionId(segmentName) % numPartitions;
      }

      return SegmentAssignmentUtils.assignSegmentWithReplicaGroup(currentAssignment, instancePartitions, partitionId);
    }
  }

  /**
   * Returns the partition id of the segment.
   */
  protected abstract int getSegmentPartitionId(String segmentName);

  /**
   * Rebalances tiers and returns a pair of tier assignments and non-tier assignment.
   */
  protected Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> rebalanceTiers(
      Map<String, Map<String, String>> currentAssignment, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, boolean bootstrap) {
    if (sortedTiers == null) {
      return Pair.of(null, currentAssignment);
    }

    Preconditions.checkState(tierInstancePartitionsMap != null, "Tier to instancePartitions map is null");
    _logger.info("Rebalancing tiers: {} for table: {} with bootstrap: {}", tierInstancePartitionsMap.keySet(),
        _tableNameWithType, bootstrap);

    // Get tier to segment assignment map i.e. current assignments split by tiers they are eligible for
    SegmentAssignmentUtils.TierSegmentAssignment tierSegmentAssignment =
        new SegmentAssignmentUtils.TierSegmentAssignment(_tableNameWithType, sortedTiers, currentAssignment);
    Map<String, Map<String, Map<String, String>>> tierNameToSegmentAssignmentMap =
        tierSegmentAssignment.getTierNameToSegmentAssignmentMap();

    // For each tier, calculate new assignment using instancePartitions for that tier
    List<Map<String, Map<String, String>>> newTierAssignments = new ArrayList<>(tierNameToSegmentAssignmentMap.size());
    for (Map.Entry<String, Map<String, Map<String, String>>> entry : tierNameToSegmentAssignmentMap.entrySet()) {
      String tierName = entry.getKey();
      Map<String, Map<String, String>> tierCurrentAssignment = entry.getValue();

      InstancePartitions tierInstancePartitions = tierInstancePartitionsMap.get(tierName);
      Preconditions.checkNotNull(tierInstancePartitions, "Failed to find instance partitions for tier: %s of table: %s",
          tierName, _tableNameWithType);

      _logger.info("Rebalancing tier: {} for table: {} with bootstrap: {}, instance partitions: {}", tierName,
          _tableNameWithType, bootstrap, tierInstancePartitions);
      newTierAssignments.add(reassignSegments(tierName, tierCurrentAssignment, tierInstancePartitions, bootstrap));
    }

    return Pair.of(newTierAssignments, tierSegmentAssignment.getNonTierSegmentAssignment());
  }

  /**
   * Rebalances segments in the current assignment using the instancePartitions and returns new assignment
   */
  protected Map<String, Map<String, String>> reassignSegments(String instancePartitionType,
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions, boolean bootstrap) {
    Map<String, Map<String, String>> newAssignment;
    if (bootstrap) {
      _logger.info("Bootstrapping segment assignment for {} segments of table: {}", instancePartitionType,
          _tableNameWithType);

      // When bootstrap is enabled, start with an empty assignment and reassign all segments
      newAssignment = new TreeMap<>();
      for (String segment : currentAssignment.keySet()) {
        List<String> assignedInstances = assignSegment(segment, newAssignment, instancePartitions);
        newAssignment.put(segment,
            SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
      }
    } else {
      int numReplicaGroups = instancePartitions.getNumReplicaGroups();
      int numPartitions = instancePartitions.getNumPartitions();

      if (numReplicaGroups == 1 && numPartitions == 1) {
        // Non-replica-group based assignment

        List<String> instances =
            SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
        newAssignment =
            SegmentAssignmentUtils.rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, instances,
                _replication);
      } else {
        // Replica-group based assignment

        checkReplication(instancePartitions);

        if (_partitionColumn == null || numPartitions == 1) {
          // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
          //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the
          //       table name hash as the random seed for the shuffle so that the result is deterministic.
          List<String> segments = new ArrayList<>(currentAssignment.keySet());
          Collections.shuffle(segments, new Random(_tableNameWithType.hashCode()));

          newAssignment = new TreeMap<>();
          SegmentAssignmentUtils.rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, 0, segments,
              newAssignment);
        } else {
          Map<Integer, List<String>> instancePartitionIdToSegmentsMap =
              getInstancePartitionIdToSegmentsMap(currentAssignment.keySet(), instancePartitions.getNumPartitions());

          // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
          //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the
          //       table name hash as the random seed for the shuffle so that the result is deterministic.
          Random random = new Random(_tableNameWithType.hashCode());
          for (List<String> segments : instancePartitionIdToSegmentsMap.values()) {
            Collections.shuffle(segments, random);
          }

          return SegmentAssignmentUtils.rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions,
              instancePartitionIdToSegmentsMap);
        }
      }
    }
    return newAssignment;
  }

  /**
   * Returns the instance partitions for the given segments.
   */
  protected abstract Map<Integer, List<String>> getInstancePartitionIdToSegmentsMap(Set<String> segments,
      int numInstancePartitions);
}
