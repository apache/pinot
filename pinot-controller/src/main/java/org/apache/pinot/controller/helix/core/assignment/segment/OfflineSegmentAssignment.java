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
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.AllServersSegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategyFactory;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;


/// Segment assignment for offline table.
///
/// For a partition-pinned table (strict replica-group routing), all segments of a value partition are assigned to
/// the same instances, recorded in the partition assignment of the ideal state (see
/// [org.apache.pinot.common.assignment.PartitionAssignmentUtils]), even when the instance partitions have changed.
/// The partition id of a new segment is read from its ZK metadata. This requires a partition column, a table that is
/// not a dimension table (whose segments are assigned to all servers of the tenant), and OFFLINE instance partitions
/// holding exactly one instance per partition within each replica-group; otherwise the segments are assigned with the
/// segment assignment strategy, as are segments without partition id and segments eligible for a tier, and a table
/// rebalance logs a warning. The entry of a partition of a table created before the partition assignment was
/// introduced is created from the instance partitions, where the strategy would place the segment.
public class OfflineSegmentAssignment extends BaseSegmentAssignment {

  @Override
  protected boolean isPartitionPinned() {
    return super.isPartitionPinned() && _partitionColumn != null && !_tableConfig.isDimTable();
  }

  @Override
  public List<String> assignSegment(String segmentName, IdealState idealState,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    Preconditions.checkState(instancePartitions != null, "Failed to find OFFLINE instance partitions for table: %s",
        _tableNameWithType);
    _logger.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _tableNameWithType);
    List<String> instancesAssigned = null;
    if (isPinnedWith(instancePartitions)) {
      int partitionId = getPartitionId(segmentName);
      if (partitionId != NO_PARTITION_ID) {
        instancesAssigned = getOrCreatePartitionInstances(idealState, partitionId,
            getPartitionInstances(partitionId, instancePartitions));
      } else {
        _logger.warn("Failed to find partition id for segment: {} of partition-pinned table: {}, assigning it with the "
            + "segment assignment strategy", segmentName, _tableNameWithType);
      }
    }
    if (instancesAssigned == null) {
      // Gets Segment assignment strategy for instance partitions
      SegmentAssignmentStrategy segmentAssignmentStrategy =
          SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(_helixManager, _tableConfig,
              InstancePartitionsType.OFFLINE.toString(), instancePartitions);
      instancesAssigned = segmentAssignmentStrategy.assignSegment(segmentName, idealState.getRecord().getMapFields(),
          instancePartitions);
    }
    _logger.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }

  @Override
  public TableAssignment rebalanceTable(IdealState idealState,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, RebalanceConfig config) {
    InstancePartitions offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    Preconditions.checkState(offlineInstancePartitions != null,
        "Failed to find OFFLINE instance partitions for table: %s", _tableNameWithType);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    // Gets Segment assignment strategy for instance partitions
    SegmentAssignmentStrategy segmentAssignmentStrategy =
        SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(_helixManager, _tableConfig,
            InstancePartitionsType.OFFLINE.toString(), offlineInstancePartitions);
    // TODO: Right now as per tier assignment, different instances will be picked up for different tiers which
    // would produce incorrect results for Dim tables. In future, we need some preconditions to check if
    // tierPartitionMap has single tier for Dim tables and remove below check
    // See https://github.com/apache/pinot/issues/9047
    if (segmentAssignmentStrategy instanceof AllServersSegmentAssignmentStrategy) {
      return new TableAssignment(
          segmentAssignmentStrategy.reassignSegments(currentAssignment, offlineInstancePartitions), Map.of());
    }
    boolean bootstrap = config.isBootstrap();
    // Rebalance tiers first
    Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> pair =
        rebalanceTiers(currentAssignment, sortedTiers, tierInstancePartitionsMap, bootstrap);
    List<Map<String, Map<String, String>>> newTierAssignments = pair.getLeft();
    Map<String, Map<String, String>> nonTierAssignment = pair.getRight();
    // Partition ids of the non-tier segments, for the partition-pinned assignment and for the strict replica-group
    // batching of the rebalance, read from the segment ZK metadata
    Object2IntMap<String> segmentPartitionIds = SegmentAssignmentUtils.isStrictReplicaGroupRouting(_tableConfig)
        ? getPartitionIds(nonTierAssignment.keySet())
        : EMPTY_SEGMENT_PARTITION_IDS;

    boolean partitionPinned = isPinnedWith(offlineInstancePartitions);
    warnIfNotPinned();
    if (_partitionPinned && !partitionPinned) {
      _logger.warn("Instance partitions: {} of partition-pinned table: {} do not hold exactly one instance per "
          + "partition within each replica-group, cannot pin the segments to their partition, reassigning them with "
          + "the segment assignment strategy", offlineInstancePartitions, _tableNameWithType);
    }
    _logger.info("Rebalancing table: {} with instance partitions: {}, bootstrap: {}, partitionPinned: {}",
        _tableNameWithType, offlineInstancePartitions, bootstrap, partitionPinned);
    Map<String, Map<String, String>> newAssignment;
    Map<Integer, List<String>> partitionAssignment;
    if (partitionPinned) {
      // Let the segments follow their partition, whose target instances are computed from the instance partitions.
      // The partition assignment holds the partitions found from the segments, which drops the entries of the
      // partitions without segment.
      partitionAssignment = new TreeMap<>();
      newAssignment = new TreeMap<>();
      Map<String, Map<String, String>> segmentsWithoutPartitionId = new TreeMap<>();
      for (Map.Entry<String, Map<String, String>> entry : nonTierAssignment.entrySet()) {
        String segmentName = entry.getKey();
        int partitionId = segmentPartitionIds.getInt(segmentName);
        if (partitionId != NO_PARTITION_ID) {
          List<String> instancesAssigned = partitionAssignment.computeIfAbsent(partitionId,
              k -> getPartitionInstances(k, offlineInstancePartitions));
          newAssignment.put(segmentName,
              SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
        } else {
          segmentsWithoutPartitionId.put(segmentName, entry.getValue());
        }
      }
      if (!segmentsWithoutPartitionId.isEmpty()) {
        _logger.warn("Failed to find partition id for segments: {} of partition-pinned table: {}, reassigning them "
            + "with the segment assignment strategy", segmentsWithoutPartitionId.keySet(), _tableNameWithType);
        newAssignment.putAll(reassignSegments(InstancePartitionsType.OFFLINE.toString(), segmentsWithoutPartitionId,
            offlineInstancePartitions, bootstrap, segmentAssignmentStrategy));
      }
    } else {
      partitionAssignment = Map.of();
      newAssignment =
          reassignSegments(InstancePartitionsType.OFFLINE.toString(), nonTierAssignment, offlineInstancePartitions,
              bootstrap, segmentAssignmentStrategy);
    }

    // Add tier assignments, if available
    if (CollectionUtils.isNotEmpty(newTierAssignments)) {
      newTierAssignments.forEach(newAssignment::putAll);
    }

    _logger.info("Rebalanced table: {}, number of segments to be added/removed for each instance: {}",
        _tableNameWithType, SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment));
    return new TableAssignment(newAssignment, partitionAssignment, segmentPartitionIds);
  }

  /// Returns whether the segments are pinned to their partition with the given instance partitions: the table is
  /// partition-pinned, and the instance partitions are the OFFLINE ones (not the ones of a tier, whose segments are
  /// not pinned) with exactly one instance per partition within each replica-group, so that all segments of a
  /// partition land on the same instances.
  private boolean isPinnedWith(InstancePartitions instancePartitions) {
    if (!_partitionPinned) {
      return false;
    }
    if (!instancePartitions.getInstancePartitionsName().equals(
        InstancePartitionsUtils.getInstancePartitionsName(_tableNameWithType, InstancePartitionsType.OFFLINE))) {
      return false;
    }
    for (List<String> instances : instancePartitions.getPartitionToInstancesMap().values()) {
      if (instances.size() != 1) {
        return false;
      }
    }
    return true;
  }

  /// Returns the instances of the given partition with the given instance partitions holding one instance per
  /// partition within each replica-group: the instance of the instance partition the segment partition maps to in
  /// each replica-group.
  private static List<String> getPartitionInstances(int partitionId, InstancePartitions instancePartitions) {
    return PartitionAssignmentUtils.toInstanceList(SegmentAssignmentUtils.getOneInstanceFromEachReplicaGroup(
        instancePartitions, partitionId % instancePartitions.getNumPartitions(), 0));
  }
}
