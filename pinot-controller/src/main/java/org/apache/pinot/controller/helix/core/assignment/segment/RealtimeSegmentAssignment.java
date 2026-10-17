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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategyFactory;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/// Segment assignment for LLC real-time table.
///
/// - For the CONSUMING segments, it is very similar to replica-group based segment assignment with the following
///   differences:
///   - 1. Within a replica-group, all segments of the same stream partition are always assigned to the same exactly
///     one instance, and because of that we can directly assign or rebalance the CONSUMING segments to the
///     instances based on the partition id
///   - 2. If no explicit partition is configured in the instance partitions, partition id for an instance is
///     derived from the index of the instance (within the replica-group for replica-group based assignment)
/// - For the COMPLETED segments:
///   - If COMPLETED instance partitions are provided, reassign COMPLETED segments the same way as
///     [OfflineSegmentAssignment] to relocate COMPLETED segments and offload them from CONSUMING instances to
///     COMPLETED instances
///   - If COMPLETED instance partitions are not provided, reassign COMPLETED segments the same way as CONSUMING
///     segments with CONSUMING instance partitions to ensure COMPLETED segments are served by the correct instances
///     when instances for the table has been changed.
/// - For a partition-pinned table (strict replica-group routing), the instances of each partition are recorded in
///   the partition assignment of the ideal state (see
///   [org.apache.pinot.common.assignment.PartitionAssignmentUtils]), and the CONSUMING segments of a partition are
///   assigned to them even when the instance partitions have changed, so that at any time all segments of the
///   partition are hosted by the same instances. Table rebalance moves the partitions and their segments together,
///   and the COMPLETED segments not relocated to COMPLETED instance partitions or to a tier follow the partition as
///   well. Upsert and dedup rely on this to serve all segments of a partition from the same instance, so these tables
///   cannot relocate COMPLETED segments, must include CONSUMING segments in the rebalance, and upsert tables cannot
///   use tiers.
public class RealtimeSegmentAssignment extends BaseSegmentAssignment {

  @Override
  public List<String> assignSegment(String segmentName, IdealState idealState,
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
      Preconditions.checkState(!_tableConfig.isUpsertEnabled() && !_tableConfig.isDedupEnabled(),
          "Failed to find CONSUMING instance partitions for upsert/dedup table: %s", _tableNameWithType);
      // Gets Segment assignment strategy for instance partitions
      SegmentAssignmentStrategy segmentAssignmentStrategy =
          SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(_helixManager, _tableConfig,
              instancePartitionsType.toString(), instancePartitions);
      instancesAssigned = segmentAssignmentStrategy.assignSegment(segmentName, idealState.getRecord().getMapFields(),
          instancePartitions);
    } else if (_partitionPinned) {
      instancesAssigned = assignPinnedConsumingSegment(segmentName, idealState, instancePartitions);
    } else {
      instancesAssigned = assignConsumingSegment(segmentName, instancePartitions);
    }
    _logger.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }

  /// Helper method to assign instances for CONSUMING segment based on the segment partition id and instance partitions.
  private List<String> assignConsumingSegment(String segmentName, InstancePartitions instancePartitions) {
    int segmentPartitionId =
        SegmentUtils.getSegmentPartitionIdOrDefault(segmentName, _tableNameWithType, _helixManager, _partitionColumn);
    return assignConsumingSegment(segmentPartitionId, instancePartitions);
  }

  private List<String> assignConsumingSegment(int segmentPartitionId, InstancePartitions instancePartitions) {
    // For multi-stream tables, Pinot partition IDs are encoded as (streamIndex * 10000 + streamPartitionId).
    // Extract the stream-level partition id before computing the instance index to avoid incorrect slot mapping.
    segmentPartitionId =
        IngestionConfigUtils.getStreamPartitionIdFromPinotPartitionId(_tableConfig, segmentPartitionId);

    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    int numPartitions = instancePartitions.getNumPartitions();

    if (numReplicaGroups == 1 && numPartitions == 1) {
      // Non-replica-group based assignment:
      // Uniformly spray the partitions and replicas across the instances.
      // E.g. (6 instances, 3 partitions, 4 replicas)
      // "0_0": [i0,  i1,  i2,  i3,  i4,  i5  ]
      //         p0r0 p0r1 p0r2 p0r3 p1r0 p1r1
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

  /// Assigns a CONSUMING segment of a partition-pinned table to the instances of its partition, see
  /// [BaseSegmentAssignment#getOrCreatePartitionInstances].
  private List<String> assignPinnedConsumingSegment(String segmentName, IdealState idealState,
      InstancePartitions instancePartitions) {
    Integer partitionId =
        SegmentUtils.getSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, _partitionColumn);
    Preconditions.checkState(partitionId != null, "Failed to find partition id for segment: %s of table: %s",
        segmentName, _tableNameWithType);
    return getOrCreatePartitionInstances(idealState, partitionId,
        assignConsumingSegment(partitionId, instancePartitions),
        () -> getExistingAssignment(partitionId, idealState.getRecord().getMapFields()));
  }

  /// Returns the instances hosting the existing segments of the given partition, or `null` if the partition has no
  /// segment. This is used once per partition, to add the partition to the partition assignment of a table created
  /// before the partition assignment was introduced. OFFLINE segments are skipped because they are not rebalanced, so
  /// their assignment can be stale. The instances are taken from, in order:
  /// - The CONSUMING segment of the partition, which is always hosted by the CONSUMING instances of the partition.
  /// - The latest LLC segment of the partition, unless the table has tiers and the segment has been moved to one
  ///   (the segment ZK metadata is read to check that), in which case the partition is not on the CONSUMING
  ///   instances anymore.
  /// - An uploaded segment of the partition, whose partition id is in its name or in its ZK metadata.
  @Nullable
  private Collection<String> getExistingAssignment(int partitionId,
      Map<String, Map<String, String>> currentAssignment) {
    LLCSegmentName latestLLCSegmentName = null;
    Map<String, String> latestInstanceStateMap = null;
    List<String> uploadedSegments = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      if (SegmentAssignmentUtils.isOfflineSegment(instanceStateMap)) {
        continue;
      }
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName == null) {
        uploadedSegments.add(segmentName);
        continue;
      }
      if (llcSegmentName.getPartitionGroupId() != partitionId) {
        continue;
      }
      if (instanceStateMap.containsValue(SegmentStateModel.CONSUMING)) {
        return instanceStateMap.keySet();
      }
      if (latestLLCSegmentName == null
          || llcSegmentName.getSequenceNumber() > latestLLCSegmentName.getSequenceNumber()) {
        latestLLCSegmentName = llcSegmentName;
        latestInstanceStateMap = instanceStateMap;
      }
    }
    if (latestLLCSegmentName != null) {
      if (CollectionUtils.isNotEmpty(_tableConfig.getTierConfigsList())) {
        String latestSegmentName = latestLLCSegmentName.getSegmentName();
        SegmentZKMetadata segmentZKMetadata =
            ZKMetadataProvider.getSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _tableNameWithType,
                latestSegmentName);
        if (segmentZKMetadata == null) {
          _logger.warn("Failed to find ZK metadata for latest segment: {} of partition: {}, skipping existing "
              + "assignment", latestSegmentName, partitionId);
          return null;
        }
        if (segmentZKMetadata.getTier() != null) {
          _logger.warn("Latest segment: {} of partition: {} is on tier: {}, skipping existing assignment",
              latestSegmentName, partitionId, segmentZKMetadata.getTier());
          return null;
        }
      }
      return latestInstanceStateMap.keySet();
    }
    for (String uploadedSegment : uploadedSegments) {
      Integer uploadedSegmentPartitionId =
          SegmentUtils.getSegmentPartitionId(uploadedSegment, _tableNameWithType, _helixManager, _partitionColumn);
      if (uploadedSegmentPartitionId != null && uploadedSegmentPartitionId == partitionId) {
        return currentAssignment.get(uploadedSegment).keySet();
      }
    }
    return null;
  }

  @Override
  public TableAssignment rebalanceTable(IdealState idealState,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, RebalanceConfig config) {
    InstancePartitions completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    InstancePartitions consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(consumingInstancePartitions != null,
        "Failed to find CONSUMING instance partitions for table: %s", _tableNameWithType);
    boolean includeConsuming = config.isIncludeConsuming();
    boolean bootstrap = config.isBootstrap();
    if (_tableConfig.isUpsertEnabled() || _tableConfig.isDedupEnabled()) {
      Preconditions.checkState(completedInstancePartitions == null,
          "COMPLETED instance partitions must not be provided for upsert/dedup table: %s", _tableNameWithType);
      Preconditions.checkArgument(includeConsuming,
          "Consuming segments must be included when rebalancing upsert/dedup table: %s", _tableNameWithType);
      Preconditions.checkState(sortedTiers == null || !_tableConfig.isUpsertEnabled(),
          "Tiers must not be specified for upsert table: %s", _tableNameWithType);
    }
    _logger.info("Rebalancing table: {} with COMPLETED instance partitions: {}, CONSUMING instance partitions: {}, "
            + "includeConsuming: {}, bootstrap: {}, partitionPinned: {}", _tableNameWithType,
        completedInstancePartitions, consumingInstancePartitions, includeConsuming, bootstrap, _partitionPinned);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();

    // Rebalance tiers first
    Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> pair =
        rebalanceTiers(currentAssignment, sortedTiers, tierInstancePartitionsMap, bootstrap);

    List<Map<String, Map<String, String>>> newTierAssignments = pair.getLeft();
    Map<String, Map<String, String>> nonTierAssignment = pair.getRight();

    Set<String> committingSegments = null;
    if (PauselessConsumptionUtils.isPauselessEnabled(_tableConfig)) {
      List<String> committingSegmentList = PinotLLCRealtimeSegmentManager.getCommittingSegments(_tableNameWithType,
          _helixManager.getHelixPropertyStore());
      if (!committingSegmentList.isEmpty()) {
        committingSegments = new HashSet<>(committingSegmentList);
      }
    }
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment completedConsumingOfflineSegmentAssignment =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(nonTierAssignment, committingSegments);

    // For a partition-pinned table, compute the target instances of every partition in the partition assignment from
    // the CONSUMING instance partitions. Partitions found from the segments are added on the fly.
    Map<Integer, List<String>> partitionAssignment;
    if (_partitionPinned) {
      partitionAssignment = new TreeMap<>();
      for (int partitionId : PartitionAssignmentUtils.getPartitionAssignment(idealState).keySet()) {
        partitionAssignment.put(partitionId,
            PartitionAssignmentUtils.toInstanceList(assignConsumingSegment(partitionId, consumingInstancePartitions)));
      }
    } else {
      partitionAssignment = Map.of();
    }
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
          completedInstancePartitions, bootstrap, segmentAssignmentStrategy);
    } else {
      // When COMPLETED instance partitions are not provided, reassign COMPLETED segments the same way as CONSUMING
      // segments with CONSUMING instance partitions (ensure COMPLETED segments are served by the correct instances when
      // instances for the table has been changed)
      _logger.info(
          "No COMPLETED instance partitions found, reassigning COMPLETED segments the same way as CONSUMING segments "
              + "with CONSUMING instance partitions for table: {}", _tableNameWithType);

      newAssignment = new TreeMap<>();
      for (String segmentName : completedSegmentAssignment.keySet()) {
        List<String> instancesAssigned =
            getTargetInstances(segmentName, consumingInstancePartitions, partitionAssignment);
        Map<String, String> instanceStateMap =
            SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE);
        newAssignment.put(segmentName, instanceStateMap);
      }
    }

    // Reassign CONSUMING segments if configured
    Map<String, Map<String, String>> consumingSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getConsumingSegmentAssignment();
    if (includeConsuming) {
      _logger.info("Reassigning CONSUMING segments with CONSUMING instance partitions for table: {}",
          _tableNameWithType);

      for (Map.Entry<String, Map<String, String>> entry : consumingSegmentAssignment.entrySet()) {
        String segmentName = entry.getKey();
        Map<String, String> originalInstanceStateMap = entry.getValue();
        // The consumingSegmentAssignment can contain two types of segments:
        //   1. Segments in CONSUMING state (for both pauseless and non-pauseless tables)
        //   2. Segments in ONLINE state with segment status in COMMITTING state for pauseless tables
        // The target state should be determined based on the current state in the original instanceStateMap
        String targetState = originalInstanceStateMap.containsValue(SegmentStateModel.ONLINE)
            ? SegmentStateModel.ONLINE : SegmentStateModel.CONSUMING;
        List<String> instancesAssigned =
            getTargetInstances(segmentName, consumingInstancePartitions, partitionAssignment);
        Map<String, String> instanceStateMap =
            SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, targetState);
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

    _logger.info("Rebalanced table: {}, number of segments to be added/removed for each instance: {}",
        _tableNameWithType, SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment));
    return new TableAssignment(newAssignment, partitionAssignment);
  }

  /// Returns the target instances of the given segment with the CONSUMING instance partitions. For a partition-pinned
  /// table, the segment follows its partition, which is added to the given target partition assignment if absent.
  private List<String> getTargetInstances(String segmentName, InstancePartitions consumingInstancePartitions,
      Map<Integer, List<String>> partitionAssignment) {
    if (!_partitionPinned) {
      return assignConsumingSegment(segmentName, consumingInstancePartitions);
    }
    Integer partitionId = getPartitionIdForRebalance(segmentName);
    Preconditions.checkState(partitionId != null, "Failed to find partition id for segment: %s of table: %s",
        segmentName, _tableNameWithType);
    return partitionAssignment.computeIfAbsent(partitionId,
        k -> PartitionAssignmentUtils.toInstanceList(assignConsumingSegment(k, consumingInstancePartitions)));
  }
}
