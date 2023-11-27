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
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;


/**
 * Segment assignment for LLC real-time table using upsert. The assignSegment() of RealtimeSegmentAssignment is
 * overridden to add new segment for a table partition in a way that's consistent with the assignment in idealState to
 * make sure that at any time the segments from the same table partition is hosted by the same server.
 * <ul>
 *   <li>
 *     For the CONSUMING segments, in addition to what's done in RealtimeSegmentAssignment, the assignment has to be
 *     checked if it's consistent with the current idealState. If the assignment calculated according to the
 *     InstancePartition and the one in idealState are different, the one in idealState must be used so that segments
 *     from the same table partition are always hosted on the same server as set in current idealState. If the
 *     idealState is not honored, segments from the same table partition may be assigned to different servers,
 *     breaking the key assumption for queries to be correct for the table using upsert.
 *   </li>
 *   <li>
 *     There is no need to handle COMPLETED segments for tables using upsert, because their completed segments should
 *     not be relocated to servers tagged to host COMPLETED segments. Basically, upsert-enabled tables can only use
 *     servers tagged for CONSUMING segments to host both consuming and completed segments from a table partition.
 *   </li>
 * </ul>
 */
public class StrictRealtimeSegmentAssignment extends RealtimeSegmentAssignment {

  // Cache segment partition id to avoid ZK reads
  private final Object2IntOpenHashMap<String> _segmentPartitionIdMap = new Object2IntOpenHashMap<>();

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
        _tableNameWithType);
    _logger.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _tableNameWithType);

    int partitionId = getPartitionId(segmentName);
    List<String> instancesAssigned = assignConsumingSegment(partitionId, instancePartitions);
    Set<String> existingAssignment = getExistingAssignment(partitionId, currentAssignment);
    // Check if the candidate assignment is consistent with existing assignment. Use existing assignment if not.
    if (existingAssignment == null) {
      _logger.info("No existing assignment from idealState, using the one decided by instancePartitions");
    } else if (!isSameAssignment(existingAssignment, instancesAssigned)) {
      _logger.warn("Assignment: {} is inconsistent with idealState: {}, using the one from idealState",
          instancesAssigned, existingAssignment);
      instancesAssigned = new ArrayList<>(existingAssignment);
      if (_controllerMetrics != null) {
        _controllerMetrics.addMeteredTableValue(_tableNameWithType,
            ControllerMeter.CONTROLLER_REALTIME_TABLE_SEGMENT_ASSIGNMENT_MISMATCH, 1L);
      }
    }
    _logger.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }

  /**
   * Returns the existing assignment for the given partition id, or {@code null} if there is no existing segment for the
   * partition. We try to derive the partition id from segment name to avoid ZK reads.
   */
  @Nullable
  private Set<String> getExistingAssignment(int partitionId, Map<String, Map<String, String>> currentAssignment) {
    List<String> uploadedSegments = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      // Skip OFFLINE segments as they are not rebalanced, so their assignment in idealState can be stale.
      if (isOfflineSegment(entry.getValue())) {
        continue;
      }
      LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
      if (llcSegmentName == null) {
        uploadedSegments.add(entry.getKey());
        continue;
      }
      if (llcSegmentName.getPartitionGroupId() == partitionId) {
        return entry.getValue().keySet();
      }
    }
    // Check ZK metadata for uploaded segments to look for a segment that's in the same partition
    for (String uploadedSegment : uploadedSegments) {
      if (getPartitionId(uploadedSegment) == partitionId) {
        return currentAssignment.get(uploadedSegment).keySet();
      }
    }
    return null;
  }

  /**
   * Returns {@code true} if all instances are OFFLINE (neither ONLINE nor CONSUMING), {@code false} otherwise.
   */
  private boolean isOfflineSegment(Map<String, String> instanceStateMap) {
    return !instanceStateMap.containsValue(SegmentStateModel.ONLINE) && !instanceStateMap.containsValue(
        SegmentStateModel.CONSUMING);
  }

  /**
   * Returns the partition id of the given segment.
   */
  private int getPartitionId(String segmentName) {
    Integer partitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, _partitionColumn);
    Preconditions.checkState(partitionId != null, "Failed to find partition id for segment: %s of table: %s",
        segmentName, _tableNameWithType);
    return partitionId;
  }

  /**
   * Returns {@code true} if the ideal assignment and the actual assignment are the same, {@code false} otherwise.
   */
  private boolean isSameAssignment(Set<String> idealAssignment, List<String> instancesAssigned) {
    return idealAssignment.size() == instancesAssigned.size() && idealAssignment.containsAll(instancesAssigned);
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, RebalanceConfig config) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
        _tableNameWithType);
    Preconditions.checkArgument(config.isIncludeConsuming(),
        "Consuming segment must be included when rebalancing upsert table: %s", _tableNameWithType);
    Preconditions.checkState(sortedTiers == null, "Tiers must not be specified for upsert table: %s",
        _tableNameWithType);
    _logger.info("Rebalancing table: {} with instance partitions: {}", _tableNameWithType, instancePartitions);

    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      if (isOfflineSegment(instanceStateMap)) {
        // Keep the OFFLINE segments not moved, and RealtimeSegmentValidationManager will periodically detect the
        // OFFLINE segments and re-assign them
        newAssignment.put(segmentName, instanceStateMap);
      } else {
        // Reassign CONSUMING and COMPLETED segments
        List<String> instancesAssigned =
            assignConsumingSegment(getPartitionIdUsingCache(segmentName), instancePartitions);
        String state = instanceStateMap.containsValue(SegmentStateModel.CONSUMING) ? SegmentStateModel.CONSUMING
            : SegmentStateModel.ONLINE;
        newAssignment.put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, state));
      }
    }
    return newAssignment;
  }

  /**
   * Returns the partition id of the given segment, using cached partition id if exists.
   */
  private int getPartitionIdUsingCache(String segmentName) {
    return _segmentPartitionIdMap.computeIntIfAbsent(segmentName, this::getPartitionId);
  }
}
