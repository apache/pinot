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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;


/**
 * This segment assignment policy doesn't allow the table to have multiple tiers. The upsert table has to use this
 * today. Because moving upsert table's segments that are out of TTL needs to move the segments' associated
 * validDocIds bitmaps as well for the upsert data to stay correct on the new tiers. Once moving bitmaps is
 * supported later, the upsert table can use the MultiTierStrictRealtimeSegmentAssignment to use multi tiers too.
 */
public class SingleTierStrictRealtimeSegmentAssignment extends BaseStrictRealtimeSegmentAssignment {
  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, RebalanceConfig config) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
        _tableNameWithType);
    Preconditions.checkArgument(config.isIncludeConsuming(),
        "Consuming segment must be included when rebalancing table: %s using single-tier "
            + "StrictRealtimeSegmentAssignment", _tableNameWithType);
    Preconditions.checkState(sortedTiers == null,
        "Tiers must not be specified for table: %s using single-tier StrictRealtimeSegmentAssignment",
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
    _logger.info("Rebalanced table: {}, number of segments to be added/removed for each instance: {}",
        _tableNameWithType, SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }
}
