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
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * This segment assignment method allows the table to use multiple tiers when rebalancing table. This can be used for
 * dedup table for now, as its segments out of TTL can be moved to new tiers w/o messing up the dedup metadata
 * tracked on the CONSUMING servers.
 */
public class MultiTierStrictRealtimeSegmentAssignment extends BaseStrictRealtimeSegmentAssignment {
  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, RebalanceConfig config) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
        _tableNameWithType);
    Preconditions.checkArgument(config.isIncludeConsuming(),
        "Consuming segment must be included when rebalancing table: %s using MultiTierStrictRealtimeSegmentAssignment",
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
        String state = instanceStateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)
            ? CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING
            : CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
        newAssignment.put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, state));
      }
    }
    return newAssignment;
  }
}
