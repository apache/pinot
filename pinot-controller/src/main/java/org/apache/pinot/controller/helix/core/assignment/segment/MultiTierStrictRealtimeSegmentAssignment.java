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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * This segment assignment policy allows the table to use multiple tiers when rebalancing table. This can be used for
 * dedup table, whose segments out of TTL can be moved to new tiers without messing up the dedup metadata tracked on
 * the CONSUMING servers.
 */
public class MultiTierStrictRealtimeSegmentAssignment extends BaseStrictRealtimeSegmentAssignment {

  @Override
  protected Set<String> getTierInstances() {
    List<TierConfig> tierConfigs = _tableConfig.getTierConfigsList();
    if (tierConfigs == null || tierConfigs.isEmpty()) {
      return Set.of();
    }
    // Fetch tier instance partitions from ZK to positively identify tier servers. Behavior breakdown:
    //   1. Purpose: build the set of all servers belonging to non-consuming tiers, so getExistingAssignment
    //      can skip segments that live entirely on those servers and avoid placing new CONSUMING segments
    //      on a non-consuming tier.
    //   2. Path missing in ZK: the fetch returns null and that tier contributes nothing to the filter. This
    //      is correct because no segment can be on a tier whose instance partitions have not been created
    //      yet (for example, a tier configured but not yet rebalanced).
    //   3. ZK unreachable and value not in local cache: the fetch throws and the exception propagates up
    //      through assignSegment. The exception is caught by the retry policy in IdealStateGroupCommit and
    //      the call is retried with exponential backoff. If the outage outlives the inline retry,
    //      RealtimeSegmentValidationManager's periodic repair takes over once ZK is reachable again.
    //   4. We never silently swallow a ZK failure and fall back to an empty tier set, because that would
    //      re-introduce the cold-tier assignment bug during ZK flakiness.
    // Reads are typically served from the local HelixPropertyStore cache, so the cost per call is small.
    Set<String> tierInstances = new HashSet<>();
    for (TierConfig tierConfig : tierConfigs) {
      String instancePartitionsName =
          InstancePartitionsUtils.getInstancePartitionsNameForTier(_tableNameWithType, tierConfig.getName());
      InstancePartitions tierInstancePartitions = InstancePartitionsUtils.fetchInstancePartitions(
          _helixManager.getHelixPropertyStore(), instancePartitionsName);
      if (tierInstancePartitions != null) {
        for (List<String> instances : tierInstancePartitions.getPartitionToInstancesMap().values()) {
          tierInstances.addAll(instances);
        }
      }
    }
    return tierInstances;
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
        "Consuming segment must be included when rebalancing table: %s using multi-tier "
            + "StrictRealtimeSegmentAssignment", _tableNameWithType);
    boolean bootstrap = config.isBootstrap();
    _logger.info("Rebalancing table: {} with instance partitions: {}, bootstrap: {}", _tableNameWithType,
        instancePartitions, bootstrap);
    // Rebalance tiers first. Only completed segments are moved to other tiers.
    Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> pair =
        rebalanceTiers(currentAssignment, sortedTiers, tierInstancePartitionsMap, bootstrap);
    List<Map<String, Map<String, String>>> newTierAssignments = pair.getLeft();
    Map<String, Map<String, String>> nonTierAssignment = pair.getRight();
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : nonTierAssignment.entrySet()) {
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
    // Add tier assignments, if available
    if (CollectionUtils.isNotEmpty(newTierAssignments)) {
      newTierAssignments.forEach(newAssignment::putAll);
    }
    _logger.info("Rebalanced table: {}, number of segments to be added/removed for each instance: {}",
        _tableNameWithType, SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }
}
