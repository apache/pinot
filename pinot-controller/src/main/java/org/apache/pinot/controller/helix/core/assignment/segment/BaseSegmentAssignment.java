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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategyFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
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
  protected TableConfig _tableConfig;
  protected ControllerMetrics _controllerMetrics;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig, @Nullable ControllerMetrics controllerMetrics) {
    _helixManager = helixManager;
    _tableNameWithType = tableConfig.getTableName();
    _tableConfig = tableConfig;
    _replication = tableConfig.getReplication();
    _partitionColumn = TableConfigUtils.getPartitionColumn(_tableConfig);
    _controllerMetrics = controllerMetrics;
    if (_partitionColumn == null) {
      _logger.info("Initialized with replication: {} without partition column for table: {} ", _replication,
          _tableNameWithType);
    } else {
      _logger.info("Initialized with replication: {} and partition column: {} for table: {}", _replication,
          _partitionColumn, _tableNameWithType);
    }
  }

  /**
   * Rebalances tiers and returns a pair of tier assignments and non-tier assignment.
   */
  protected Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> rebalanceTiers(
      Map<String, Map<String, String>> currentAssignment, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, boolean bootstrap,
      InstancePartitionsType instancePartitionsType) {
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

      // Initialize segment assignment strategy based on the tier instance partitions
      SegmentAssignmentStrategy segmentAssignmentStrategy =
          SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(_helixManager, _tableConfig, tierName,
              tierInstancePartitions);

      _logger.info("Rebalancing tier: {} for table: {} with bootstrap: {}, instance partitions: {}", tierName,
          _tableNameWithType, bootstrap, tierInstancePartitions);
      newTierAssignments.add(reassignSegments(tierName, tierCurrentAssignment, tierInstancePartitions, bootstrap,
          segmentAssignmentStrategy, instancePartitionsType));
    }

    return Pair.of(newTierAssignments, tierSegmentAssignment.getNonTierSegmentAssignment());
  }

  /**
   * Rebalances segments in the current assignment using the instancePartitions and returns new assignment
   */
  protected Map<String, Map<String, String>> reassignSegments(String instancePartitionType,
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions, boolean bootstrap,
      SegmentAssignmentStrategy segmentAssignmentStrategy, InstancePartitionsType instancePartitionsType) {
    Map<String, Map<String, String>> newAssignment;
    if (bootstrap) {
      _logger.info("Bootstrapping segment assignment for {} segments of table: {}", instancePartitionType,
          _tableNameWithType);

      // When bootstrap is enabled, start with an empty assignment and reassign all segments
      newAssignment = new TreeMap<>();
      for (String segment : currentAssignment.keySet()) {
        List<String> assignedInstances =
            segmentAssignmentStrategy.assignSegment(segment, newAssignment, instancePartitions, instancePartitionsType);
        newAssignment
            .put(segment, SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
      }
    } else {
      // Use segment assignment strategy
      newAssignment =
          segmentAssignmentStrategy.reassignSegments(currentAssignment, instancePartitions, instancePartitionsType);
    }
    return newAssignment;
  }
}
