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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.Pairs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy for offline segments that assigns segment to the instance with the least number of
 * segments. In case of a tie, assigns to the instance with the smallest index in the list. The strategy ensures that
 * replicas of the same segment are not assigned to the same server.
 * <p>To rebalance a table, use Helix AutoRebalanceStrategy.
 */
public class OfflineBalanceNumSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineBalanceNumSegmentAssignmentStrategy.class);

  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private String _tableNameWithType;
  private int _replication;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();

    LOGGER.info("Initialized OfflineBalanceNumSegmentAssignmentStrategy for table: {} with replication: {}",
        _tableNameWithType, _replication);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment) {
    List<String> instances = SegmentAssignmentUtils
        .getInstancesForBalanceNumStrategy(_helixManager, _tableConfig, _replication, InstancePartitionsType.OFFLINE);
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);

    // Assign the segment to the instance with the least segments, or the smallest id if there is a tie
    int numInstances = numSegmentsAssignedPerInstance.length;
    PriorityQueue<Pairs.IntPair> heap = new PriorityQueue<>(numInstances, Pairs.intPairComparator());
    for (int instanceId = 0; instanceId < numInstances; instanceId++) {
      heap.add(new Pairs.IntPair(numSegmentsAssignedPerInstance[instanceId], instanceId));
    }
    List<String> instancesAssigned = new ArrayList<>(_replication);
    for (int i = 0; i < _replication; i++) {
      instancesAssigned.add(instances.get(heap.remove().getRight()));
    }

    LOGGER.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Configuration config) {
    List<String> instances = SegmentAssignmentUtils
        .getInstancesForBalanceNumStrategy(_helixManager, _tableConfig, _replication, InstancePartitionsType.OFFLINE);
    Map<String, Map<String, String>> newAssignment =
        SegmentAssignmentUtils.rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, instances, _replication);

    LOGGER.info(
        "Rebalanced {} segments to instances: {} for table: {} with replication: {}, number of segments to be moved to each instance: {}",
        currentAssignment.size(), instances, _tableNameWithType, _replication,
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }
}
