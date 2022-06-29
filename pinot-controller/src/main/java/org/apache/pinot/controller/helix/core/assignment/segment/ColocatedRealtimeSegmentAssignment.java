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
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ColocatedRealtimeSegmentAssignment implements SegmentAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColocatedRealtimeSegmentAssignment.class);

  private HelixManager _helixManager;
  private String _realtimeTableName;
  private int _replication;
  private String _partitionColumn;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _realtimeTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;

    LOGGER.info("Initialized ColocatedRealtimeSegmentAssignment with replication: {}, partitionColumn: {} for table: "
            + "{}",
        _replication, _partitionColumn, _realtimeTableName);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions.checkState(instancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
        _realtimeTableName);
    Preconditions
        .checkState(instancePartitions.getNumPartitions() == 1, "Instance partitions: %s should contain 1 partition",
            instancePartitions.getInstancePartitionsName());
    LOGGER.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _realtimeTableName);
    checkReplication(instancePartitions);

    int partitionGroupId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _realtimeTableName, _helixManager, _partitionColumn);
    List<String> instances = SegmentAssignmentUtils.assignSegmentForColocatedTable(instancePartitions,
        partitionGroupId);
    return instances;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, Configuration config) {
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment completedConsumingOfflineSegmentAssignment =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(currentAssignment);
    Map<String, Map<String, String>> completedAssignment = assignForOneType(
        completedConsumingOfflineSegmentAssignment.getCompletedSegmentAssignment(), instancePartitionsMap,
        CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
    Map<String, Map<String, String>> consumingAssignment = assignForOneType(
        completedConsumingOfflineSegmentAssignment.getConsumingSegmentAssignment(), instancePartitionsMap,
        CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING);
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : completedAssignment.entrySet()) {
      newAssignment.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Map<String, String>> entry : consumingAssignment.entrySet()) {
      newAssignment.put(entry.getKey(), entry.getValue());
    }
    return newAssignment;
  }

  private Map<String, Map<String, String>> assignForOneType(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, String segmentStateModel) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Map<Integer, List<String>> partitionIdToSegmentsMap = new TreeMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int segmentPartitionId = SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _realtimeTableName,
          _helixManager, _partitionColumn);
      partitionIdToSegmentsMap.computeIfAbsent(segmentPartitionId, (x) -> new ArrayList<>()).add(segmentName);
    }
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<Integer, List<String>> entry : partitionIdToSegmentsMap.entrySet()) {
      int segmentPartitionId = entry.getKey();
      for (String segmentName : entry.getValue()) {
        List<String> instancesAssigned = SegmentAssignmentUtils.assignSegmentForColocatedTable(
            instancePartitions, segmentPartitionId);
        Map<String, String> instanceStateMap = new TreeMap<>();
        instancesAssigned.forEach(instance -> instanceStateMap.put(
            instance, segmentStateModel));
        newAssignment.put(segmentName, instanceStateMap);
      }
    }
    return newAssignment;
  }

  /**
   * Helper method to check whether the number of replica-groups matches the table replication for replica-group based
   * instance partitions. Log a warning if they do not match and use the one inside the instance partitions. The
   * mismatch can happen when table is not configured correctly (table replication and numReplicaGroups does not match
   * or replication changed without reassigning instances).
   */
  private void checkReplication(InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    if (numReplicaGroups != 1 && numReplicaGroups != _replication) {
      LOGGER.warn(
          "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for "
              + "table: {}, use: {}", instancePartitions.getInstancePartitionsName(), numReplicaGroups, _replication,
          _realtimeTableName, numReplicaGroups);
    }
  }
}
