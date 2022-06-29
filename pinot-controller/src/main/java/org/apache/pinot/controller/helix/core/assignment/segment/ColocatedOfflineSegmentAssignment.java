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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ColocatedOfflineSegmentAssignment implements SegmentAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColocatedOfflineSegmentAssignment.class);

  private HelixManager _helixManager;
  private String _offlineTableName;
  private int _replication;
  private String _partitionColumn;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _offlineTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;

    if (_partitionColumn == null) {
      LOGGER.error("Co-located segment assignment works only when a segment partition column is specified");
    } else {
      LOGGER.info("Initialized OfflineSegmentAssignment with replication: {} and partition column: {} for table: {}",
          _replication, _partitionColumn, _offlineTableName);
    }
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    SegmentZKMetadata segmentZKMetadata = ZKMetadataProvider
        .getSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _offlineTableName, segmentName);
    Preconditions
        .checkState(segmentZKMetadata != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
            segmentName, _offlineTableName);
    int segmentPartitionId = getPartitionId(segmentZKMetadata);
    return SegmentAssignmentUtils.assignSegmentForColocatedTable(
        instancePartitionsMap.get(InstancePartitionsType.OFFLINE), segmentPartitionId);
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, Configuration config) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    // Fetch partition id from segment ZK metadata
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_helixManager.getHelixPropertyStore(), _offlineTableName);
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      segmentZKMetadataMap.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata);
    }
    Map<Integer, List<String>> partitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int partitionId = getPartitionId(segmentZKMetadataMap.get(segmentName));
      partitionIdToSegmentsMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(segmentName);
    }

    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<Integer, List<String>> entry : partitionIdToSegmentsMap.entrySet()) {
      int segmentPartitionId = entry.getKey();
      for (String segmentName : entry.getValue()) {
        List<String> instancesAssigned = SegmentAssignmentUtils.assignSegmentForColocatedTable(
            instancePartitions, segmentPartitionId);
        Map<String, String> instanceStateMap = new TreeMap<>();
        instancesAssigned.forEach(instance -> instanceStateMap.put(instance,
            CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
        newAssignment.put(segmentName, instanceStateMap);
      }
    }
    return newAssignment;
  }

  private int getPartitionId(SegmentZKMetadata segmentZKMetadata) {
    String segmentName = segmentZKMetadata.getSegmentName();
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(_partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata for column: %s",
        segmentName, _offlineTableName, _partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for segment: %s of table: %s contains multiple partitions for column: %s", segmentName,
        _offlineTableName, _partitionColumn);
    return partitions.iterator().next();
  }
}
