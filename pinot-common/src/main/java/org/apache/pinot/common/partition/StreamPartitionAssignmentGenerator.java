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
package org.apache.pinot.common.partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.config.RealtimeTagConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;


/**
 * Class to generate stream partitions assignment based on num partitions in ideal state, num tagged instances and num replicas
 */
public class StreamPartitionAssignmentGenerator {

  private HelixManager _helixManager;

  public StreamPartitionAssignmentGenerator(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  /**
   * Gets stream partition assignment of a table by reading the segment assignment in ideal state
   */
  public PartitionAssignment getStreamPartitionAssignmentFromIdealState(TableConfig tableConfig,
      IdealState idealState) {
    String tableNameWithType = tableConfig.getTableName();

    // get latest segment in each partition
    Map<String, LLCSegmentName> partitionIdToLatestSegment = getPartitionToLatestSegments(idealState);

    // extract partition assignment from the latest segments
    PartitionAssignment partitionAssignment = new PartitionAssignment(tableNameWithType);
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
    for (Map.Entry<String, LLCSegmentName> entry : partitionIdToLatestSegment.entrySet()) {
      String segmentName = entry.getValue().getSegmentName();
      Map<String, String> instanceStateMap = mapFields.get(segmentName);
      partitionAssignment.addPartition(entry.getKey(), Lists.newArrayList(instanceStateMap.keySet()));
    }
    return partitionAssignment;
  }

  /**
   * Generates a map of partition id to latest llc segment
   */
  @VisibleForTesting
  public Map<String, LLCSegmentName> getPartitionToLatestSegments(IdealState idealState) {
    Map<String, LLCSegmentName> partitionIdToLatestSegment = new HashMap<>();
    // read all segments
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // get latest segment in each partition
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      String segmentName = entry.getKey();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        String partitionId = String.valueOf(llcSegmentName.getPartitionId());
        LLCSegmentName latestSegment = partitionIdToLatestSegment.get(partitionId);
        if (latestSegment == null || llcSegmentName.getSequenceNumber() > latestSegment.getSequenceNumber()) {
          partitionIdToLatestSegment.put(partitionId, llcSegmentName);
        }
      }
    }
    return partitionIdToLatestSegment;
  }

  public int getNumPartitionsFromIdealState(IdealState idealState) {
    Set<Integer> partitions = new HashSet<>();
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      String segmentName = entry.getKey();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        partitions.add(llcSegmentName.getPartitionId());
      }
    }
    return partitions.size();
  }

  /**
   * Generates stream partition assignment for given table, using tagged hosts and num partitions
   */
  public PartitionAssignment generateStreamPartitionAssignment(TableConfig tableConfig, int numPartitions)
      throws InvalidConfigException {

    List<String> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(String.valueOf(i));
    }

    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    List<String> consumingTaggedInstances = getConsumingTaggedInstances(tableConfig);

    StreamPartitionAssignmentStrategy streamPartitionAssignmentStrategy =
        StreamPartitionAssignmentStrategyFactory.getStreamPartitionAssignmentStrategy(tableConfig);
    return streamPartitionAssignmentStrategy.getStreamPartitionAssignment(_helixManager, tableNameWithType, partitions,
        numReplicas, consumingTaggedInstances);
  }

  @VisibleForTesting
  protected List<String> getConsumingTaggedInstances(TableConfig tableConfig) {
    RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(tableConfig);
    String consumingServerTag = realtimeTagConfig.getConsumingServerTag();
    List<String> consumingTaggedInstances = HelixHelper.getInstancesWithTag(_helixManager, consumingServerTag);
    if (consumingTaggedInstances.isEmpty()) {
      throw new IllegalStateException("No instances found with tag " + consumingServerTag);
    }
    return consumingTaggedInstances;
  }
}
