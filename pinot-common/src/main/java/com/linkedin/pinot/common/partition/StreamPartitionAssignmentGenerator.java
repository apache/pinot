/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;


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
   * @param idealState
   * @return
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

    // TODO: add an override which can read from znode, instead of generating on the fly

    List<String> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(String.valueOf(i));
    }

    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    List<String> consumingTaggedInstances = getConsumingTaggedInstances(tableConfig);
    if (consumingTaggedInstances.size() < numReplicas) {
      throw new InvalidConfigException(
          "Not enough consuming instances tagged. Must be atleast equal to numReplicas:" + numReplicas);
    }

    /**
     * TODO: We will use only uniform assignment for now
     * This will be refactored as AssignmentStrategy interface and implementations UniformAssignment, BalancedAssignment etc
     * {@link StreamPartitionAssignmentGenerator} and AssignmentStrategy interface will together replace
     * StreamPartitionAssignmentGenerator and StreamPartitionAssignmentStrategy
     */
    return uniformAssignment(tableNameWithType, partitions, numReplicas, consumingTaggedInstances);
  }

  /**
   * Uniformly sprays the partitions and replicas across given list of instances
   * Picks starting point based on table hash value. This ensures that we will always pick the same starting point,
   * and return consistent assignment across calls
   * @param allInstances
   * @param partitions
   * @param numReplicas
   * @return
   */
  private PartitionAssignment uniformAssignment(String tableName, List<String> partitions, int numReplicas,
      List<String> allInstances) {

    PartitionAssignment partitionAssignment = new PartitionAssignment(tableName);

    Collections.sort(allInstances);

    int numInstances = allInstances.size();
    int serverId = Math.abs(EqualityUtils.hashCodeOf(tableName)) % numInstances;
    for (String partition : partitions) {
      List<String> instances = new ArrayList<>(numReplicas);
      for (int r = 0; r < numReplicas; r++) {
        instances.add(allInstances.get(serverId));
        serverId = (serverId + 1) % numInstances;
      }
      partitionAssignment.addPartition(partition, instances);
    }
    return partitionAssignment;
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
