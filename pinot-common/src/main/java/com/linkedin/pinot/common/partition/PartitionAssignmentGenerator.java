/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.LLCSegmentName;
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
 * Class to generate partitions assignment based on num partitions in ideal state, num tagged instances and num replicas
 */
public class PartitionAssignmentGenerator {

  private static final int MAX_NUM_SERVERS = 5;

  private HelixManager _helixManager;

  public PartitionAssignmentGenerator(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  /**
   * Gets partition assignment of a table by reading the segment assignment in ideal state
   */
  private PartitionAssignment getPartitionAssignmentFromIdealState(TableConfig tableConfig, IdealState idealState) {
    String tableNameWithType = tableConfig.getTableName();

    // read all segments
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // get latest segment in each partition
    Map<String, LLCSegmentName> partitionIdToLatestSegment = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(entry.getKey());
      String partitionId = String.valueOf(llcSegmentName.getPartitionId());
      LLCSegmentName latestSegment = partitionIdToLatestSegment.get(partitionId);
      if (latestSegment == null || llcSegmentName.getSequenceNumber() > latestSegment.getSequenceNumber()) {
        partitionIdToLatestSegment.put(partitionId, llcSegmentName);
      }
    }

    // extract partition assignment from the latest segments
    PartitionAssignment partitionAssignment = new PartitionAssignment(tableNameWithType);
    for (Map.Entry<String, LLCSegmentName> entry : partitionIdToLatestSegment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = mapFields.get(segmentName);
      partitionAssignment.addPartition(segmentName, Lists.newArrayList(instanceStateMap.keySet()));
    }
    return partitionAssignment;
  }

  /**
   * Generates partition assignment for given table, using the tagged hosts
   */
  public PartitionAssignment generatePartitionAssignment(TableConfig tableConfig, IdealState idealState) {
    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    // read all segments
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // get all partitions
    Set<String> partitionsSet = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(entry.getKey());
      String partitionId = String.valueOf(llcSegmentName.getPartitionId());
      partitionsSet.add(partitionId);
    }
    List<String> partitions = Lists.newArrayList(partitionsSet);

    // get consuming server tagged hosts from helix admin
    RealtimeTagConfig tagConfig = new RealtimeTagConfig(tableConfig, _helixManager);
    String consumingServerTag = tagConfig.getConsumingServerTag();
    List<String> consumingTaggedInstances =
        _helixManager.getClusterManagmentTool().getInstancesInClusterWithTag(_helixManager.getClusterName(), consumingServerTag);
    if (consumingTaggedInstances.isEmpty()) {
      throw new IllegalStateException("No instances found with tag " + consumingServerTag);
    }

    return uniformAssignment(tableNameWithType, consumingTaggedInstances, partitions, numReplicas);
  }


  /**
   * Generates partition assignment for given table, using tagged hosts and num partitions
   */
  public PartitionAssignment getPartitionAssignment(TableConfig tableConfig, IdealState idealState, int numPartitions) {
    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    List<String> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(String.valueOf(i));
    }

    // get consuming server tagged hosts from helix admin
    RealtimeTagConfig tagConfig = new RealtimeTagConfig(tableConfig, _helixManager);
    String consumingServerTag = tagConfig.getConsumingServerTag();
    List<String> consumingTaggedInstances =
        _helixManager.getClusterManagmentTool().getInstancesInClusterWithTag(_helixManager.getClusterName(), consumingServerTag);
    if (consumingTaggedInstances.isEmpty()) {
      throw new IllegalStateException("No instances found with tag " + consumingServerTag);
    }

    return uniformAssignment(tableNameWithType, consumingTaggedInstances, partitions, numReplicas);
  }

  /**
   * Uniformly sprays the partitions and replicas across given list of instances
   * Picks starting point based on table hash value. This ensures that we will always pick the same starting point,
   * and resturn consistent assignment across calls
   * @param allInstances
   * @param partitions
   * @param numReplicas
   * @return
   */
  private PartitionAssignment uniformAssignment(String tableName, List<String> allInstances,
      List<String> partitions, int numReplicas) {

    PartitionAssignment partitionAssignment = new PartitionAssignment(tableName);

    Collections.sort(allInstances);
    int numInstances = Math.min(MAX_NUM_SERVERS, allInstances.size());
    List<String> instancesToUse = new ArrayList<>();
    if (allInstances.size() <= numInstances) {
      instancesToUse.addAll(allInstances);
    } else {
      int hashedServerId = EqualityUtils.hashCodeOf(tableName) % allInstances.size();
      for (int i = 0; i < numInstances; i++) {
        instancesToUse.add(allInstances.get(hashedServerId++));
        if (hashedServerId == allInstances.size()) {
          hashedServerId = 0;
        }
      }
    }

    int hashedStartingServer = EqualityUtils.hashCodeOf(tableName) % numInstances;
    for (String partition : partitions) {
      List<String> instances = new ArrayList<>(numReplicas);
      for (int r = 0; r < numReplicas; r++) {
        instances.add(instancesToUse.get(hashedStartingServer++));
        if (hashedStartingServer == numInstances) {
          hashedStartingServer = 0;
        }
      }
      partitionAssignment.addPartition(partition, instances);
    }
    return partitionAssignment;
  }
}
