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
package com.linkedin.pinot.controller.utils;

import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignment;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ReplicaGroupTestUtils {
  private static final String SEGMENT_PREFIX = "segment_";

  private ReplicaGroupTestUtils() {
  }

  public static Map<Integer, Set<String>> uploadMultipleSegmentsWithPartitionNumber(String tableName, int numSegments,
      String partitionColumn, PinotHelixResourceManager resourceManager, int numPartition) {
    Map<Integer, Set<String>> segmentsPerPartition = new HashMap<>();
    for (int i = 0; i < numSegments; ++i) {
      int partition = i % numPartition;
      String segmentName = SEGMENT_PREFIX + i;
      SegmentMetadata segmentMetadata =
          SegmentMetadataMockUtils.mockSegmentMetadataWithPartitionInfo(tableName, segmentName, partitionColumn, partition);
      resourceManager.addNewSegment(segmentMetadata, "downloadUrl");
      if (!segmentsPerPartition.containsKey(partition)) {
        segmentsPerPartition.put(partition, new HashSet<String>());
      }
      segmentsPerPartition.get(partition).add(segmentName);
    }
    return segmentsPerPartition;
  }

  public static void uploadSingleSegmentWithPartitionNumber(String tableName, String segmentName,
      String partitionColumn, PinotHelixResourceManager resourceManager) {
    SegmentMetadata segmentMetadata =
        SegmentMetadataMockUtils.mockSegmentMetadataWithPartitionInfo(tableName, segmentName, partitionColumn, 0);
    resourceManager.addNewSegment(segmentMetadata, "downloadUrl");
  }

  /**
   * Validate if the segment assignments satisfies the conditions for the replica group configuration
   *
   * @param tableConfig Table config
   * @param replicaGroupMapping Replica group server mapping
   * @param segmentAssignment Segment assignment to validate
   * @return True if the given segment assignment satisfies the condition for replica group segment assignment.
   *         False otherwise
   */
  public static boolean validateReplicaGroupSegmentAssignment(TableConfig tableConfig,
      ReplicaGroupPartitionAssignment replicaGroupMapping, Map<String, Map<String, String>> segmentAssignment,
      Map<Integer, Set<String>> segmentsPerPartition) {
    ReplicaGroupStrategyConfig replicaGroupConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();

    // Create the server to segments mapping
    Map<String, List<String>> serverToSegments = new HashMap<>();
    for (String server: replicaGroupMapping.getAllInstances()) {
      serverToSegments.put(server, new ArrayList<String>());
    }

    for (Map.Entry<String, Map<String, String>> entry: segmentAssignment.entrySet()) {
      String segment = entry.getKey();
      for (String server: entry.getValue().keySet()) {
        if (!serverToSegments.containsKey(server)) {
          serverToSegments.put(server, new ArrayList<String>());
        }
        serverToSegments.get(server).add(segment);
      }
    }

    // Build the mapping from partition to a list of all segments for that partition
    int numPartitions = replicaGroupMapping.getNumPartitions();
    int numReplicaGroups = replicaGroupMapping.getNumReplicaGroups();

    // Check if the servers in a replica group covers all segments
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
        List<String> replicaGroup = replicaGroupMapping.getInstancesfromReplicaGroup(partitionId, replicaId);
        Set<String> replicaGroupSegments = new HashSet<>();
        for (String server: replicaGroup) {
          for (String segment: serverToSegments.get(server)) {
            if (segmentsPerPartition.get(partitionId).contains(segment)) {
              if (!replicaGroupSegments.contains(segment)) {
                replicaGroupSegments.add(segment);
              } else {
                // Each replica group needs to cover a segment only for once
                return false;
              }
            }
          }
        }
        if (!segmentsPerPartition.get(partitionId).equals(replicaGroupSegments)) {
          return false;
        }
      }
    }

    // If mirroring is enabled, need to check if servers with the same index contains the same segment
    if (replicaGroupConfig.getMirrorAssignmentAcrossReplicaGroups()) {
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        for (int serverIndex = 0; serverIndex < replicaGroupConfig.getNumInstancesPerPartition(); serverIndex++) {
          Set<String> mirrorSegments = new HashSet<>();
          for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
            List<String> replicaGroup = replicaGroupMapping.getInstancesfromReplicaGroup(partitionId, replicaId);
            String server = replicaGroup.get(serverIndex);
            Set<String> currentSegments = new HashSet<>(serverToSegments.get(server));
            mirrorSegments.addAll(currentSegments);
            if (!mirrorSegments.equals(currentSegments)) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }
}
