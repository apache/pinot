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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Partition aware routing table builder for offline table.
 *
 * For an external view change, we need to update the look up table for routing. What we need to do here is to build a
 * routing look up table that can quickly return the server instance when we are given a segment name and replica id.
 * Since we assume that the replica group aware segment assignment strategy is used on the controller, we can now prune
 * servers on the broker. For instance, let's say we have 1 partition, 2 replica groups, 4 servers.
 *
 * P0: RG0(server0, server1), RG1(server2, server3)
 *
 * Because it is guaranteed that servers of a replica group have a complete set of all segments of the table, we can
 * pick either RG0(server0, server1) or RG1(server2, server3) for query. This will reduce the number of servers
 * required for each query to 2 servers from 4 servers.
 *
 *
 * In high level, we need to compute the look up table in the format of (segment_name -> (replica_id -> server_instance))
 * while we are given the following information.
 *   - external view: (segment_name -> set(servers))
 *   - partition to replica group mapping: ((partition_number, replica_id) -> set(servers))
 *
 * We go through multiple steps to get the final look up table. The high level algorithm is as follows:
 *   1. Compute the partition id set by looking at the segment zk metadata and cache metadata when possible.
 *   2. Build a mapping table of (partition_id -> (server -> replica_id)) using partition set from step 1 and
 *      the partition to replica group mapping table from property store.
 *   3. Compute the final routing look up table from external view and the mapping from step 2.
 *
 */
public class PartitionAwareOfflineRoutingTableBuilder extends BasePartitionAwareRoutingTableBuilder {

  // When we use the table level replica group, we can assume that the number of partition is 1.
  private static final int TABLE_LEVEL_PARTITION_NUMBER = 0;

  private boolean _isPartitionLevelReplicaGroupAssignment;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    super.init(configuration, tableConfig, propertyStore, brokerMetrics);
    String partitionColumn = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig().getPartitionColumn();
    _isPartitionLevelReplicaGroupAssignment = (partitionColumn != null);
    _numReplicas = tableConfig.getValidationConfig().getReplicationNumber();
  }

  @Override
  public synchronized void computeOnExternalViewChange(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    Set<String> segmentSet = externalView.getPartitionSet();

    // Fetch the partition to replica group mapping table from the property store
    ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(_propertyStore);
    ReplicaGroupPartitionAssignment partitionAssignment =
        partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableName);

    // Update numReplicas if the replica group partition assignment has been changed.
    int numReplicas = partitionAssignment.getNumReplicaGroups();
    if (_numReplicas != numReplicas) {
      _numReplicas = numReplicas;
    }

    // 1. Compute the partition id set by looking at the segment zk metadata and cache metadata when possible
    Set<Integer> partitionIds = new HashSet<>();
    for (String segmentName : segmentSet) {
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentName);
      if (segmentZKMetadata == null || segmentZKMetadata.getPartitionMetadata() == null
          || segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().size() == 0) {
        segmentZKMetadata = ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentName);
        if (segmentZKMetadata != null) {
          _segmentToZkMetadataMapping.put(segmentName, segmentZKMetadata);
        }
      }
      int partitionId = getPartitionId(segmentZKMetadata);
      if (partitionId != NO_PARTITION_NUMBER) {
        partitionIds.add(partitionId);
      }
    }

    // 2. Build a map from partition to map from server to replica
    Map<Integer, Map<String, Integer>> partitionToServerToReplicaMap = new HashMap<>();
    for (Integer partitionId : partitionIds) {
      for (int replicaId = 0; replicaId < _numReplicas; replicaId++) {
        List<String> serversForPartitionAndReplica =
            partitionAssignment.getInstancesfromReplicaGroup(partitionId, replicaId);
        for (String serverName : serversForPartitionAndReplica) {
          Map<String, Integer> serverToReplicaMap = partitionToServerToReplicaMap.get(partitionId);
          if (serverToReplicaMap == null) {
            serverToReplicaMap = new HashMap<>();
            partitionToServerToReplicaMap.put(partitionId, serverToReplicaMap);
          }
          serverToReplicaMap.put(serverName, replicaId);
        }
      }
    }

    // 3. Compute the final routing look up table
    Map<String, Map<Integer, String>> segmentToReplicaToServerMap = new HashMap<>();
    for (String segmentName : segmentSet) {
      // Get partition_id from cached segment zk metadata
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentName);
      int partitionId = getPartitionId(segmentZKMetadata);

      // Initialize data intermediate data structures or data
      Map<Integer, String> replicaToServerMap = new HashMap<>();
      int replicaIdForNoPartitionMetadata = 0;

      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName)) {
          // If there's no partition number in the metadata, assign replica id sequentially.
          if (partitionId == NO_PARTITION_NUMBER) {
            replicaToServerMap.put(replicaIdForNoPartitionMetadata++, serverName);
          } else {
            int replicaId = partitionToServerToReplicaMap.get(partitionId).get(serverName);
            replicaToServerMap.put(replicaId, serverName);
          }
        }
      }

      // Update the final routing look up table.
      if (!replicaToServerMap.isEmpty()) {
        segmentToReplicaToServerMap.put(segmentName, replicaToServerMap);
      } else {
        handleNoServingHost(segmentName);
      }
    }

    // Delete segment metadata from cache if the segment no longer exists in the external view.
    for (String segmentName : _segmentToZkMetadataMapping.keySet()) {
      if (!segmentSet.contains(segmentName)) {
        _segmentToZkMetadataMapping.remove(segmentName);
      }
    }

    // Update segment to replica to server mapping
    _segmentToReplicaToServerMap = segmentToReplicaToServerMap;
  }

  /**
   * Get partition id from segment Zk metadata. This helper function assumes there is only one column partition metadata.
   *
   * @param segmentZKMetadata segment zk metadata for a segment
   * @return partition id
   */
  private int getPartitionId(SegmentZKMetadata segmentZKMetadata) {
    // If we use the partition level replica group assignment, we need to get the partition id by looking at the
    // segment metadata.
    if (_isPartitionLevelReplicaGroupAssignment) {
      SegmentPartitionMetadata partitionMetadata = segmentZKMetadata.getPartitionMetadata();
      if (partitionMetadata == null) {
        return NO_PARTITION_NUMBER;
      }
      Map<String, ColumnPartitionMetadata> columnPartitionMap = partitionMetadata.getColumnPartitionMap();
      if (columnPartitionMap == null || columnPartitionMap.size() == 0) {
        return NO_PARTITION_NUMBER;
      }
      ColumnPartitionMetadata columnPartitionMetadata;
      if (columnPartitionMap.size() == 1) {
        columnPartitionMetadata = columnPartitionMap.values().iterator().next();
        int partitionIdStart = columnPartitionMetadata.getPartitionRanges().get(0).getMaximumInteger();
        // int partitionIdEnd = columnPartitionMetadata.getPartitionRanges().get(0).getMaximumInteger();
        return partitionIdStart;
      }
    }
    // If we use the table level replica group assignment, we can simply return the default partition number.
    return TABLE_LEVEL_PARTITION_NUMBER;
  }
}
