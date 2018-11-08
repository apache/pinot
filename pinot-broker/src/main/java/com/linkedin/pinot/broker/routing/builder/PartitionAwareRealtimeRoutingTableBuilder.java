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
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.LLCUtils;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Partition aware routing table builder for the low level consumer.
 *
 * In contrast to the offline partition aware routing builder where the replica group aware segment assignment can be
 * assumed, we do not use the concept of replica group for a realtime table. The routing look up table is simply built
 * from the external view.
 *
 */
public class PartitionAwareRealtimeRoutingTableBuilder extends BasePartitionAwareRoutingTableBuilder {

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    super.init(configuration, tableConfig, propertyStore, brokerMetrics);
    _numReplicas = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());
  }

  @Override
  public synchronized void computeOnExternalViewChange(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    // Update the cache for the segment ZK metadata
    Set<String> segmentSet = externalView.getPartitionSet();
    for (String segmentName : segmentSet) {
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentName);
      if (segmentZKMetadata == null || segmentZKMetadata.getPartitionMetadata() == null
          || segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().size() == 0) {
        segmentZKMetadata = ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, tableName, segmentName);
        if (segmentZKMetadata != null) {
          _segmentToZkMetadataMapping.put(segmentName, segmentZKMetadata);
        }
      }
    }

    // Gather all segments and group them by stream partition id, sorted by sequence number
    Map<String, SortedSet<SegmentName>> sortedSegmentsByStreamPartition =
        LLCUtils.sortSegmentsByStreamPartition(externalView.getPartitionSet());

    // Ensure that for each partition, we have at most one Helix partition (Pinot segment) in consuming state
    Map<String, SegmentName> allowedSegmentInConsumingStateByPartition =
        LowLevelRoutingTableBuilderUtil.getAllowedConsumingStateSegments(externalView,
            sortedSegmentsByStreamPartition);

    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);

    // Compute map from segment to map from replica to server
    Map<String, Map<Integer, String>> segmentToReplicaToServerMap = new HashMap<>();
    for (String segmentName : segmentSet) {
      int partitionId = getPartitionId(segmentName);
      SegmentName validConsumingSegment =
          allowedSegmentInConsumingStateByPartition.get(Integer.toString(partitionId));

      Map<Integer, String> replicaToServerMap = new HashMap<>();
      int replicaId = 0;
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        String state = entry.getValue();

        // Do not add the server if it is inactive
        if (instancePruner.isInactive(serverName)) {
          continue;
        }

        // If the server is in ONLINE status, it's always to safe to add
        if (state.equals(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
          replicaToServerMap.put(replicaId++, serverName);
        }

        // If the server is in CONSUMING status, the segment has to be match with the valid consuming segment
        if (state.equals(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)
            && validConsumingSegment != null && segmentName.equals(validConsumingSegment.getSegmentName())) {
          replicaToServerMap.put(replicaId++, serverName);
        }
      }

      // Update the final routing look up table.
      if (!replicaToServerMap.isEmpty()) {
        segmentToReplicaToServerMap.put(segmentName, replicaToServerMap);
      } else {
        handleNoServingHost(segmentName);
      }
    }

    // Delete segment metadata from the cache if the segment no longer exists in the external view.
    for (String segmentName : _segmentToZkMetadataMapping.keySet()) {
      if (!segmentSet.contains(segmentName)) {
        _segmentToZkMetadataMapping.remove(segmentName);
      }
    }

    // Get the unique set of replica ids and find the maximum id to update the number of replicas
    Set<Integer> replicaGroupIds = new HashSet<>();
    for (Map<Integer, String> replicaToServer : segmentToReplicaToServerMap.values()) {
      replicaGroupIds.addAll(replicaToServer.keySet());
    }
    int numReplicas = Collections.max(replicaGroupIds) + 1;
    if (_numReplicas != numReplicas) {
      _numReplicas = numReplicas;
    }

    // Update segment to replica to server mapping
    _segmentToReplicaToServerMap = segmentToReplicaToServerMap;
  }

  /**
   * Retrieve the partition Id from the segment name of the realtime segment
   *
   * @param segmentName the name of the realtime segment
   * @return partition id of the segment
   */
  private int getPartitionId(String segmentName) {
    final LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    return llcSegmentName.getPartitionId();
  }
}
