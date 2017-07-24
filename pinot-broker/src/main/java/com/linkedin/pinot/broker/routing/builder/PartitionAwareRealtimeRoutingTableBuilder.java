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

package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;

/**
 * Partition aware routing table builder for the Kafka low level consumer.
 *
 * In contrast to the offline partition aware routing builder where the replica group aware segment assignment can be
 * assumed, we do not use the concept of replica group for a realtime table. The routing look up table is simply built
 * from the external view.
 *
 */
public class PartitionAwareRealtimeRoutingTableBuilder extends AbstractPartitionAwareRoutingTableBuilder {

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {

    _numReplicas = Integer.valueOf(_tableConfig.getValidationConfig().getReplicasPerPartition());

    // Update the cache for the segment ZK metadata
    for (String segment : externalView.getPartitionSet()) {
      SegmentId segmentId = new SegmentId(segment);
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentId);
      if (segmentZKMetadata == null || segmentZKMetadata.getPartitionMetadata() == null ||
          segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().size() == 0) {
        segmentZKMetadata = ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, tableName, segment);
        if (segmentZKMetadata != null) {
          _segmentToZkMetadataMapping.put(segmentId, segmentZKMetadata);
        }
      }
    }

    // Gather all segments and group them by Kafka partition, sorted by sequence number
    Map<String, SortedSet<SegmentName>> sortedSegmentsByKafkaPartition =
        KafkaLowLevelRoutingTableBuilderUtil.getSortedSegmentsByKafkaPartition(externalView);

    // Ensure that for each Kafka partition, we have at most one Helix partition (Pinot segment) in consuming state
    Map<String, SegmentName> allowedSegmentInConsumingStateByKafkaPartition =
        KafkaLowLevelRoutingTableBuilderUtil.getAllowedConsumingStateSegments(externalView,
            sortedSegmentsByKafkaPartition);

    RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);

    // Compute segment id to replica id to server instance
    Map<SegmentId, Map<Integer, ServerInstance>> segmentIdToServersMapping = new HashMap<>();
    for (String segmentName : externalView.getPartitionSet()) {
      SegmentId segmentId = new SegmentId(segmentName);
      int partitionId = getPartitionId(segmentName);
      Map<String, String> instanceToStateMap = new HashMap<>(externalView.getStateMap(segmentName));
      Map<Integer, ServerInstance> serverInstanceMap = new HashMap<>();

      String partitionStr = Integer.toString(partitionId);
      SegmentName validConsumingSegment = allowedSegmentInConsumingStateByKafkaPartition.get(partitionStr);

      int replicaId = 0;
      for (String instanceName : instanceToStateMap.keySet()) {
        // Do not add the server if it is inactive
        if (pruner.isInactive(instanceName)) {
          continue;
        }

        String state = instanceToStateMap.get(instanceName);
        ServerInstance serverInstance = ServerInstance.forInstanceName(instanceName);

        // If the server is in ONLINE status, it's always to safe to add
        if (state.equalsIgnoreCase(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
          serverInstanceMap.put(replicaId, serverInstance);
        }

        // If the server is in CONSUMING status, the segment has to be match with the valid consuming segment
        if (state.equalsIgnoreCase(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          if (validConsumingSegment != null && segmentName.equals(validConsumingSegment.getSegmentName())) {
            serverInstanceMap.put(replicaId, serverInstance);
          }
        }
        replicaId++;
      }

      // Update the final routing look up table.
      if (!serverInstanceMap.isEmpty()) {
        segmentIdToServersMapping.put(segmentId, serverInstanceMap);
      }
    }

    // Delete segment metadata from the cache if the segment no longer exists in the external view.
    Set<String> segmentsFromExternalView = externalView.getPartitionSet();
    for (SegmentId segmentId : _segmentToZkMetadataMapping.keySet()) {
      if (!segmentsFromExternalView.contains(segmentId.getSegmentId())) {
        _segmentToZkMetadataMapping.remove(segmentId);
      }
    }

    _mappingReference.set(segmentIdToServersMapping);
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
