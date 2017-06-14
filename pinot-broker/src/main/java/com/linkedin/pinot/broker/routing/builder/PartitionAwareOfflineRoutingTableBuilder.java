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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.broker.pruner.PartitionZKMetadataPruner;
import com.linkedin.pinot.broker.pruner.SegmentPrunerContext;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;

public class PartitionAwareOfflineRoutingTableBuilder extends AbstractRoutingTableBuilder {
  Map<SegmentId, Map<Integer, ServerInstance>> _segmentId2ServersMapping = new HashMap<>();
  AtomicReference<Map<SegmentId, Map<Integer, ServerInstance>>> _mappingRef = new AtomicReference<>(_segmentId2ServersMapping);
  Map<SegmentId, SegmentZKMetadata> _zkSegment2ColumnMetadataMap = new ConcurrentHashMap<>();
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private PartitionZKMetadataPruner _pruner;
  private PartitionToReplicaGroupMappingZKMetadata _partitionToReplicaGroupMappingZKMedata;
  private Random _random = new Random();
  private TableConfig _tableConfig;
  private int _numReplicas;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableConfig = tableConfig;
    _propertyStore = propertyStore;
    _pruner = new PartitionZKMetadataPruner();
  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigList) {
    Map<SegmentId, Map<Integer, ServerInstance>> segmentId2ServersMapping = new HashMap<>();
    RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);
    String[] segmentSet = externalView.getPartitionSet().toArray(new String[0]);
    _numReplicas = _tableConfig.getValidationConfig().getReplicationNumber();
    _partitionToReplicaGroupMappingZKMedata = ZKMetadataProvider.getPartitionToReplicaGroupMappingZKMedata(_propertyStore, tableName);

    Set<Integer> partitionIds = new HashSet<>();
    for (String segment : segmentSet) {
      SegmentId segmentId = new SegmentId(segment);
      // retrieve the metadata for the segment and compute the partitionIds set
      SegmentZKMetadata segmentZKMetadata = _zkSegment2ColumnMetadataMap.get(segmentId);
      if (segmentZKMetadata == null) {
        segmentZKMetadata = ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segment);
        _zkSegment2ColumnMetadataMap.put(segmentId, segmentZKMetadata);
      }
      int partitionId = getPartitionId(segmentZKMetadata);
      partitionIds.add(partitionId);

    }
    // compute server to replica id mapping for each partition
    Map<Integer, Map<ServerInstance, Integer>> perPartitionServer2ReplicaIdMapping = new HashMap<>();
    for (Integer partitionId : partitionIds) {
      for (int replicaId = 0; replicaId < _numReplicas; replicaId++) {
        List<String> instancesfromReplicaGroup = _partitionToReplicaGroupMappingZKMedata.getInstancesfromReplicaGroup(partitionId, replicaId);
        for (String instanceName : instancesfromReplicaGroup) {
          perPartitionServer2ReplicaIdMapping.get(partitionId).put(ServerInstance.forInstanceName(instanceName), replicaId);
        }
      }

    }

    for (String segment : segmentSet) {
      SegmentId segmentId = new SegmentId(segment);
      SegmentZKMetadata segmentZKMetadata = _zkSegment2ColumnMetadataMap.get(segmentId);
      int partitionId = getPartitionId(segmentZKMetadata);
      Map<String, String> instanceToStateMap = new HashMap<>(externalView.getStateMap(segment));
      Map<Integer, ServerInstance> serverInstanceMap = new HashMap<>();

      for (String instance : instanceToStateMap.keySet()) {
        if (pruner.isInactive(instance)) {
          continue;
        }
        if (instanceToStateMap.get(instance).equals("ONLINE")) {
          ServerInstance serverInstance = ServerInstance.forInstanceName(instance);
          int replicaId = perPartitionServer2ReplicaIdMapping.get(partitionId).get(serverInstance);
          serverInstanceMap.put(replicaId, serverInstance);
        }
      }
      segmentId2ServersMapping.put(segmentId, serverInstanceMap);
    }
    _mappingRef.set(segmentId2ServersMapping);

  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {

    Map<ServerInstance, SegmentIdSet> result = new HashMap<>();
    Map<SegmentId, Map<Integer, ServerInstance>> map = _mappingRef.get();
    SegmentPrunerContext prunerContext = new SegmentPrunerContext(request.getBrokerRequest());
    int replicaGroupId = _random.nextInt(_numReplicas);
    for (SegmentId segmentId : map.keySet()) {
      SegmentZKMetadata segmentZKMetadata = _zkSegment2ColumnMetadataMap.get(segmentId);
      boolean pruned = _pruner.prune(segmentZKMetadata, prunerContext);
      if (!pruned) {
        Map<Integer, ServerInstance> replicaId2ServerMapping = map.get(segmentId);
        ServerInstance serverInstance = replicaId2ServerMapping.get(replicaGroupId);
        // pick any other available server instance when the node is down/disabled
        if (serverInstance == null && !replicaId2ServerMapping.isEmpty()) {
          serverInstance = replicaId2ServerMapping.values().iterator().next();
        }
        SegmentIdSet segmentIdSet = result.get(serverInstance);
        if (segmentIdSet == null) {
          segmentIdSet = new SegmentIdSet();
          result.put(serverInstance, segmentIdSet);
        }
        segmentIdSet.addSegment(segmentId);
      }
    }
    return result;
  }

  /**
   * Assumes there is only one column
   * 
   * @param segmentZKMetadata
   * @return
   */
  private int getPartitionId(SegmentZKMetadata segmentZKMetadata) {
    SegmentPartitionMetadata partitionMetadata = segmentZKMetadata.getPartitionMetadata();
    Map<String, ColumnPartitionMetadata> columnPartitionMap = partitionMetadata.getColumnPartitionMap();
    ColumnPartitionMetadata columnPartitionMetadata;
    if (columnPartitionMap.size() == 1) {
      columnPartitionMetadata = columnPartitionMap.values().iterator().next();
      int partitionIdStart = columnPartitionMetadata.getPartitionRanges().get(0).getMaximumInteger();
      // int partitionIdEnd = columnPartitionMetadata.getPartitionRanges().get(0).getMaximumInteger();
      return partitionIdStart;
    }
    return 0;
  }

  @Override
  public boolean isPartitionAware() {
    return true;
  }

}
