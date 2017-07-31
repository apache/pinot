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

import com.linkedin.pinot.broker.pruner.SegmentPrunerContext;
import com.linkedin.pinot.broker.pruner.SegmentZKMetadataPrunerService;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.core.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract partition aware routing table builder.
 *
 * For an external view change, a subclass is in change of updating the look up table that is used
 * for routing. The look up table is in the format of < segment_name -> (replica_id -> server_instance) >.
 *
 * When the query comes in, the routing algorithm is as follows:
 *   1. Randomly pick a replica id (or replica group id)
 *   2. For each segment of the given table,
 *      a. Check if the segment can be pruned. If pruned, go to the next segment.
 *      b. If not pruned, assign the segment to a server with the replica id that is picked above.
 *
 */
public abstract class AbstractPartitionAwareRoutingTableBuilder extends AbstractRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPartitionAwareRoutingTableBuilder.class);

  protected static final String PARTITION_METADATA_PRUNER = "PartitionZKMetadataPruner";
  protected static final int NO_PARTITION_NUMBER = -1;

  // Reference mapping table (segment id, (replica group id, server)) that is used for routing.
  Map<SegmentId, Map<Integer, ServerInstance>> _segmentNameToServersMapping = new HashMap<>();
  AtomicReference<Map<SegmentId, Map<Integer, ServerInstance>>> _mappingReference =
      new AtomicReference<>(_segmentNameToServersMapping);

  // Cache for segment zk metadata to reduce the lookup to ZK store.
  Map<SegmentId, SegmentZKMetadata> _segmentToZkMetadataMapping = new ConcurrentHashMap<>();

  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected SegmentZKMetadataPrunerService _pruner;
  protected TableConfig _tableConfig;
  protected Random _random = new Random();
  protected int _numReplicas;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableConfig = tableConfig;
    _propertyStore = propertyStore;

    // TODO: We need to specify the type of pruners via config instead of hardcoding.
    _pruner = new SegmentZKMetadataPrunerService(new String[]{PARTITION_METADATA_PRUNER});
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    Map<ServerInstance, SegmentIdSet> result = new HashMap<>();
    Map<SegmentId, Map<Integer, ServerInstance>> mappingReference = _mappingReference.get();
    SegmentPrunerContext prunerContext = new SegmentPrunerContext(request.getBrokerRequest());

    // 1. Randomly pick a replica id
    int replicaGroupId = _random.nextInt(_numReplicas);
    for (SegmentId segmentId : mappingReference.keySet()) {
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentId);

      // 2a. Check if the segment can be pruned
      boolean segmentPruned = (segmentZKMetadata != null) && _pruner.prune(segmentZKMetadata, prunerContext);

      if (!segmentPruned) {
        // 2b. Segment cannot be pruned. Assign the segment to a server with the replica id picked above.
        Map<Integer, ServerInstance> replicaId2ServerMapping = mappingReference.get(segmentId);
        ServerInstance serverInstance = replicaId2ServerMapping.get(replicaGroupId);

        // When the server is not available with this replica id, we need to pick another available server.
        if (serverInstance == null) {
          if (!replicaId2ServerMapping.isEmpty()) {
            serverInstance = replicaId2ServerMapping.values().iterator().next();
          } else {
            // No server is found for this segment.
            LOGGER.warn("No server is found for the segment {}.", segmentId.getSegmentId());
            continue;
          }
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

  @Override
  public boolean isPartitionAware() {
    return true;
  }
}
