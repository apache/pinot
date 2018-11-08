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

import com.linkedin.pinot.broker.pruner.SegmentPrunerContext;
import com.linkedin.pinot.broker.pruner.SegmentZKMetadataPrunerService;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base partition aware routing table builder.
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
public abstract class BasePartitionAwareRoutingTableBuilder implements RoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePartitionAwareRoutingTableBuilder.class);

  protected static final String PARTITION_METADATA_PRUNER = "PartitionZKMetadataPruner";
  protected static final int NO_PARTITION_NUMBER = -1;

  // Map from segment name to map from replica id to server
  // Set variable as volatile so all threads can get the up-to-date map
  protected volatile Map<String, Map<Integer, String>> _segmentToReplicaToServerMap;

  // Cache for segment zk metadata to reduce the lookup to ZK store
  protected Map<String, SegmentZKMetadata> _segmentToZkMetadataMapping = new ConcurrentHashMap<>();

  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected SegmentZKMetadataPrunerService _pruner;
  protected Random _random = new Random();
  protected volatile int _numReplicas;

  private BrokerMetrics _brokerMetrics;
  private String _tableName;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics) {
    _propertyStore = propertyStore;

    _tableName = tableConfig.getTableName();
    _brokerMetrics = brokerMetrics;
    // TODO: We need to specify the type of pruners via config instead of hardcoding.
    _pruner = new SegmentZKMetadataPrunerService(new String[]{PARTITION_METADATA_PRUNER});
  }

  @Override
  public Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request) {
    // Copy the reference for the current segment to replica to server mapping for snapshot
    Map<String, Map<Integer, String>> segmentToReplicaToServerMap = _segmentToReplicaToServerMap;

    // Get all available segments for table
    Set<String> segmentsToQuery = segmentToReplicaToServerMap.keySet();

    // TODO: add the selection logic here

    Map<String, List<String>> routingTable = new HashMap<>();
    SegmentPrunerContext prunerContext = new SegmentPrunerContext(request.getBrokerRequest());

    // 1. Randomly pick a replica id
    int replicaId = _random.nextInt(_numReplicas);
    for (String segmentName : segmentsToQuery) {
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentName);

      // 2a. Check if the segment can be pruned
      boolean segmentPruned = (segmentZKMetadata != null) && _pruner.prune(segmentZKMetadata, prunerContext);

      if (!segmentPruned) {
        // 2b. Segment cannot be pruned. Assign the segment to a server with the replica id picked above.
        Map<Integer, String> replicaIdToServerMap = segmentToReplicaToServerMap.get(segmentName);
        String serverName = replicaIdToServerMap.get(replicaId);

        // When the server is not available with this replica id, we need to pick another available server.
        if (serverName == null) {
          if (!replicaIdToServerMap.isEmpty()) {
            serverName = replicaIdToServerMap.values().iterator().next();
          } else {
            // No server is found for this segment
            continue;
          }
        }
        List<String> segmentsForServer = routingTable.get(serverName);
        if (segmentsForServer == null) {
          segmentsForServer = new ArrayList<>();
          routingTable.put(serverName, segmentsForServer);
        }
        segmentsForServer.add(segmentName);
      }
    }

    return routingTable;
  }

  @Override
  public List<Map<String, List<String>>> getRoutingTables() {
    throw new UnsupportedOperationException("Partition aware routing table cannot be pre-computed");
  }

  protected void handleNoServingHost(String segmentName) {
    LOGGER.error("Found no server hosting segment {} for table {}", segmentName, _tableName);
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(_tableName, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
    }
  }
}
