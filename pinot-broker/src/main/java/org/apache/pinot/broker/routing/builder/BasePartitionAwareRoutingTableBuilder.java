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
package org.apache.pinot.broker.routing.builder;

import it.unimi.dsi.fastutil.ints.IntArrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.pruner.SegmentPrunerContext;
import org.apache.pinot.broker.pruner.SegmentZKMetadataPrunerService;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.selector.SegmentSelector;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.ServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base partition aware routing table builder.
 *
 * For an external view change, a subclass is in change of updating the look up table that is used
 * for routing. The look up table is in the format of < segment_name -> (replica_id -> server_instance) >.
 *
 * When the query comes in, the routing algorithm is as follows:
 *   1. Shuffle the replica group ids
 *   2. For each segment of the given table,
 *      a. Check if the segment can be pruned. If pruned, go to the next segment.
 *      b. If not pruned, assign the segment to a server with the replica id based on the shuffled replica group ids.
 *
 */
public abstract class BasePartitionAwareRoutingTableBuilder implements RoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePartitionAwareRoutingTableBuilder.class);

  protected static final String PARTITION_METADATA_PRUNER = "PartitionZKMetadataPruner";
  protected static final int NO_PARTITION_NUMBER = -1;

  // Map from segment name to map from replica id to server
  // Set variable as volatile so all threads can get the up-to-date map
  protected volatile Map<String, Map<Integer, ServerInstance>> _segmentToReplicaToServerMap;

  // Cache for segment zk metadata to reduce the lookup to ZK store
  protected Map<String, SegmentZKMetadata> _segmentToZkMetadataMapping = new ConcurrentHashMap<>();

  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected SegmentZKMetadataPrunerService _pruner;
  protected Random _random = new Random();
  protected volatile int _numReplicas;

  private BrokerMetrics _brokerMetrics;
  private String _tableName;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    _propertyStore = propertyStore;

    _tableName = tableConfig.getTableName();
    _brokerMetrics = brokerMetrics;
    // TODO: We need to specify the type of pruners via config instead of hardcoding.
    _pruner = new SegmentZKMetadataPrunerService(new String[]{PARTITION_METADATA_PRUNER});
  }

  @Override
  public Map<ServerInstance, List<String>> getRoutingTable(RoutingTableLookupRequest request, SegmentSelector segmentSelector) {
    // Copy the reference for the current segment to replica to server mapping for snapshot
    Map<String, Map<Integer, ServerInstance>> segmentToReplicaToServerMap = _segmentToReplicaToServerMap;

    // Get all available segments for table
    Set<String> segmentsToQuery = segmentToReplicaToServerMap.keySet();

    // Selecting segments only required for processing a query
    if (segmentSelector != null) {
      segmentsToQuery = segmentSelector.selectSegments(request, segmentsToQuery);
    }

    Map<ServerInstance, List<String>> routingTable = new HashMap<>();
    SegmentPrunerContext prunerContext = new SegmentPrunerContext(request.getBrokerRequest());

    // Shuffle the replica group ids in order to satisfy:
    // a. Pick a replica group in an evenly distributed fashion
    // b. When a server is not available, the request should be distributed evenly among other available servers.
    int[] shuffledReplicaGroupIds = new int[_numReplicas];
    for (int i = 0; i < _numReplicas; i++) {
      shuffledReplicaGroupIds[i] = i;
    }
    IntArrays.shuffle(shuffledReplicaGroupIds, _random);

    for (String segmentName : segmentsToQuery) {
      SegmentZKMetadata segmentZKMetadata = _segmentToZkMetadataMapping.get(segmentName);

      // 2a. Check if the segment can be pruned
      boolean segmentPruned = (segmentZKMetadata != null) && _pruner.prune(segmentZKMetadata, prunerContext);

      if (!segmentPruned) {
        // 2b. Segment cannot be pruned. Assign the segment to a server based on the shuffled replica group ids
        Map<Integer, ServerInstance> replicaIdToServerMap = segmentToReplicaToServerMap.get(segmentName);

        ServerInstance serverInstance = null;
        for (int i = 0; i < _numReplicas; i++) {
          serverInstance = replicaIdToServerMap.get(shuffledReplicaGroupIds[i]);
          // If a server is found, update routing table for the current segment
          if (serverInstance != null) {
            break;
          }
        }

        if (serverInstance != null) {
          routingTable.computeIfAbsent(serverInstance, k -> new ArrayList<>()).add(segmentName);
        } else {
          // No server is found for this segment if the code reach here

          // TODO: we need to discuss and decide on how we will be handling this case since we are not returning the
          // complete result here.
        }
      }
    }

    return routingTable;
  }

  @Override
  public List<Map<ServerInstance, List<String>>> getRoutingTables() {
    throw new UnsupportedOperationException("Partition aware routing table cannot be pre-computed");
  }

  protected void handleNoServingHost(String segmentName) {
    LOGGER.error("Found no server hosting segment {} for table {}", segmentName, _tableName);
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(_tableName, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
    }
  }
}
