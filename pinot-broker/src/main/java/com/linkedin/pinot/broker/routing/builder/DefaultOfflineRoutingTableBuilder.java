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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;

/**
 * Create a given number of routing tables based on random selections from ExternalView.
 */
public class DefaultOfflineRoutingTableBuilder extends AbstractRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOfflineRoutingTableBuilder.class);
  private RoutingTableBuilder _largeClusterRoutingTableBuilder;
  private RoutingTableBuilder _smallClusterRoutingTableBuilder;

  private static int MIN_SERVER_COUNT_FOR_LARGE_CLUSTER = 30;
  private static int MIN_REPLICA_COUNT_FOR_LARGE_CLUSTER = 4;

  RoutingTableBuilder _routingTableBuilder;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _largeClusterRoutingTableBuilder = new LargeClusterRoutingTableBuilder();
    _smallClusterRoutingTableBuilder = new BalancedRandomRoutingTableBuilder();
    if (configuration.containsKey("minServerCountForLargeCluster")) {
      final String minServerCountForLargeCluster = configuration.getString("minServerCountForLargeCluster");
      try {
        MIN_SERVER_COUNT_FOR_LARGE_CLUSTER = Integer.parseInt(minServerCountForLargeCluster);
        LOGGER.info("Using large cluster min server count of {}", MIN_SERVER_COUNT_FOR_LARGE_CLUSTER);
      } catch (Exception e) {
        LOGGER.warn("Could not get the large cluster min server count from configuration value {}, keeping default value {}", minServerCountForLargeCluster,
            MIN_SERVER_COUNT_FOR_LARGE_CLUSTER, e);
      }
    } else {
      LOGGER.info("Using default value for large cluster min server count of {}", MIN_SERVER_COUNT_FOR_LARGE_CLUSTER);
    }

    if (configuration.containsKey("minReplicaCountForLargeCluster")) {
      final String minReplicaCountForLargeCluster = configuration.getString("minReplicaCountForLargeCluster");
      try {
        MIN_REPLICA_COUNT_FOR_LARGE_CLUSTER = Integer.parseInt(minReplicaCountForLargeCluster);
        LOGGER.info("Using large cluster min replica count of {}", MIN_REPLICA_COUNT_FOR_LARGE_CLUSTER);
      } catch (Exception e) {
        LOGGER.warn("Could not get the large cluster min replica count from configuration value {}, keeping default value {}", minReplicaCountForLargeCluster,
            MIN_REPLICA_COUNT_FOR_LARGE_CLUSTER, e);
      }
    } else {
      LOGGER.info("Using default value for large cluster min replica count of {}", MIN_REPLICA_COUNT_FOR_LARGE_CLUSTER);
    }

    _largeClusterRoutingTableBuilder.init(configuration, tableConfig, propertyStore);
    _smallClusterRoutingTableBuilder.init(configuration, tableConfig, propertyStore);
  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigList) {
    if (isLargeCluster(externalView)) {
      _routingTableBuilder = _largeClusterRoutingTableBuilder;
    } else {
      _routingTableBuilder = _smallClusterRoutingTableBuilder;
    }
    _routingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigList);
  }

  private boolean isLargeCluster(ExternalView externalView) {
    // Check if the number of replicas is sufficient to treat it as a large cluster
    final String helixReplicaCount = externalView.getRecord().getSimpleField("REPLICAS");
    final int replicaCount;

    try {
      replicaCount = Integer.parseInt(helixReplicaCount);
    } catch (Exception e) {
      LOGGER.warn("Failed to parse the replica count ({}) from external view of table {}", helixReplicaCount, externalView.getResourceName());
      return false;
    }

    if (replicaCount < MIN_REPLICA_COUNT_FOR_LARGE_CLUSTER) {
      return false;
    }

    // Check if the server count is high enough to count as a large cluster
    final Set<String> instanceSet = new HashSet<>();
    for (String partition : externalView.getPartitionSet()) {
      instanceSet.addAll(externalView.getStateMap(partition).keySet());
    }

    return MIN_SERVER_COUNT_FOR_LARGE_CLUSTER <= instanceSet.size();
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    return _routingTableBuilder.findServers(request);
  }
  
  @Override
  public List<ServerToSegmentSetMap> getRoutingTables() {
    return _routingTableBuilder.getRoutingTables();
  }
}
