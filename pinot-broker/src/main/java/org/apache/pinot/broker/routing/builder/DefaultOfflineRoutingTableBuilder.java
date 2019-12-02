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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.selector.SegmentSelector;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.ServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Create a given number of routing tables based on random selections from ExternalView.
 */
public class DefaultOfflineRoutingTableBuilder implements RoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOfflineRoutingTableBuilder.class);

  private RoutingTableBuilder _largeClusterRoutingTableBuilder;
  private RoutingTableBuilder _smallClusterRoutingTableBuilder;

  // Set variable as volatile so all threads can get the up-to-date routing table builder
  private volatile RoutingTableBuilder _routingTableBuilder;

  private int _minServerCountForLargeCluster = 30;
  private int _minReplicaCountForLargeCluster = 4;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    _largeClusterRoutingTableBuilder = new LargeClusterRoutingTableBuilder();
    _smallClusterRoutingTableBuilder = new BalancedRandomRoutingTableBuilder();
    if (configuration.containsKey("minServerCountForLargeCluster")) {
      final String minServerCountForLargeCluster = configuration.getString("minServerCountForLargeCluster");
      try {
        _minServerCountForLargeCluster = Integer.parseInt(minServerCountForLargeCluster);
        LOGGER.info("Using large cluster min server count of {}", _minServerCountForLargeCluster);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not get the large cluster min server count from configuration value {}, keeping default value {}",
            minServerCountForLargeCluster, _minServerCountForLargeCluster, e);
      }
    } else {
      LOGGER.info("Using default value for large cluster min server count of {}", _minServerCountForLargeCluster);
    }

    if (configuration.containsKey("minReplicaCountForLargeCluster")) {
      final String minReplicaCountForLargeCluster = configuration.getString("minReplicaCountForLargeCluster");
      try {
        _minReplicaCountForLargeCluster = Integer.parseInt(minReplicaCountForLargeCluster);
        LOGGER.info("Using large cluster min replica count of {}", _minReplicaCountForLargeCluster);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not get the large cluster min replica count from configuration value {}, keeping default value {}",
            minReplicaCountForLargeCluster, _minReplicaCountForLargeCluster, e);
      }
    } else {
      LOGGER.info("Using default value for large cluster min replica count of {}", _minReplicaCountForLargeCluster);
    }

    _largeClusterRoutingTableBuilder.init(configuration, tableConfig, propertyStore, brokerMetrics);
    _smallClusterRoutingTableBuilder.init(configuration, tableConfig, propertyStore, brokerMetrics);
  }

  @Override
  public void computeOnExternalViewChange(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    if (isLargeCluster(externalView)) {
      _largeClusterRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);
      _routingTableBuilder = _largeClusterRoutingTableBuilder;
    } else {
      _smallClusterRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);
      _routingTableBuilder = _smallClusterRoutingTableBuilder;
    }
  }

  private boolean isLargeCluster(ExternalView externalView) {
    // Check if the number of replicas is sufficient to treat it as a large cluster
    final String helixReplicaCount = externalView.getRecord().getSimpleField("REPLICAS");
    final int replicaCount;

    try {
      replicaCount = Integer.parseInt(helixReplicaCount);
    } catch (Exception e) {
      LOGGER.warn("Failed to parse the replica count ({}) from external view of table {}", helixReplicaCount,
          externalView.getResourceName());
      return false;
    }

    if (replicaCount < _minReplicaCountForLargeCluster) {
      return false;
    }

    // Check if the server count is high enough to count as a large cluster
    final Set<String> instanceSet = new HashSet<>();
    for (String partition : externalView.getPartitionSet()) {
      instanceSet.addAll(externalView.getStateMap(partition).keySet());
    }

    return _minServerCountForLargeCluster <= instanceSet.size();
  }

  @Override
  public Map<ServerInstance, List<String>> getRoutingTable(RoutingTableLookupRequest request,
      SegmentSelector segmentSelector) {
    return _routingTableBuilder.getRoutingTable(request, segmentSelector);
  }

  @Override
  public List<Map<ServerInstance, List<String>>> getRoutingTables() {
    return _routingTableBuilder.getRoutingTables();
  }
}
