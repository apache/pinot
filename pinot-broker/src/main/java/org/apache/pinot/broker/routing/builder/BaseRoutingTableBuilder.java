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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.selector.SegmentSelector;
import org.apache.pinot.common.config.RoutingConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.transport.ServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base routing table builder class to share common methods between routing table builders.
 */
public abstract class BaseRoutingTableBuilder implements RoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseRoutingTableBuilder.class);

  protected final Random _random = new Random();
  private BrokerMetrics _brokerMetrics;
  private String _tableName;
  private boolean _enableDynamicComputing;

  // Set variable as volatile so all threads can get the up-to-date routing tables
  // Routing tables are used for storing pre-computed routing table
  protected volatile List<Map<ServerInstance, List<String>>> _routingTables;

  // A mapping of segments to servers is used for dynamic routing table building process
  protected volatile Map<String, List<ServerInstance>> _segmentToServersMap;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    _tableName = tableConfig.getTableName();
    _brokerMetrics = brokerMetrics;

    // Enable dynamic routing when the config is explicitly set
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      Map<String, String> routingOption = routingConfig.getRoutingTableBuilderOptions();
      _enableDynamicComputing = Boolean.parseBoolean(routingOption.get(RoutingConfig.ENABLE_DYNAMIC_COMPUTING_KEY));
      if (_enableDynamicComputing) {
        LOGGER.info("Dynamic routing table computation is enabled for table {}", _tableName);
      }
    }
  }

  protected static void assignSegmentToLeastAssignedServer(String segmentName, List<ServerInstance> servers,
      Map<ServerInstance, List<String>> routingTable) {
    Collections.shuffle(servers);

    List<String> segmentsForLeastAssignedServer = null;
    int minNumSegmentsAssigned = Integer.MAX_VALUE;
    for (ServerInstance serverInstance : servers) {
      List<String> segments = routingTable.computeIfAbsent(serverInstance, k -> new ArrayList<>());
      int numSegmentsAssigned = segments.size();
      if (numSegmentsAssigned == 0) {
        segments.add(segmentName);
        return;
      } else {
        if (numSegmentsAssigned < minNumSegmentsAssigned) {
          minNumSegmentsAssigned = numSegmentsAssigned;
          segmentsForLeastAssignedServer = segments;
        }
      }
    }
    assert segmentsForLeastAssignedServer != null;
    segmentsForLeastAssignedServer.add(segmentName);
  }

  /**
   * Helper function to log and emit the metric when no serving server is found for a segment.
   * Note: this has to be called during the external view computation and should not be called during query processing.
   *
   * @param segmentName segment name
   */
  protected void handleNoServingHost(String segmentName) {
    LOGGER.error("Found no server hosting segment {} for table {}", segmentName, _tableName);
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(_tableName, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
    }
  }

  @Override
  public void computeOnExternalViewChange(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, List<ServerInstance>> segmentToServersMap =
        computeSegmentToServersMapFromExternalView(externalView, instanceConfigs);

    if (_enableDynamicComputing) {
      // When dynamic computing is enabled, cache the mapping
      _segmentToServersMap = segmentToServersMap;
    } else {
      // Otherwise, we cache the pre-computed routing tables
      _routingTables = computeRoutingTablesFromSegmentToServersMap(segmentToServersMap);
    }
  }

  public Map<ServerInstance, List<String>> getRoutingTable(RoutingTableLookupRequest request,
      SegmentSelector segmentSelector) {
    if (_enableDynamicComputing) {
      // Copy the pointer for snapshot since the pointer for segment to servers map can change at anytime
      Map<String, List<ServerInstance>> segmentToServersMap = _segmentToServersMap;

      // Selecting segments only required for processing a query
      Set<String> segmentsToQuery = segmentToServersMap.keySet();
      if (segmentSelector != null) {
        segmentsToQuery = segmentSelector.selectSegments(request, segmentsToQuery);
      }

      // Compute the final routing table
      return computeDynamicRoutingTable(segmentToServersMap, segmentsToQuery);
    }

    // Return a pre-computed routing table if we don't use dynamic computing
    return _routingTables.get(_random.nextInt(_routingTables.size()));
  }

  @Override
  public List<Map<ServerInstance, List<String>>> getRoutingTables() {
    return _routingTables;
  }

  /**
   * Computes a routing table on-the-fly using a mapping of segment to servers. Because of the performance concern,
   * the default behavior is to randomly pick a server among available servers for each segment.
   *
   * @param segmentsToQuery a list of segments that need to be processed for a particular query
   * @return a routing table
   */
  public Map<ServerInstance, List<String>> computeDynamicRoutingTable(
      Map<String, List<ServerInstance>> segmentToServersMap, Set<String> segmentsToQuery) {
    Map<ServerInstance, List<String>> routingTable = new HashMap<>();
    for (String segmentName : segmentsToQuery) {
      List<ServerInstance> servers = segmentToServersMap.get(segmentName);
      ServerInstance selectedServer = servers.get(_random.nextInt(servers.size()));
      routingTable.computeIfAbsent(selectedServer, k -> new ArrayList<>()).add(segmentName);
    }
    return routingTable;
  }

  /**
   * Given an external view and a list of instance configs, computes the mapping of segment to servers. The mapping
   * will be cached if we use dynamic routing. By default, this will check ONLINE segments and active servers.
   *
   * @param externalView an external view
   * @param instanceConfigs a list of instance config
   * @return a mapping of segment to servers
   */
  protected Map<String, List<ServerInstance>> computeSegmentToServersMapFromExternalView(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, List<ServerInstance>> segmentToServersMap =
        new HashMap<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    InstanceConfigManager instanceConfigManager = new InstanceConfigManager(instanceConfigs);
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      List<ServerInstance> servers = new ArrayList<>(instanceStateMap.size());
      for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
        if (instanceStateEntry.getValue().equals(SegmentOnlineOfflineStateModel.ONLINE)) {
          InstanceConfig instanceConfig = instanceConfigManager.getActiveInstanceConfig(instanceStateEntry.getKey());
          if (instanceConfig != null) {
            servers.add(new ServerInstance(instanceConfig));
          }
        }
      }
      if (!servers.isEmpty()) {
        segmentToServersMap.put(segmentName, servers);
      } else {
        handleNoServingHost(segmentName);
      }
    }
    return segmentToServersMap;
  }

  /**
   * Given a mapping of segment to servers, compute a list of final routing tables that will be cached when
   * we use pre-computing routing.
   *
   * @param segmentToServersMap a mapping of segment to servers
   * @return a list of final routing tables
   */
  protected abstract List<Map<ServerInstance, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<ServerInstance>> segmentToServersMap);
}
