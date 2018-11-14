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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.RoutingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Random;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
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
  protected volatile List<Map<String, List<String>>> _routingTables;

  // A mapping of segments to servers is used for dynamic routing table building process
  protected volatile Map<String, List<String>> _segmentToServersMap;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    _tableName = tableConfig.getTableName();
    _brokerMetrics = brokerMetrics;

    // Enable dynamic routing when the config is explicitly set
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      Map<String, String> routingOption = routingConfig.getRoutingTableBuilderOptions();
      _enableDynamicComputing = Boolean.valueOf(routingOption.get(RoutingConfig.ENABLE_DYNAMIC_COMPUTING_KEY));
      if (_enableDynamicComputing) {
        LOGGER.info("Dynamic routing table computation is enabled for table {}", _tableName);
      }
    }
  }

  protected static String getServerWithLeastSegmentsAssigned(List<String> servers,
      Map<String, List<String>> routingTable) {
    Collections.shuffle(servers);

    String selectedServer = null;
    int minNumSegmentsAssigned = Integer.MAX_VALUE;
    for (String server : servers) {
      List<String> segments = routingTable.get(server);
      if (segments == null) {
        routingTable.put(server, new ArrayList<>());
        return server;
      } else {
        int numSegmentsAssigned = segments.size();
        if (numSegmentsAssigned < minNumSegmentsAssigned) {
          minNumSegmentsAssigned = numSegmentsAssigned;
          selectedServer = server;
        }
      }
    }
    return selectedServer;
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
    Map<String, List<String>> segmentToServersMap =
        computeSegmentToServersMapFromExternalView(externalView, instanceConfigs);

    if (_enableDynamicComputing) {
      // When dynamic computing is enabled, cache the mapping
      _segmentToServersMap = segmentToServersMap;
    } else {
      // Otherwise, we cache the pre-computed routing tables
      List<Map<String, List<String>>> routingTables = computeRoutingTablesFromSegmentToServersMap(segmentToServersMap);
      _routingTables = routingTables;
    }
  }

  @Override
  public Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request) {
    if (_enableDynamicComputing) {
      // Copy the pointer for snapshot since the pointer for segment to servers map can change at anytime
      Map<String, List<String>> segmentToServersMap = _segmentToServersMap;

      // Get all existing segments
      Set<String> segmentsToQuery = segmentToServersMap.keySet();

      // TODO: add the selection logic here

      // Compute the final routing table
      return computeDynamicRoutingTable(segmentToServersMap, segmentsToQuery);
    }

    // Return a pre-computed routing table if we don't use dynamic computing
    return _routingTables.get(_random.nextInt(_routingTables.size()));
  }

  @Override
  public List<Map<String, List<String>>> getRoutingTables() {
    return _routingTables;
  }

  /**
   * Computes a routing table on-the-fly using a mapping of segment to servers. Because of the performance concern,
   * the default behavior is to randomly pick a server among available servers for each segment.
   *
   * @param segmentsToQuery a list of segments that need to be processed for a particular query
   * @return a routing table
   */
  public Map<String, List<String>> computeDynamicRoutingTable(Map<String, List<String>> segmentToServersMap,
      Set<String> segmentsToQuery) {
    Map<String, List<String>> routingTable = new HashMap<>();
    for (String segmentName : segmentsToQuery) {
      List<String> servers = segmentToServersMap.get(segmentName);
      String selectedServer = servers.get(_random.nextInt(servers.size()));
      List<String> segments = routingTable.computeIfAbsent(selectedServer, k -> new ArrayList<>());
      segments.add(segmentName);
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
  protected Map<String, List<String>> computeSegmentToServersMapFromExternalView(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, List<String>> segmentToServersMap = new HashMap<>();
    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      // List of servers that are active and are serving the segment
      List<String> servers = new ArrayList<>();
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName)) {
          servers.add(serverName);
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
  protected abstract List<Map<String, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<String>> segmentToServersMap);
}
