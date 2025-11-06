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
package org.apache.pinot.broker.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FederatedRoutingManager manages routing across multiple ZooKeeper clusters.
 * It maintains a primary BrokerRoutingManager and multiple secondary routing managers,
 * each connected to their own ZooKeeper cluster.
 */
public class FederatedRoutingManager implements RoutingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(FederatedRoutingManager.class);

  // Primary routing manager (connected to primary ZooKeeper cluster)
  private final BrokerRoutingManager _primaryRoutingManager;

  // Secondary routing managers (connected to secondary ZooKeeper clusters)
  private final List<BrokerRoutingManager> _secondaryRoutingManagers;

  // Combined routing information
  private final Map<String, ServerInstance> _combinedServerInstanceMap = new ConcurrentHashMap<>();
  private final Map<String, RoutingTable> _combinedRoutingTableCache = new ConcurrentHashMap<>();

  public FederatedRoutingManager(BrokerRoutingManager primaryRoutingManager,
      List<BrokerRoutingManager> secondaryRoutingManagers) {
    _primaryRoutingManager = primaryRoutingManager;
    _secondaryRoutingManagers = secondaryRoutingManagers;
  }

  private void updateCombinedServerInstances() {
    _combinedServerInstanceMap.clear();

    // Add server instances from primary routing manager
    Map<String, ServerInstance> primaryServerInstances = _primaryRoutingManager.getEnabledServerInstanceMap();
    _combinedServerInstanceMap.putAll(primaryServerInstances);

    // Add server instances from secondary routing managers
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      Map<String, ServerInstance> secondaryServerInstances = secondaryRoutingManager.getEnabledServerInstanceMap();
      _combinedServerInstanceMap.putAll(secondaryServerInstances);
    }

    LOGGER.info("Updated combined server instances. Total: {}", _combinedServerInstanceMap.size());
  }

  @Override
  public boolean routingExists(String tableNameWithType) {
    // Check if routing exists in primary routing manager
    if (_primaryRoutingManager.routingExists(tableNameWithType)) {
      return true;
    }

    // Check if routing exists in any secondary routing manager
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      if (secondaryRoutingManager.routingExists(tableNameWithType)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isTableDisabled(String tableNameWithType) {
    // Check if table is disabled in primary routing manager
    if (_primaryRoutingManager.isTableDisabled(tableNameWithType)) {
      return true;
    }

    // Check if table is disabled in any secondary routing manager
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      if (secondaryRoutingManager.isTableDisabled(tableNameWithType)) {
        return true;
      }
    }

    return false;
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
    String tableNameWithType = brokerRequest.getQuerySource().getTableName();
    return getRoutingTable(brokerRequest, tableNameWithType, requestId);
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
    // Check cache first
    String cacheKey = tableNameWithType + "_" + requestId;
    RoutingTable cachedRoutingTable = _combinedRoutingTableCache.get(cacheKey);
    if (cachedRoutingTable != null) {
      return cachedRoutingTable;
    }

    // Get routing table from primary routing manager
    RoutingTable primaryRoutingTable = _primaryRoutingManager.getRoutingTable(brokerRequest,
        tableNameWithType, requestId);

    // Combine with routing tables from secondary routing managers
    RoutingTable combinedRoutingTable = combineRoutingTables(primaryRoutingTable, tableNameWithType,
        brokerRequest, requestId);

    // Cache the result
    if (combinedRoutingTable != null) {
      _combinedRoutingTableCache.put(cacheKey, combinedRoutingTable);
    }

    return combinedRoutingTable;
  }

  private RoutingTable combineRoutingTables(RoutingTable primaryRoutingTable, String tableNameWithType,
      BrokerRequest brokerRequest, long requestId) {
    if (primaryRoutingTable == null) {
      // If primary routing table is null, try to get routing table from secondary managers
      for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
        RoutingTable secondaryRoutingTable = secondaryRoutingManager.getRoutingTable(brokerRequest,
            tableNameWithType, requestId);
        if (secondaryRoutingTable != null) {
          return secondaryRoutingTable;
        }
      }
      return null;
    }

    // Start with primary routing table
    Map<ServerInstance, SegmentsToQuery> combinedServerInstanceToSegmentsMap =
        new HashMap<>(primaryRoutingTable.getServerInstanceToSegmentsMap());
    List<String> combinedUnavailableSegments = new ArrayList<>(primaryRoutingTable.getUnavailableSegments());
    int combinedNumPrunedSegments = primaryRoutingTable.getNumPrunedSegments();

    // Add routing information from secondary routing managers
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      try {
        RoutingTable secondaryRoutingTable = secondaryRoutingManager.getRoutingTable(brokerRequest,
            tableNameWithType, requestId);
        if (secondaryRoutingTable != null) {
          // Combine server instance to segments map
          for (Map.Entry<ServerInstance, SegmentsToQuery> entry
              : secondaryRoutingTable.getServerInstanceToSegmentsMap().entrySet()) {
            ServerInstance serverInstance = entry.getKey();
            SegmentsToQuery secondaryRouteInfo = entry.getValue();

            SegmentsToQuery existingRouteInfo = combinedServerInstanceToSegmentsMap.get(serverInstance);
            if (existingRouteInfo != null) {
              // Merge segments
              existingRouteInfo.getSegments().addAll(secondaryRouteInfo.getSegments());
              existingRouteInfo.getOptionalSegments().addAll(secondaryRouteInfo.getOptionalSegments());
            } else {
              // Add new server instance
              combinedServerInstanceToSegmentsMap.put(serverInstance, secondaryRouteInfo);
            }
          }

          // Combine unavailable segments
          combinedUnavailableSegments.addAll(secondaryRoutingTable.getUnavailableSegments());

          // Add pruned segments count
          combinedNumPrunedSegments += secondaryRoutingTable.getNumPrunedSegments();
        }
      } catch (Exception e) {
        LOGGER.error("Error combining routing table from secondary routing manager for table {}",
            tableNameWithType, e);
      }
    }

    return new RoutingTable(combinedServerInstanceToSegmentsMap,
        combinedUnavailableSegments, combinedNumPrunedSegments);
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo(String tableNameWithType) {
    // Try to get time boundary info from primary routing manager first
    TimeBoundaryInfo primaryTimeBoundaryInfo = _primaryRoutingManager.getTimeBoundaryInfo(tableNameWithType);
    if (primaryTimeBoundaryInfo != null) {
      return primaryTimeBoundaryInfo;
    }

    // Try to get time boundary info from secondary routing managers
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      try {
        TimeBoundaryInfo secondaryTimeBoundaryInfo = secondaryRoutingManager.getTimeBoundaryInfo(tableNameWithType);
        if (secondaryTimeBoundaryInfo != null) {
          return secondaryTimeBoundaryInfo;
        }
      } catch (Exception e) {
        LOGGER.error("Error getting time boundary info from secondary routing manager for table {}",
            tableNameWithType, e);
      }
    }

    return null;
  }

  @Override
  public Map<String, ServerInstance> getEnabledServerInstanceMap() {
    updateCombinedServerInstances();
    return _combinedServerInstanceMap;
  }

  @Override
  public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
    // Try to get table partition info from primary routing manager first
    TablePartitionInfo primaryTablePartitionInfo = _primaryRoutingManager.getTablePartitionInfo(tableNameWithType);
    if (primaryTablePartitionInfo != null) {
      return primaryTablePartitionInfo;
    }

    // Try to get table partition info from secondary routing managers
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      try {
        TablePartitionInfo secondaryTablePartitionInfo =
            secondaryRoutingManager.getTablePartitionInfo(tableNameWithType);
        if (secondaryTablePartitionInfo != null) {
          return secondaryTablePartitionInfo;
        }
      } catch (Exception e) {
        LOGGER.error("Error getting table partition info from secondary routing manager for table {}",
            tableNameWithType, e);
      }
    }

    return null;
  }

  @Override
  public Set<String> getServingInstances(String tableNameWithType) {
    // Get serving instances from primary routing manager
    Set<String> primaryServingInstances = _primaryRoutingManager.getServingInstances(tableNameWithType);
    if (primaryServingInstances == null) {
      primaryServingInstances = Collections.emptySet();
    }
    // Combine with serving instances from secondary routing managers
    Set<String> combinedServingInstances = new HashSet<>(primaryServingInstances);
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      try {
        Set<String> secondaryServingInstances = secondaryRoutingManager.getServingInstances(tableNameWithType);
        if (secondaryServingInstances != null) {
          combinedServingInstances.addAll(secondaryServingInstances);
        }
      } catch (Exception e) {
        LOGGER.error("Error getting serving instances from secondary routing manager for table {}",
            tableNameWithType, e);
      }
    }

    return combinedServingInstances.isEmpty() ? null : combinedServingInstances;
  }

  @Override
  public List<String> getSegments(BrokerRequest brokerRequest) {
    // Get segments from primary routing manager
    List<String> primarySegments = _primaryRoutingManager.getSegments(brokerRequest);

    // Combine with segments from secondary routing managers
    List<String> combinedSegments = new ArrayList<>(primarySegments);
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      try {
        List<String> secondarySegments = secondaryRoutingManager.getSegments(brokerRequest);
        combinedSegments.addAll(secondarySegments);
      } catch (Exception e) {
        LOGGER.error("Error getting segments from secondary routing manager", e);
      }
    }

    return combinedSegments;
  }

  @Override
  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
    // Try to get table partition replicated servers info from primary routing manager first
    TablePartitionReplicatedServersInfo primaryInfo =
        _primaryRoutingManager.getTablePartitionReplicatedServersInfo(tableNameWithType);
    if (primaryInfo != null) {
      return primaryInfo;
    }

    // Try to get table partition replicated servers info from secondary routing managers
    for (BrokerRoutingManager secondaryRoutingManager : _secondaryRoutingManagers) {
      try {
        TablePartitionReplicatedServersInfo secondaryInfo =
            secondaryRoutingManager.getTablePartitionReplicatedServersInfo(tableNameWithType);
        if (secondaryInfo != null) {
          return secondaryInfo;
        }
      } catch (Exception e) {
        LOGGER.error(
            "Error getting table partition replicated servers info from secondary routing manager for table {}",
            tableNameWithType, e);
      }
    }

    return null;
  }

  public RoutingManager getRelevantRoutingManager(Map<String, String> queryOptions) {
    boolean isFederationEnabled = QueryOptionsUtils.isEnableFederation(queryOptions, false);
    if (isFederationEnabled) {
      return this;
    } else {
      return _primaryRoutingManager;
    }
  }
}
