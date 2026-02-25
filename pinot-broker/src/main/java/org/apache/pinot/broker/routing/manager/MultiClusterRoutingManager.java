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
package org.apache.pinot.broker.routing.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
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
 * The {@code MultiClusterRoutingManager} implements the {@link RoutingManager} to support multi-cluster routing.
 * It contains a local {@link BrokerRoutingManager} and multiple remote {@link RemoteClusterBrokerRoutingManager}
 * instances. For each routing request, it first queries the local cluster routing manager, and then queries the remote
 * cluster routing managers to combine the results.
 * For example, when getting the routing table for a table, it first gets the routing table from the local cluster
 * routing manager, and then gets the routing tables from the remote cluster routing managers to merge into a combined
 * routing table.
 */
public class MultiClusterRoutingManager implements RoutingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterRoutingManager.class);

  private final BrokerRoutingManager _localClusterRoutingManager;
  private final List<RemoteClusterBrokerRoutingManager> _remoteClusterRoutingManagers;

  public MultiClusterRoutingManager(BrokerRoutingManager localClusterRoutingManager,
      List<RemoteClusterBrokerRoutingManager> remoteClusterRoutingManagers) {
    _localClusterRoutingManager = localClusterRoutingManager;
    _remoteClusterRoutingManagers = remoteClusterRoutingManagers;
  }

  private Stream<BaseBrokerRoutingManager> allClusters() {
    return Stream.concat(Stream.of(_localClusterRoutingManager), _remoteClusterRoutingManagers.stream());
  }

  @Nullable
  private <T> T findFirst(Function<BaseBrokerRoutingManager, T> getter, String tableNameForLog) {
    return allClusters()
        .map(mgr -> {
          try {
            return getter.apply(mgr);
          } catch (Exception e) {
            LOGGER.error("Error querying remote cluster routing manager for table {}", tableNameForLog, e);
            return null;
          }
        })
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  private boolean anyMatch(Predicate<BaseBrokerRoutingManager> predicate) {
    return allClusters().anyMatch(predicate);
  }

  @Override
  public boolean routingExists(String tableNameWithType) {
    return anyMatch(mgr -> mgr.routingExists(tableNameWithType));
  }

  @Override
  public boolean isTableDisabled(String tableNameWithType) {
    return anyMatch(mgr -> mgr.isTableDisabled(tableNameWithType));
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
    return getRoutingTable(brokerRequest, brokerRequest.getQuerySource().getTableName(), requestId);
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
    RoutingTable localTable = _localClusterRoutingManager.getRoutingTable(brokerRequest, tableNameWithType, requestId);
    return combineRoutingTables(localTable, tableNameWithType, brokerRequest, requestId);
  }

  private RoutingTable combineRoutingTables(@Nullable RoutingTable localTable, String tableNameWithType,
      BrokerRequest brokerRequest, long requestId) {
    Map<ServerInstance, SegmentsToQuery> combinedMap = localTable != null
        ? new HashMap<>(localTable.getServerInstanceToSegmentsMap()) : new HashMap<>();
    List<String> unavailableSegments = localTable != null
        ? new ArrayList<>(localTable.getUnavailableSegments()) : new ArrayList<>();
    int prunedCount = localTable != null ? localTable.getNumPrunedSegments() : 0;

    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      try {
        RoutingTable remoteTable = remoteCluster.getRoutingTable(brokerRequest, tableNameWithType, requestId);
        if (remoteTable != null) {
          mergeRoutingTable(combinedMap, remoteTable);
          unavailableSegments.addAll(remoteTable.getUnavailableSegments());
          prunedCount += remoteTable.getNumPrunedSegments();
        }
      } catch (Exception e) {
        LOGGER.error("Error combining routing table for table {}", tableNameWithType, e);
      }
    }
    return combinedMap.isEmpty() && unavailableSegments.isEmpty() ? null
        : new RoutingTable(combinedMap, unavailableSegments, prunedCount);
  }

  private void mergeRoutingTable(Map<ServerInstance, SegmentsToQuery> target, RoutingTable source) {
    for (Map.Entry<ServerInstance, SegmentsToQuery> entry : source.getServerInstanceToSegmentsMap().entrySet()) {
      SegmentsToQuery existing = target.get(entry.getKey());
      if (existing != null) {
        existing.getSegments().addAll(entry.getValue().getSegments());
        existing.getOptionalSegments().addAll(entry.getValue().getOptionalSegments());
      } else {
        target.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo(String tableNameWithType) {
    return findFirst(mgr -> mgr.getTimeBoundaryInfo(tableNameWithType), tableNameWithType);
  }

  @Override
  public Map<String, ServerInstance> getEnabledServerInstanceMap() {
    Map<String, ServerInstance> combined = new HashMap<>(_localClusterRoutingManager.getEnabledServerInstanceMap());
    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      combined.putAll(remoteCluster.getEnabledServerInstanceMap());
    }
    return combined;
  }

  @Override
  public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
    return findFirst(mgr -> mgr.getTablePartitionInfo(tableNameWithType), tableNameWithType);
  }

  @Override
  public Set<String> getServingInstances(String tableNameWithType) {
    Set<String> combined = new HashSet<>();
    Set<String> localInstances = _localClusterRoutingManager.getServingInstances(tableNameWithType);
    if (localInstances != null) {
      combined.addAll(localInstances);
    }
    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      try {
        Set<String> instances = remoteCluster.getServingInstances(tableNameWithType);
        if (instances != null) {
          combined.addAll(instances);
        }
      } catch (Exception e) {
        LOGGER.error("Error getting serving instances for table {}", tableNameWithType, e);
      }
    }
    return combined.isEmpty() ? null : combined;
  }

  @Override
  public List<String> getSegments(BrokerRequest brokerRequest) {
    return getSegments(brokerRequest, BaseBrokerRoutingManager.extractSamplerName(brokerRequest));
  }

  @Override
  public List<String> getSegments(BrokerRequest brokerRequest, @Nullable String samplerName) {
    List<String> combined = new ArrayList<>();
    List<String> localSegments = _localClusterRoutingManager.getSegments(brokerRequest, samplerName);
    if (localSegments != null) {
      combined.addAll(localSegments);
    }
    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      try {
        List<String> remoteSegments = remoteCluster.getSegments(brokerRequest, samplerName);
        if (remoteSegments != null) {
          combined.addAll(remoteSegments);
        }
      } catch (Exception e) {
        LOGGER.error("Error getting segments from remote cluster routing manager", e);
      }
    }
    return combined.isEmpty() ? null : combined;
  }

  @Override
  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
    return findFirst(mgr -> mgr.getTablePartitionReplicatedServersInfo(tableNameWithType), tableNameWithType);
  }
}
