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


/// The `MultiClusterRoutingManager` implements the [RoutingManager] to support multi-cluster routing.
/// It contains a local [BrokerRoutingManager] and multiple remote [RemoteClusterBrokerRoutingManager]
/// instances. For each routing request, it first queries the local cluster routing manager, and then queries the remote
/// cluster routing managers to combine the results.
/// For example, when getting the routing table for a table, it first gets the routing table from the local cluster
/// routing manager, and then gets the routing tables from the remote cluster routing managers to merge into a combined
/// routing table.
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
  public Map<String, ServerInstance> getRoutableServerInstanceMap() {
    Map<String, ServerInstance> combined = new HashMap<>(_localClusterRoutingManager.getRoutableServerInstanceMap());
    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      combined.putAll(remoteCluster.getRoutableServerInstanceMap());
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

  /// Combines by *intersection* over the clusters that have routing for the table, which is the opposite of how
  /// [#getSegments] combines and deliberately so: that returns segments that survive, and a segment survives if any
  /// cluster keeps it, while this returns segments that are provably eliminated, and a proof only holds if every
  /// cluster that could route the segment eliminated it. Unioning instead would let one cluster's pruners speak for a
  /// segment another cluster would still have queried -- silently dropping matching data.
  ///
  /// Restricting the intersection to the clusters that have the table is what keeps it useful: the usual case is a
  /// table in exactly one cluster, where the intersection is that cluster's own verdict. A cluster without the table
  /// would otherwise contribute an empty set and reduce every answer to "nothing proven".
  @Nullable
  @Override
  public Set<String> getPrunedSegments(BrokerRequest brokerRequest) {
    String tableNameWithType = brokerRequest.getQuerySource().getTableName();
    Set<String> combined = intersectPrunedSegments(null, _localClusterRoutingManager, brokerRequest,
        tableNameWithType);
    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      combined = intersectPrunedSegments(combined, remoteCluster, brokerRequest, tableNameWithType);
    }
    // Still null when no cluster has the table at all, which is what the interface reports for a table that does not
    // exist -- as opposed to an empty set, which is a cluster that ran the pruners and proved nothing.
    return combined;
  }

  /// Folds one cluster's verdict into the running intersection, or returns an empty set to end it: once nothing is
  /// proven, nothing downstream can make it provable again, and asking the remaining clusters would run a full
  /// selection and pruner chain each for an answer that is already fixed. `null` means no cluster has answered yet.
  @Nullable
  private Set<String> intersectPrunedSegments(@Nullable Set<String> combined, BaseBrokerRoutingManager cluster,
      BrokerRequest brokerRequest, String tableNameWithType) {
    if (combined != null && combined.isEmpty()) {
      return combined;
    }
    try {
      // One lookup rather than routingExists-then-get: a table appearing between the two would let this skip a
      // cluster that can route it, and the intersection would then claim segments eliminated that nobody asked about.
      Set<String> prunedSegments = cluster.getPrunedSegments(brokerRequest);
      if (prunedSegments == null) {
        // This cluster has no routing for the table, so it eliminates nothing and constrains nothing.
        return combined;
      }
      if (prunedSegments.isEmpty()) {
        // This cluster proves nothing, so neither does the intersection.
        return Set.of();
      }
      if (combined == null) {
        return new HashSet<>(prunedSegments);
      }
      combined.retainAll(prunedSegments);
      return combined;
    } catch (Exception e) {
      LOGGER.error("Error getting pruned segments from cluster routing manager for table {}", tableNameWithType, e);
      // A cluster we could not ask may still have routed any of these segments, so prove nothing.
      return Set.of();
    }
  }

  /// Returns the partition info only when a single cluster has any, and `null` when more than one does.
  ///
  /// Unlike [#getRoutingTable], [#getSegments] and [#getServingInstances], this cannot union the clusters: the info is
  /// a per-partition array of the servers holding every segment of that partition, and no server holds the segments
  /// that live in another cluster. One cluster's array would make a partition served only by another cluster look like
  /// a partition holding no data, and a colocated join treats such a partition as empty and silently drops its rows. So
  /// a table spread over several clusters reports nothing and its callers fail. Expressing it properly needs the array
  /// to carry each partition's cluster, which the current shape cannot do.
  @Override
  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
    TablePartitionReplicatedServersInfo partitionInfo =
        _localClusterRoutingManager.getTablePartitionReplicatedServersInfo(tableNameWithType);
    for (BaseBrokerRoutingManager remoteCluster : _remoteClusterRoutingManagers) {
      TablePartitionReplicatedServersInfo remotePartitionInfo;
      try {
        remotePartitionInfo = remoteCluster.getTablePartitionReplicatedServersInfo(tableNameWithType);
      } catch (Exception e) {
        LOGGER.error("Error getting table partition info from remote cluster routing manager for table {}",
            tableNameWithType, e);
        continue;
      }
      if (remotePartitionInfo == null) {
        continue;
      }
      if (partitionInfo != null) {
        LOGGER.warn("Found table partition info in multiple clusters for table: {}, returning null so that "
            + "partition-aware routing is not attempted on a partial view", tableNameWithType);
        return null;
      }
      partitionInfo = remotePartitionInfo;
    }
    return partitionInfo;
  }
}
