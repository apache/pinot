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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
 * FederatedRoutingManager manages routing across multiple ZooKeeper clusters.
 * It maintains a primary BrokerRoutingManager and multiple secondary routing managers,
 * each connected to their own ZooKeeper cluster.
 */
public class FederatedRoutingManager implements RoutingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(FederatedRoutingManager.class);

  private final BrokerRoutingManager _primaryRoutingManager;
  private final List<BrokerRoutingManager> _secondaryRoutingManagers;

  public FederatedRoutingManager(BrokerRoutingManager primaryRoutingManager,
      List<BrokerRoutingManager> secondaryRoutingManagers) {
    _primaryRoutingManager = primaryRoutingManager;
    _secondaryRoutingManagers = secondaryRoutingManagers;
  }

  @Nullable
  private <T> T findFirst(Function<BrokerRoutingManager, T> getter, String tableNameForLog) {
    T result = getter.apply(_primaryRoutingManager);
    if (result != null) {
      return result;
    }
    for (BrokerRoutingManager secondary : _secondaryRoutingManagers) {
      try {
        result = getter.apply(secondary);
        if (result != null) {
          return result;
        }
      } catch (Exception e) {
        LOGGER.error("Error querying secondary routing manager for table {}", tableNameForLog, e);
      }
    }
    return null;
  }

  private boolean anyMatch(Predicate<BrokerRoutingManager> predicate) {
    if (predicate.test(_primaryRoutingManager)) {
      return true;
    }
    for (BrokerRoutingManager secondary : _secondaryRoutingManagers) {
      if (predicate.test(secondary)) {
        return true;
      }
    }
    return false;
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
    RoutingTable primaryTable = _primaryRoutingManager.getRoutingTable(brokerRequest, tableNameWithType, requestId);
    return combineRoutingTables(primaryTable, tableNameWithType, brokerRequest, requestId);
  }

  private RoutingTable combineRoutingTables(@Nullable RoutingTable primaryTable, String tableNameWithType,
      BrokerRequest brokerRequest, long requestId) {
    Map<ServerInstance, SegmentsToQuery> combinedMap = primaryTable != null
        ? new HashMap<>(primaryTable.getServerInstanceToSegmentsMap()) : new HashMap<>();
    List<String> unavailableSegments = primaryTable != null
        ? new ArrayList<>(primaryTable.getUnavailableSegments()) : new ArrayList<>();
    int prunedCount = primaryTable != null ? primaryTable.getNumPrunedSegments() : 0;

    for (BrokerRoutingManager secondary : _secondaryRoutingManagers) {
      try {
        RoutingTable secondaryTable = secondary.getRoutingTable(brokerRequest, tableNameWithType, requestId);
        if (secondaryTable != null) {
          mergeRoutingTable(combinedMap, secondaryTable);
          unavailableSegments.addAll(secondaryTable.getUnavailableSegments());
          prunedCount += secondaryTable.getNumPrunedSegments();
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
    Map<String, ServerInstance> combined = new HashMap<>(_primaryRoutingManager.getEnabledServerInstanceMap());
    for (BrokerRoutingManager secondary : _secondaryRoutingManagers) {
      combined.putAll(secondary.getEnabledServerInstanceMap());
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
    Set<String> primary = _primaryRoutingManager.getServingInstances(tableNameWithType);
    if (primary != null) {
      combined.addAll(primary);
    }
    for (BrokerRoutingManager secondary : _secondaryRoutingManagers) {
      try {
        Set<String> instances = secondary.getServingInstances(tableNameWithType);
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
    List<String> combined = new ArrayList<>(_primaryRoutingManager.getSegments(brokerRequest));
    for (BrokerRoutingManager secondary : _secondaryRoutingManagers) {
      try {
        combined.addAll(secondary.getSegments(brokerRequest));
      } catch (Exception e) {
        LOGGER.error("Error getting segments from secondary routing manager", e);
      }
    }
    return combined;
  }

  @Override
  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
    return findFirst(mgr -> mgr.getTablePartitionReplicatedServersInfo(tableNameWithType), tableNameWithType);
  }
}
