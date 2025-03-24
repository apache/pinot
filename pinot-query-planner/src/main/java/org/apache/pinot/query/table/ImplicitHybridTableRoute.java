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
package org.apache.pinot.query.table;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.Route;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class ImplicitHybridTableRoute implements Route {
  private final PhysicalTableRoute _offlineTableRoute;
  private final PhysicalTableRoute _realtimeTableRoute;
  private final List<String> _unavailableSegments;
  private final int _numPrunedSegmentsTotal;

  public static ImplicitHybridTableRoute from(HybridTable hybridTable, RoutingManager routingManager,
      BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest, long requestId) {
    PhysicalTableRoute offlineTableRoute = null;
    PhysicalTableRoute realtimeTableRoute = null;
    int numPrunedSegmentsTotal = 0;
    List<String> unavailableSegments = new ArrayList<>();

    if (offlineBrokerRequest != null) {
      PhysicalTable physicalTable = hybridTable.getOfflineTable();
      Preconditions.checkNotNull(physicalTable);

      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!physicalTable.isDisabled()) {
        routingTable = routingManager.getRoutingTable(offlineBrokerRequest, requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineTableRoute = new PhysicalTableRoute(serverInstanceToSegmentsMap, physicalTable.getTableNameWithType(),
              offlineBrokerRequest);
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      }
    }
    if (realtimeBrokerRequest != null) {
      PhysicalTable physicalTable = hybridTable.getOfflineTable();
      Preconditions.checkNotNull(physicalTable);

      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!physicalTable.isDisabled()) {
        routingTable = routingManager.getRoutingTable(realtimeBrokerRequest, requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeTableRoute = new PhysicalTableRoute(serverInstanceToSegmentsMap, physicalTable.getTableNameWithType(),
              realtimeBrokerRequest);
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      }
    }

    return new ImplicitHybridTableRoute(offlineTableRoute, realtimeTableRoute, unavailableSegments, numPrunedSegmentsTotal);
  }

  public static ImplicitHybridTableRoute from(String rawTableName,
      @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable) {
    PhysicalTableRoute offlineTableRoute = null;
    if (offlineBrokerRequest != null && offlineRoutingTable != null) {
      offlineTableRoute = new PhysicalTableRoute(offlineRoutingTable, TableNameBuilder.OFFLINE.tableNameWithType(rawTableName), offlineBrokerRequest);
    }

    PhysicalTableRoute realtimeTableRoute = null;
    if (realtimeBrokerRequest != null && realtimeRoutingTable != null) {
      realtimeTableRoute = new PhysicalTableRoute(realtimeRoutingTable, TableNameBuilder.REALTIME.tableNameWithType(rawTableName), realtimeBrokerRequest);
    }

    return new ImplicitHybridTableRoute(offlineTableRoute, realtimeTableRoute, Collections.emptyList(), 0);
  }

  private ImplicitHybridTableRoute(PhysicalTableRoute offlineTableRoute, PhysicalTableRoute realtimeTableRoute,
      List<String> unavailableSegments, int numPrunedSegmentsTotal) {
    _offlineTableRoute = offlineTableRoute;
    _realtimeTableRoute = realtimeTableRoute;
    _unavailableSegments = unavailableSegments;
    _numPrunedSegmentsTotal = numPrunedSegmentsTotal;
  }

  @Nullable
  @Override
  public BrokerRequest getOfflineBrokerRequest() {
    return _offlineTableRoute != null ? _offlineTableRoute.getBrokerRequest() : null;
  }

  @Nullable
  @Override
  public BrokerRequest getRealtimeBrokerRequest() {
    return _realtimeTableRoute != null ? _realtimeTableRoute.getBrokerRequest() : null;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable() {
    return _offlineTableRoute != null ? _offlineTableRoute.getRoutingTable() : null;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return _realtimeTableRoute != null ? _realtimeTableRoute.getRoutingTable() : null;
  }

  @Override
  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  @Override
  public int getNumPrunedSegmentsTotal() {
    return _numPrunedSegmentsTotal;
  }

  @Override
  public boolean isEmpty() {
    return _offlineTableRoute == null && _realtimeTableRoute == null;
  }

  @Nullable
  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getOfflineRequestMap(long requestId, String brokerId,
      boolean preferTls) {
    return _offlineTableRoute != null ? _offlineTableRoute.getRequestMap(requestId, brokerId, preferTls) : null;
  }

  @Nullable
  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRealtimeRequestMap(long requestId, String brokerId,
      boolean preferTls) {
    return _realtimeTableRoute != null ? _realtimeTableRoute.getRequestMap(requestId, brokerId, preferTls) : null;
  }
}
