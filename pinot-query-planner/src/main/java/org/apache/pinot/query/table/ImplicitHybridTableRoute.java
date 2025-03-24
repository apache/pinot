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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.AbstractRoute;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;


public class ImplicitHybridTableRoute extends AbstractRoute {
  private final BrokerRequest _offlineBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
  private final BrokerRequest _realtimeBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;
  private final List<String> _unavailableSegments;
  private final int _numPrunedSegmentsTotal;

  public static ImplicitHybridTableRoute from(HybridTable hybridTable, RoutingManager routingManager,
      BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest, long requestId) {
    Map<ServerInstance, ServerRouteInfo> offlineTableRoute = null;
    Map<ServerInstance, ServerRouteInfo> realtimeTableRoute = null;

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
          offlineTableRoute = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      PhysicalTable physicalTable = hybridTable.getRealtimeTable();
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
          realtimeTableRoute = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        realtimeBrokerRequest = null;
      }
    }

    return new ImplicitHybridTableRoute(offlineBrokerRequest, offlineTableRoute, realtimeBrokerRequest,
        realtimeTableRoute, unavailableSegments, numPrunedSegmentsTotal);
  }

  private ImplicitHybridTableRoute(BrokerRequest offlineBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> offlineRoutingTable, BrokerRequest realtimeBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, List<String> unavailableSegments,
      int numPrunedSegmentsTotal) {
    _offlineBrokerRequest = offlineBrokerRequest;
    _offlineRoutingTable = offlineRoutingTable;
    _realtimeBrokerRequest = realtimeBrokerRequest;
    _realtimeRoutingTable = realtimeRoutingTable;
    _unavailableSegments = unavailableSegments;
    _numPrunedSegmentsTotal = numPrunedSegmentsTotal;
  }

  @Nullable
  @Override
  public BrokerRequest getOfflineBrokerRequest() {
    return _offlineBrokerRequest;
  }

  @Nullable
  @Override
  public BrokerRequest getRealtimeBrokerRequest() {
    return _realtimeBrokerRequest;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable() {
    return _offlineRoutingTable;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return _realtimeRoutingTable;
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
    return _offlineRoutingTable == null && _realtimeRoutingTable == null;
  }

  @Nullable
  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getOfflineRequestMap(long requestId, String brokerId,
      boolean preferTls) {
    if (_offlineRoutingTable != null && _offlineBrokerRequest != null) {
      return getRequestMapFromRoutingTable(_offlineRoutingTable, _offlineBrokerRequest, requestId, brokerId, preferTls);
    }
    return null;
  }

  @Nullable
  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRealtimeRequestMap(long requestId, String brokerId,
      boolean preferTls) {
    if (_realtimeRoutingTable != null && _realtimeBrokerRequest != null) {
      return getRequestMapFromRoutingTable(_realtimeRoutingTable, _realtimeBrokerRequest, requestId, brokerId,
          preferTls);
    }
    return null;
  }
}
