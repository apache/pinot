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
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.PhysicalTableRoute;
import org.apache.pinot.core.transport.Route;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;


public class HybridTableRoute implements Route {
  private final PhysicalTableRoute _offlineTableRoute;
  private final PhysicalTableRoute _realtimeTableRoute;

  public static HybridTableRoute from(HybridTable hybridTable, RoutingManager routingManager,
      BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest, long requestId) {
    PhysicalTableRoute offlineTableRoute = null;
    PhysicalTableRoute realtimeTableRoute = null;

    if (offlineBrokerRequest != null) {
      PhysicalTable physicalTable = hybridTable.getOfflineTable();
      Preconditions.checkNotNull(physicalTable);
      offlineTableRoute =
          PhysicalTableRoute.from(physicalTable.getTableNameWithType(), routingManager, offlineBrokerRequest,
              requestId);
    }
    if (realtimeBrokerRequest != null) {
      PhysicalTable physicalTable = hybridTable.getRealtimeTable();
      Preconditions.checkNotNull(physicalTable);
      realtimeTableRoute =
          PhysicalTableRoute.from(physicalTable.getTableNameWithType(), routingManager, realtimeBrokerRequest,
              requestId);
    }

    return new HybridTableRoute(offlineTableRoute, realtimeTableRoute);
  }

  private HybridTableRoute(PhysicalTableRoute offlineTableRoute, PhysicalTableRoute realtimeTableRoute) {
    _offlineTableRoute = offlineTableRoute;
    _realtimeTableRoute = realtimeTableRoute;
  }

  @Override
  public PhysicalTableRoute getOfflineTableRoute() {
    return _offlineTableRoute;
  }

  @Override
  public PhysicalTableRoute getRealtimeTableRoute() {
    return _realtimeTableRoute;
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
    List<String> unavailableSegments = Collections.emptyList();
    if (_offlineTableRoute != null && !_offlineTableRoute.getUnavailableSegments().isEmpty()) {
      unavailableSegments = _offlineTableRoute.getUnavailableSegments();
    }

    if (_realtimeTableRoute != null && !_realtimeTableRoute.getUnavailableSegments().isEmpty()) {
      if (unavailableSegments.isEmpty()) {
        unavailableSegments = _realtimeTableRoute.getUnavailableSegments();
      } else {
        unavailableSegments =
            new ArrayList<>(unavailableSegments.size() + _realtimeTableRoute.getUnavailableSegments().size());
        unavailableSegments.addAll(_offlineTableRoute.getUnavailableSegments());
        unavailableSegments.addAll(_realtimeTableRoute.getUnavailableSegments());
      }
    }

    return unavailableSegments;
  }

  @Override
  public int getNumPrunedSegments() {
    int numPrunedSegments = 0;
    if (_offlineTableRoute != null) {
      numPrunedSegments += _offlineTableRoute.getNumPrunedSegments();
    }
    if (_realtimeTableRoute != null) {
      numPrunedSegments += _realtimeTableRoute.getNumPrunedSegments();
    }
    return numPrunedSegments;
  }

  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = null;
    if (_offlineTableRoute != null) {
      requestMap = _offlineTableRoute.getRequestMap(requestId, brokerId, preferTls);
    }
    if (_realtimeTableRoute != null) {
      Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap =
          _realtimeTableRoute.getRequestMap(requestId, brokerId, preferTls);
      if (requestMap == null) {
        requestMap = realtimeRequestMap;
      } else {
        requestMap.putAll(realtimeRequestMap);
      }
    }

    return requestMap;
  }

  @Override
  public boolean isEmpty() {
    return _offlineTableRoute == null && _realtimeTableRoute == null;
  }
}
