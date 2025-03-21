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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;


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
  public boolean isEmpty() {
    return _offlineTableRoute == null && _realtimeTableRoute == null;
  }
}
