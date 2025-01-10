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
package org.apache.pinot.query.planner.physical.table;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HybridTableRoute {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridTableRoute.class);

  private final List<PhysicalTableRoute> _offlineTableRoutes;
  private final List<PhysicalTableRoute> _realtimeTableRoutes;
  private final List<PhysicalTable> _tablesWithNoRouting;
  private final TimeBoundaryInfo _timeBoundaryInfo;

  public static HybridTableRoute from(HybridTable hybridTable, RoutingManager routingManager,
      BrokerRequest brokerRequest, long requestId) {
    List<PhysicalTableRoute> offlineTableRoutes = new ArrayList<>();
    List<PhysicalTable> tablesWithNoRouting = new ArrayList<>();
    for (PhysicalTable physicalTable : hybridTable.getOfflineTables()) {
      PhysicalTableRoute route =
          PhysicalTableRoute.from(physicalTable.getTableName(), routingManager, brokerRequest, requestId);
      if (route != null) {
        offlineTableRoutes.add(route);
      } else {
        tablesWithNoRouting.add(physicalTable);
      }
    }

    List<PhysicalTableRoute> realtimeTableRoutes = new ArrayList<>();
    for (PhysicalTable physicalTable : hybridTable.getRealtimeTables()) {
      PhysicalTableRoute route =
          PhysicalTableRoute.from(physicalTable.getTableName(), routingManager, brokerRequest, requestId);
      if (route != null) {
        realtimeTableRoutes.add(route);
      } else {
        tablesWithNoRouting.add(physicalTable);
      }
    }

    TimeBoundaryInfo timeBoundaryInfo = null;
    if (offlineTableRoutes.isEmpty() && realtimeTableRoutes.isEmpty()) {
      timeBoundaryInfo = routingManager.getTimeBoundaryInfo(hybridTable.getOfflineTables().get(0).getTableName());
      if (timeBoundaryInfo == null) {
        LOGGER.debug("No time boundary info found for hybrid table: {}", hybridTable);
        offlineTableRoutes = List.of();
      }
    }
    return new HybridTableRoute(offlineTableRoutes, realtimeTableRoutes, timeBoundaryInfo, tablesWithNoRouting);
  }

  private HybridTableRoute(List<PhysicalTableRoute> offlineTableRoutes, List<PhysicalTableRoute> realtimeTableRoutes,
      TimeBoundaryInfo timeBoundaryInfo, List<PhysicalTable> tablesWithNoRouting) {
    _offlineTableRoutes = offlineTableRoutes;
    _realtimeTableRoutes = realtimeTableRoutes;
    _timeBoundaryInfo = timeBoundaryInfo;
    _tablesWithNoRouting = tablesWithNoRouting;
  }

  public List<PhysicalTableRoute> getOfflineTableRoutes() {
    return _offlineTableRoutes;
  }

  public List<PhysicalTableRoute> getRealtimeTableRoutes() {
    return _realtimeTableRoutes;
  }

  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public List<PhysicalTable> getTablesWithNoRouting() {
    return _tablesWithNoRouting;
  }

  public boolean isEmpty() {
    return _offlineTableRoutes.isEmpty() && _realtimeTableRoutes.isEmpty();
  }
}
