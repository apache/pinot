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
package org.apache.pinot.query.routing.table;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.auth.request.BrokerRequest;


/**
 * PhysicalTableRouteProvider is used to calculate the routing table for a physical table.
 * It differs from ImplicitHybridTableRouteProvider in that it calls a different RoutingManager function to get the
 * routing table.
 * For Logical Tables, the broker request has the name of the logical table. It is inefficient to create a broker
 * request for every physical table. Therefore the name of the physical table is passed explicitly to the
 * RoutingManager.
 *
 * The relevant call is getRoutingTable(BrokerRequest, String, long)
 */
public class PhysicalTableRouteProvider extends ImplicitHybridTableRouteProvider {
  @Override
  public void calculateRoutes(TableRouteInfo tableRouteInfo, RoutingManager routingManager,
      BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest, long requestId) {
    String offlineTableName = tableRouteInfo.getOfflineTableName();
    String realtimeTableName = tableRouteInfo.getRealtimeTableName();
    Map<ServerInstance, ServerRouteInfo> offlineRoutingTable = null;
    Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable = null;
    List<String> unavailableSegments = new ArrayList<>();
    int numPrunedSegmentsTotal = 0;

    if (offlineBrokerRequest != null) {
      Preconditions.checkNotNull(offlineTableName);

      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!tableRouteInfo.isOfflineTableDisabled()) {
        routingTable =
            routingManager.getRoutingTable(offlineBrokerRequest, tableRouteInfo.getOfflineTableName(), requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      Preconditions.checkNotNull(realtimeTableName);

      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!tableRouteInfo.isRealtimeTableDisabled()) {
        routingTable =
            routingManager.getRoutingTable(realtimeBrokerRequest, tableRouteInfo.getRealtimeTableName(), requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        realtimeBrokerRequest = null;
      }
    }

    ImplicitHybridTableRouteInfo hybridTableRouteInfo = (ImplicitHybridTableRouteInfo) tableRouteInfo;
    hybridTableRouteInfo.setUnavailableSegments(unavailableSegments);
    hybridTableRouteInfo.setNumPrunedSegmentsTotal(numPrunedSegmentsTotal);
    hybridTableRouteInfo.setOfflineBrokerRequest(offlineBrokerRequest);
    hybridTableRouteInfo.setRealtimeBrokerRequest(realtimeBrokerRequest);
    hybridTableRouteInfo.setOfflineRoutingTable(offlineRoutingTable);
    hybridTableRouteInfo.setRealtimeRoutingTable(realtimeRoutingTable);
  }
}
