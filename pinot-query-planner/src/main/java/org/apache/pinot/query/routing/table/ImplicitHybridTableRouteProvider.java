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
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ImplicitHybridTable represents the default definition of a hybrid table - If a table name does not have a type,
 * then it represents a OFFLINE and REALTIME table with the same raw table name.
 * If the table name has a type, then it represents the table with the given type.
 */
public class ImplicitHybridTableRouteProvider implements TableRouteProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImplicitHybridTableRouteProvider.class);

  @Override
  public TableRouteInfo getTableRouteInfo(String tableName, TableCache tableCache,
      RoutingManager routingManager) {
    ImplicitHybridTableRouteInfo tableRouteInfo = new ImplicitHybridTableRouteInfo();

    fillTableConfigMetadata(tableRouteInfo, tableName, tableCache);
    fillRouteMetadata(tableRouteInfo, routingManager);

    return tableRouteInfo;
  }

  public void fillTableConfigMetadata(ImplicitHybridTableRouteInfo tableRouteInfo,
      String tableName, TableCache tableCache) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

    if (tableType == TableType.OFFLINE) {
      tableRouteInfo.setOfflineTableName(tableName);
    } else if (tableType == TableType.REALTIME) {
      tableRouteInfo.setRealtimeTableName(tableName);
    } else {
      tableRouteInfo.setOfflineTableName(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      tableRouteInfo.setRealtimeTableName(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    }

    String offlineTableName = tableRouteInfo.getOfflineTableName();
    String realtimeTableName = tableRouteInfo.getRealtimeTableName();

    if (offlineTableName != null) {
      tableRouteInfo.setOfflineTableConfig(tableCache.getTableConfig(offlineTableName));
    }

    if (realtimeTableName != null) {
      tableRouteInfo.setRealtimeTableConfig(tableCache.getTableConfig(realtimeTableName));
    }
  }

  public void fillRouteMetadata(ImplicitHybridTableRouteInfo tableRouteInfo, RoutingManager routingManager) {
    if (tableRouteInfo.hasOffline()) {
      String offlineTableName = tableRouteInfo.getOfflineTableName();
      tableRouteInfo.setOfflineRouteExists(routingManager.routingExists(offlineTableName));
      tableRouteInfo.setOfflineTableDisabled(routingManager.isTableDisabled(offlineTableName));
    }

    if (tableRouteInfo.hasRealtime()) {
      String realtimeTableName = tableRouteInfo.getRealtimeTableName();
      tableRouteInfo.setRealtimeRouteExists(routingManager.routingExists(realtimeTableName));
      tableRouteInfo.setRealtimeTableDisabled(routingManager.isTableDisabled(realtimeTableName));
    }

    // Get TimeBoundaryInfo. If there is no time boundary, then do not consider the offline table.
    if (tableRouteInfo.isHybrid()) {
      String offlineTableName = tableRouteInfo.getOfflineTableName();
      // Time boundary info might be null when there is no segment in the offline table, query real-time side only
      TimeBoundaryInfo timeBoundaryInfo = routingManager.getTimeBoundaryInfo(offlineTableName);
      if (timeBoundaryInfo == null) {
        LOGGER.debug("No time boundary info found for hybrid table: {}",
            TableNameBuilder.extractRawTableName(offlineTableName));
        tableRouteInfo.setOfflineTableName(null);
        tableRouteInfo.setOfflineTableConfig(null);
      } else {
        tableRouteInfo.setTimeBoundaryInfo(timeBoundaryInfo);
      }
    }
  }

  @Override
  public void calculateRoutes(TableRouteInfo tableRouteInfo, RoutingManager routingManager,
      BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, long requestId) {
    assert (tableRouteInfo.isExists());
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
        routingTable = routingManager.getRoutingTable(offlineBrokerRequest, requestId);
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
        routingTable = routingManager.getRoutingTable(realtimeBrokerRequest, requestId);
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
