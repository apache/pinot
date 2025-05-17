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
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.query.timeboundary.TimeBoundaryStrategy;
import org.apache.pinot.query.timeboundary.TimeBoundaryStrategyService;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogicalTableRouteProvider implements TableRouteProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableRouteProvider.class);

  @Override
  public TableRouteInfo getTableRouteInfo(String tableName, TableCache tableCache, RoutingManager routingManager) {
    LogicalTableConfig logicalTable = tableCache.getLogicalTableConfig(tableName);
    if (logicalTable == null) {
      return new LogicalTableRouteInfo();
    }

    PhysicalTableRouteProvider routeProvider = new PhysicalTableRouteProvider();

    List<TableRouteInfo> offlineTables = new ArrayList<>();
    List<TableRouteInfo> realtimeTables = new ArrayList<>();
    for (String physicalTableName : logicalTable.getPhysicalTableConfigMap().keySet()) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(physicalTableName);
      Preconditions.checkNotNull(tableType);
      TableRouteInfo physicalTableInfo =
          routeProvider.getTableRouteInfo(physicalTableName, tableCache, routingManager);
      if (physicalTableInfo.isExists()) {
        if (tableType == TableType.OFFLINE) {
          offlineTables.add(physicalTableInfo);
        } else {
          realtimeTables.add(physicalTableInfo);
        }
      }
    }

    LogicalTableRouteInfo routeInfo = new LogicalTableRouteInfo(logicalTable);
    if (!offlineTables.isEmpty()) {
      TableConfig offlineTableConfig = tableCache.getTableConfig(logicalTable.getRefOfflineTableName());
      Preconditions.checkNotNull(offlineTableConfig,
          "Offline table config not found: " + logicalTable.getRefOfflineTableName());
      routeInfo.setOfflineTables(offlineTables);
      routeInfo.setOfflineTableConfig(offlineTableConfig);
    }
    if (!realtimeTables.isEmpty()) {
      TableConfig realtimeTableConfig = tableCache.getTableConfig(logicalTable.getRefRealtimeTableName());
      Preconditions.checkNotNull(realtimeTableConfig,
          "Realtime table config not found: " + logicalTable.getRefRealtimeTableName());
      routeInfo.setRealtimeTables(realtimeTables);
      routeInfo.setRealtimeTableConfig(realtimeTableConfig);
    }
    routeInfo.setQueryConfig(logicalTable.getQueryConfig());

    TimeBoundaryInfo timeBoundaryInfo;
    if (!offlineTables.isEmpty() && !realtimeTables.isEmpty()) {
      String boundaryStrategy = logicalTable.getTimeBoundaryConfig().getBoundaryStrategy();
      TimeBoundaryStrategy timeBoundaryStrategy =
          TimeBoundaryStrategyService.getInstance().getTimeBoundaryStrategy(boundaryStrategy);
      timeBoundaryInfo = timeBoundaryStrategy.computeTimeBoundary(logicalTable, tableCache, routingManager);
      if (timeBoundaryInfo == null) {
        LOGGER.info("No time boundary info found for logical hybrid table: ");
        routeInfo.setOfflineTables(null);
      } else {
        routeInfo.setTimeBoundaryInfo(timeBoundaryInfo);
      }
    }
    return routeInfo;
  }

  @Override
  public void calculateRoutes(TableRouteInfo tableRouteInfo, RoutingManager routingManager,
      BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest, long requestId) {
    LogicalTableRouteInfo routeInfo = (LogicalTableRouteInfo) tableRouteInfo;
    int numPrunedSegments = 0;
    List<String> unavailableSegments = new ArrayList<>();
    PhysicalTableRouteProvider routeProvider = new PhysicalTableRouteProvider();

    if (routeInfo.getOfflineTables() != null) {
      for (TableRouteInfo physicalTableInfo : routeInfo.getOfflineTables()) {
        routeProvider.calculateRoutes(physicalTableInfo, routingManager, offlineBrokerRequest, null,
            requestId);
        numPrunedSegments += physicalTableInfo.getNumPrunedSegmentsTotal();
        if (physicalTableInfo.getUnavailableSegments() != null) {
          unavailableSegments.addAll(physicalTableInfo.getUnavailableSegments());
        }
      }
    }

    if (routeInfo.getRealtimeTables() != null) {
      for (TableRouteInfo physicalTableInfo : routeInfo.getRealtimeTables()) {
        routeProvider.calculateRoutes(physicalTableInfo, routingManager, null, realtimeBrokerRequest,
            requestId);
        numPrunedSegments += physicalTableInfo.getNumPrunedSegmentsTotal();
        if (physicalTableInfo.getUnavailableSegments() != null) {
          unavailableSegments.addAll(physicalTableInfo.getUnavailableSegments());
        }
      }
    }

    //Set BrokerRequests to NULL if there is no route.
    if (routeInfo.getOfflineExecutionServers().isEmpty()) {
      routeInfo.setOfflineBrokerRequest(null);
    } else {
      routeInfo.setOfflineBrokerRequest(offlineBrokerRequest);
    }

    if (routeInfo.getRealtimeExecutionServers().isEmpty()) {
      routeInfo.setRealtimeBrokerRequest(null);
    } else {
      routeInfo.setRealtimeBrokerRequest(realtimeBrokerRequest);
    }

    routeInfo.setUnavailableSegments(unavailableSegments);
    routeInfo.setNumPrunedSegments(numPrunedSegments);
  }
}
