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
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ImplicitHybridTable represents the default definition of a hybrid table - If a table name does not have a type,
 * then it represents a OFFLINE and REALTIME table with the same raw table name.
 * If the table name has a type, then it represents the table with the given type.
 */
public class ImplicitHybridTableRouteProvider implements TableRouteProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImplicitHybridTableRouteProvider.class);

  private String _offlineTableName = null;
  private boolean _isOfflineRouteExists;
  private TableConfig _offlineTableConfig;
  private boolean _isOfflineTableDisabled;

  private String _realtimeTableName = null;
  private boolean _isRealtimeRouteExists;
  private TableConfig _realtimeTableConfig;
  private boolean _isRealtimeTableDisabled;

  private TimeBoundaryInfo _timeBoundaryInfo;

  private final List<String> _unavailableSegments = new ArrayList<>();
  private int _numPrunedSegmentsTotal = 0;

  public static ImplicitHybridTableRouteProvider create(String tableName, TableCache tableCache,
      RoutingManager routingManager) {
    ImplicitHybridTableRouteProvider provider = new ImplicitHybridTableRouteProvider(tableName);
    provider.getTableConfig(tableCache);
    provider.checkRoutes(routingManager);

    return provider;
  }

  public static ImplicitHybridTableRouteProvider create(String tableName, RoutingManager routingManager) {
    ImplicitHybridTableRouteProvider provider = new ImplicitHybridTableRouteProvider(tableName);
    provider.checkRoutes(routingManager);

    return provider;
  }

  private ImplicitHybridTableRouteProvider(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

    if (tableType == TableType.OFFLINE) {
      _offlineTableName = tableName;
    } else if (tableType == TableType.REALTIME) {
      _realtimeTableName = tableName;
    } else {
      _offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      _realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    }
  }

  /*
   *  Table Config Section
   */

  private void getTableConfig(TableCache tableCache) {
    if (_offlineTableName != null) {
      _offlineTableConfig = tableCache.getTableConfig(_offlineTableName);
    }

    if (_realtimeTableName != null) {
      _realtimeTableConfig = tableCache.getTableConfig(_realtimeTableName);
    }
  }

  @Nullable
  @Override
  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }

  @Nullable
  @Override
  public TableConfig getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }

  /**
   * Offline if offline table config is present.
   * @return true if there is an OFFLINE table, false otherwise
   */
  @Override
  public boolean hasOffline() {
    return _offlineTableConfig != null;
  }

  /**
   * Realtime if realtime table config is present.
   * @return true if there is a REALTIME table, false otherwise
   */
  @Override
  public boolean hasRealtime() {
    return _realtimeTableConfig != null;
  }

  /**
   * Hybrid if both offline and realtime table configs are present.
   * @return true if the table is hybrid, false otherwise
   */
  @Override
  public boolean isHybrid() {
    return hasOffline() && hasRealtime();
  }

  /**
   * Offline if offline table config is present and realtime table config is not present.
   * @return true if the table is offline, false otherwise
   */
  @Override
  public boolean isOffline() {
    return hasOffline() && !hasRealtime();
  }

  /**
   * Realtime if realtime table config is present and offline table config is not present.
   * @return true if the table is realtime, false otherwise
   */
  @Override
  public boolean isRealtime() {
    return !hasOffline() && hasRealtime();
  }

  /**
   * Exists if there is at least one table with a TableConfig.
   * @return true if the table exists, false otherwise
   */
  @Override
  public boolean isExists() {
    return hasOffline() || hasRealtime();
  }

  @Nullable
  @Override
  public String getOfflineTableName() {
    return _offlineTableName;
  }

  @Nullable
  @Override
  public String getRealtimeTableName() {
    return _realtimeTableName;
  }

  /*
   *  Check Routes Section
   */

  private void checkRoutes(RoutingManager routingManager) {
    if (hasOffline()) {
      _isOfflineRouteExists = routingManager.routingExists(_offlineTableName);
      _isOfflineTableDisabled = routingManager.isTableDisabled(_offlineTableName);
    }

    if (hasRealtime()) {
      _isRealtimeRouteExists = routingManager.routingExists(_realtimeTableName);
      _isRealtimeTableDisabled = routingManager.isTableDisabled(_realtimeTableName);
    }

    // Get TimeBoundaryInfo. If there is no time boundary, then do not consider the offline table.
    if (isHybrid()) {
      // Time boundary info might be null when there is no segment in the offline table, query real-time side only
      _timeBoundaryInfo = routingManager.getTimeBoundaryInfo(_offlineTableName);
      if (_timeBoundaryInfo == null) {
        LOGGER.debug("No time boundary info found for hybrid table: {}",
            TableNameBuilder.extractRawTableName(_offlineTableName));
        _offlineTableName = null;
        _offlineTableConfig = null;
      }
    }
  }

  /**
   * Route exists if at least one of the physical tables has a route.
   * @return true if a route exists, false otherwise
   */
  @Override
  public boolean isRouteExists() {
    if (isOffline()) {
      return _isOfflineRouteExists;
    } else if (isRealtime()) {
      return _isRealtimeRouteExists;
    } else {
      return _isOfflineRouteExists || _isRealtimeRouteExists;
    }
  }

  public boolean isOfflineRouteExists() {
    return _isOfflineRouteExists;
  }

  public boolean isRealtimeRouteExists() {
    return _isRealtimeRouteExists;
  }

  /**
   * Disabled if all physical tables are disabled.
   * @return true if the table is disabled, false
   */
  @Override
  public boolean isDisabled() {
    if (isOffline()) {
      return _isOfflineTableDisabled;
    } else if (isRealtime()) {
      return _isRealtimeTableDisabled;
    } else {
      return _isOfflineTableDisabled && _isRealtimeTableDisabled;
    }
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public boolean isOfflineTableDisabled() {
    return _isOfflineTableDisabled;
  }

  public boolean isRealtimeTableDisabled() {
    return _isRealtimeTableDisabled;
  }

  @Nullable
  @Override
  public List<String> getDisabledTableNames() {
    if (isOffline() && isOfflineTableDisabled()) {
      return List.of(_offlineTableName);
    } else if (isRealtime() && isRealtimeTableDisabled()) {
      return List.of(_realtimeTableName);
    } else if (isOfflineTableDisabled() && isRealtimeTableDisabled()) {
      return List.of(_offlineTableName, _realtimeTableName);
    } else if (isOfflineTableDisabled()) {
      return List.of(_offlineTableName);
    } else if (isRealtimeTableDisabled()) {
      return List.of(_realtimeTableName);
    }
    return null;
  }

  /*
   *  Calculate Routes Section
   */

  @Override
  public TableRouteInfo calculateRoutes(RoutingManager routingManager, long requestId) {
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;

    if (_offlineTableName != null) {
      offlineBrokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + _offlineTableName + "\"");
    }

    if (_realtimeTableName != null) {
      realtimeBrokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + _realtimeTableName + "\"");
    }

    return calculateRoutes(routingManager, offlineBrokerRequest, realtimeBrokerRequest, requestId);
  }

  @Override
  public TableRouteInfo calculateRoutes(RoutingManager routingManager, BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, long requestId) {
    assert (isExists());
    Map<ServerInstance, ServerRouteInfo> offlineRoutingTable = null;
    Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable = null;

    if (offlineBrokerRequest != null) {
      Preconditions.checkNotNull(_offlineTableName);

      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!_isOfflineTableDisabled) {
        routingTable = routingManager.getRoutingTable(offlineBrokerRequest, requestId);
      }
      if (routingTable != null) {
        _unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
        _numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      Preconditions.checkNotNull(_realtimeTableName);

      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!_isRealtimeTableDisabled) {
        routingTable = routingManager.getRoutingTable(realtimeBrokerRequest, requestId);
      }
      if (routingTable != null) {
        _unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
        _numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        realtimeBrokerRequest = null;
      }
    }

    return new ImplicitHybridTableRouteInfo(offlineBrokerRequest, realtimeBrokerRequest, offlineRoutingTable,
        realtimeRoutingTable);
  }

  @Override
  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  @Override
  public int getNumPrunedSegmentsTotal() {
    return _numPrunedSegmentsTotal;
  }
}
