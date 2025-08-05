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
package org.apache.pinot.core.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.TableSegmentsInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryStrategy;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTableRouteInfo implements TableRouteInfo {
  private String _logicalTableName;
  private List<ImplicitHybridTableRouteInfo> _offlineTables;
  private List<ImplicitHybridTableRouteInfo> _realtimeTables;
  private TableConfig _offlineTableConfig;
  private TableConfig _realtimeTableConfig;
  private QueryConfig _queryConfig;
  private List<String> _unavailableSegments;
  private int _numPrunedSegments = 0;

  private BrokerRequest _offlineBrokerRequest;
  private BrokerRequest _realtimeBrokerRequest;

  private TimeBoundaryStrategy _timeBoundaryStrategy;
  private TimeBoundaryInfo _timeBoundaryInfo;

  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls) {
    Map<ServerInstance, List<TableSegmentsInfo>> offlineTableRouteInfo = new HashMap<>();
    Map<ServerInstance, List<TableSegmentsInfo>> realtimeTableRouteInfo = new HashMap<>();

    if (_offlineTables != null) {
      for (TableRouteInfo physicalTableRoute : _offlineTables) {
        if (physicalTableRoute.getOfflineRoutingTable() != null) {
          for (Map.Entry<ServerInstance, SegmentsToQuery> entry : physicalTableRoute.getOfflineRoutingTable()
              .entrySet()) {
            TableSegmentsInfo tableSegmentsInfo = new TableSegmentsInfo();
            tableSegmentsInfo.setTableName(physicalTableRoute.getOfflineTableName());
            tableSegmentsInfo.setSegments(entry.getValue().getSegments());
            if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
              tableSegmentsInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
            }

            offlineTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableSegmentsInfo);
          }
        }
      }
    }

    if (_realtimeTables != null) {
      for (TableRouteInfo physicalTableRoute : _realtimeTables) {
        if (physicalTableRoute.getRealtimeRoutingTable() != null) {
          for (Map.Entry<ServerInstance, SegmentsToQuery> entry : physicalTableRoute.getRealtimeRoutingTable()
              .entrySet()) {
            TableSegmentsInfo tableSegmentsInfo = new TableSegmentsInfo();
            tableSegmentsInfo.setTableName(physicalTableRoute.getRealtimeTableName());
            tableSegmentsInfo.setSegments(entry.getValue().getSegments());
            if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
              tableSegmentsInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
            }

            realtimeTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableSegmentsInfo);
          }
        }
      }
    }

    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();

    for (Map.Entry<ServerInstance, List<TableSegmentsInfo>> entry : offlineTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.OFFLINE),
          getInstanceRequest(requestId, brokerId, _offlineBrokerRequest, entry.getValue()));
    }

    for (Map.Entry<ServerInstance, List<TableSegmentsInfo>> entry : realtimeTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.REALTIME),
          getInstanceRequest(requestId, brokerId, _realtimeBrokerRequest, entry.getValue()));
    }

    return requestMap;
  }

  private InstanceRequest getInstanceRequest(long requestId, String brokerId, BrokerRequest brokerRequest,
      List<TableSegmentsInfo> tableSegmentsInfoList) {
    InstanceRequest instanceRequest = TableRouteInfo.createInstanceRequest(brokerRequest, brokerId, requestId);
    instanceRequest.setTableSegmentsInfoList(tableSegmentsInfoList);
    return instanceRequest;
  }

  public void setLogicalTableName(String logicalTableName) {
    _logicalTableName = logicalTableName;
  }

  @Nullable
  public String getLogicalTableName() {
    return _logicalTableName;
  }

  @Nullable
  @Override
  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }

  public void setOfflineTableConfig(TableConfig offlineTableConfig) {
    _offlineTableConfig = offlineTableConfig;
  }

  @Nullable
  @Override
  public TableConfig getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }

  public void setRealtimeTableConfig(TableConfig realtimeTableConfig) {
    _realtimeTableConfig = realtimeTableConfig;
  }

  @Nullable
  @Override
  public QueryConfig getOfflineTableQueryConfig() {
    return _queryConfig;
  }

  @Nullable
  @Override
  public QueryConfig getRealtimeTableQueryConfig() {
    return _queryConfig;
  }

  public void setQueryConfig(QueryConfig queryConfig) {
    _queryConfig = queryConfig;
  }

  @Override
  public Set<ServerInstance> getOfflineExecutionServers() {
    if (hasOffline()) {
      Set<ServerInstance> offlineExecutionServers = new HashSet<>();
      for (TableRouteInfo offlineTable : _offlineTables) {
        if (offlineTable.isOfflineRouteExists()) {
          Map<ServerInstance, SegmentsToQuery> offlineRoutingTable = offlineTable.getOfflineRoutingTable();
          if (offlineRoutingTable != null) {
            offlineExecutionServers.addAll(offlineRoutingTable.keySet());
          }
        }
      }
      return offlineExecutionServers;
    }
    return Set.of();
  }

  @Override
  public Set<ServerInstance> getRealtimeExecutionServers() {
    if (hasRealtime()) {
      Set<ServerInstance> realtimeExecutionServers = new HashSet<>();
      for (TableRouteInfo realtimeTable : _realtimeTables) {
        if (realtimeTable.isRealtimeRouteExists()) {
          Map<ServerInstance, SegmentsToQuery> realtimeRoutingTable = realtimeTable.getRealtimeRoutingTable();
          if (realtimeRoutingTable != null) {
            realtimeExecutionServers.addAll(realtimeRoutingTable.keySet());
          }
        }
      }
      return realtimeExecutionServers;
    }
    return Set.of();
  }

  @Nullable
  @Override
  public Map<ServerInstance, SegmentsToQuery> getOfflineRoutingTable() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Map<ServerInstance, SegmentsToQuery> getRealtimeRoutingTable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasOffline() {
    return _offlineTables != null && !_offlineTables.isEmpty();
  }

  @Override
  public boolean hasRealtime() {
    return _realtimeTables != null && !_realtimeTables.isEmpty();
  }

  @Nullable
  @Override
  public String getOfflineTableName() {
    return hasOffline() && _logicalTableName != null ? TableNameBuilder.OFFLINE.tableNameWithType(_logicalTableName)
        : null;
  }

  @Nullable
  @Override
  public String getRealtimeTableName() {
    return hasRealtime() && _logicalTableName != null ? TableNameBuilder.REALTIME.tableNameWithType(_logicalTableName)
        : null;
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

  @Override
  public boolean isOfflineRouteExists() {
    if (_offlineTables != null) {
      for (TableRouteInfo offlineTable : _offlineTables) {
        if (offlineTable.isRouteExists()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isRealtimeRouteExists() {
    if (_realtimeTables != null) {
      for (TableRouteInfo realtimeTable : _realtimeTables) {
        if (realtimeTable.isRouteExists()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isOfflineTableDisabled() {
    if (_offlineTables != null) {
      for (TableRouteInfo offlineTable : _offlineTables) {
        if (!offlineTable.isDisabled()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean isRealtimeTableDisabled() {
    if (_realtimeTables != null) {
      for (TableRouteInfo realtimeTable : _realtimeTables) {
        if (!realtimeTable.isDisabled()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Nullable
  @Override
  public List<String> getDisabledTableNames() {
    List<String> disabledTableNames = null;
    if (_offlineTables != null) {
      for (TableRouteInfo offlineTable : _offlineTables) {
        if (offlineTable.isDisabled()) {
          if (disabledTableNames == null) {
            disabledTableNames = new ArrayList<>();
          }
          disabledTableNames.add(offlineTable.getOfflineTableName());
        }
      }
    }

    if (_realtimeTables != null) {
      for (TableRouteInfo realtimeTable : _realtimeTables) {
        if (realtimeTable.isDisabled()) {
          if (disabledTableNames == null) {
            disabledTableNames = new ArrayList<>();
          }
          disabledTableNames.add(realtimeTable.getRealtimeTableName());
        }
      }
    }

    return disabledTableNames;
  }

  // TODO: https://github.com/apache/pinot/issues/15640
  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public void setTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  @Override
  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  @Override
  public int getNumPrunedSegmentsTotal() {
    return _numPrunedSegments;
  }

  @Nullable
  public List<ImplicitHybridTableRouteInfo> getOfflineTables() {
    return _offlineTables;
  }

  public void setOfflineTables(List<ImplicitHybridTableRouteInfo> offlineTables) {
    _offlineTables = offlineTables;
  }

  @Nullable
  public List<ImplicitHybridTableRouteInfo> getRealtimeTables() {
    return _realtimeTables;
  }

  public void setRealtimeTables(List<ImplicitHybridTableRouteInfo> realtimeTables) {
    _realtimeTables = realtimeTables;
  }

  public void setUnavailableSegments(List<String> unavailableSegments) {
    _unavailableSegments = unavailableSegments;
  }

  public void setNumPrunedSegments(int numPrunedSegments) {
    _numPrunedSegments = numPrunedSegments;
  }

  public void setOfflineBrokerRequest(BrokerRequest offlineBrokerRequest) {
    _offlineBrokerRequest = offlineBrokerRequest;
  }

  public void setRealtimeBrokerRequest(BrokerRequest realtimeBrokerRequest) {
    _realtimeBrokerRequest = realtimeBrokerRequest;
  }

  @Nullable
  public TimeBoundaryStrategy getTimeBoundaryStrategy() {
    return _timeBoundaryStrategy;
  }

  public void setTimeBoundaryStrategy(TimeBoundaryStrategy timeBoundaryStrategy) {
    _timeBoundaryStrategy = timeBoundaryStrategy;
  }
}
