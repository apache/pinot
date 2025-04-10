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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.TableRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.query.planner.physical.table.PhysicalTableRoute;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;


public class LogicalQueryRouteInfo implements org.apache.pinot.core.transport.TableRouteInfo {
  private final BrokerRequest _offlineBrokerRequest;
  private final BrokerRequest _realtimeBrokerRequest;
  private final List<PhysicalTableRoute> _offlineTableRoutes;
  private final List<PhysicalTableRoute> _realtimeTableRoutes;

  public LogicalQueryRouteInfo(BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest,
      List<PhysicalTableRoute> offlineTableRoutes, List<PhysicalTableRoute> realtimeTableRoutes) {
    _offlineBrokerRequest = offlineBrokerRequest;
    _realtimeBrokerRequest = realtimeBrokerRequest;
    _offlineTableRoutes = offlineTableRoutes;
    _realtimeTableRoutes = realtimeTableRoutes;
  }

  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls) {
    Map<ServerInstance, List<TableRouteInfo>> offlineTableRouteInfo = new HashMap<>();
    Map<ServerInstance, List<TableRouteInfo>> realtimeTableRouteInfo = new HashMap<>();

    for (PhysicalTableRoute physicalTableRoute : _offlineTableRoutes) {
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : physicalTableRoute.getServerRouteInfoMap().entrySet()) {
        TableRouteInfo tableRouteInfo = new TableRouteInfo();
        tableRouteInfo.setTableName(physicalTableRoute.getTableName());
        tableRouteInfo.setSegments(entry.getValue().getSegments());
        if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
          tableRouteInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
        }

        offlineTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableRouteInfo);
      }
    }

    for (PhysicalTableRoute physicalTableRoute : _realtimeTableRoutes) {
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : physicalTableRoute.getServerRouteInfoMap().entrySet()) {
        TableRouteInfo tableRouteInfo = new TableRouteInfo();
        tableRouteInfo.setTableName(physicalTableRoute.getTableName());
        tableRouteInfo.setSegments(entry.getValue().getSegments());
        if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
          tableRouteInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
        }

        realtimeTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableRouteInfo);
      }
    }
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();

    for (Map.Entry<ServerInstance, List<TableRouteInfo>> entry : offlineTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.OFFLINE),
          getInstanceRequest(requestId, brokerId, _offlineBrokerRequest, entry.getValue()));
    }

    for (Map.Entry<ServerInstance, List<TableRouteInfo>> entry : realtimeTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.REALTIME),
          getInstanceRequest(requestId, brokerId, _realtimeBrokerRequest, entry.getValue()));
    }

    return requestMap;
  }

  private InstanceRequest getInstanceRequest(long requestId, String brokerId, BrokerRequest brokerRequest, List<TableRouteInfo> tableRouteInfo) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setCid(QueryThreadContext.getCid());
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setLogicalTableRouteInfo(tableRouteInfo);
    instanceRequest.setBrokerId(brokerId);
    return instanceRequest;
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
    return Map.of();
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return Map.of();
  }
}
