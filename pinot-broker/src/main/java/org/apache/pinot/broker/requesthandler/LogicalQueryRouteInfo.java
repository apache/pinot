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
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.TableRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.QueryRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.query.planner.physical.table.HybridTableRoute;
import org.apache.pinot.query.planner.physical.table.PhysicalTableRoute;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;


public class LogicalQueryRouteInfo extends QueryRouteInfo {
  private final HybridTableRoute _hybridTableRoute;

  public LogicalQueryRouteInfo(String brokerId, long requestId, String rawTableName,
      BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
      HybridTableRoute hybridTableRoute, long timeoutMs, RequestContext requestContext) {
    super(brokerId, requestId, rawTableName, originalBrokerRequest, serverBrokerRequest, rawTableName,
        serverBrokerRequest, null, rawTableName, serverBrokerRequest, null, 0, timeoutMs,
        requestContext);
    _hybridTableRoute = hybridTableRoute;
  }

  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(boolean preferTls) {
    Map<ServerInstance, List<TableRouteInfo>> offlineTableRouteInfo = new HashMap<>();
    Map<ServerInstance, List<TableRouteInfo>> realtimeTableRouteInfo = new HashMap<>();

    for (PhysicalTableRoute physicalTableRoute : _hybridTableRoute.getOfflineTableRoutes()) {
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : physicalTableRoute.getServerRouteInfoMap().entrySet()) {
        TableRouteInfo tableRouteInfo = new TableRouteInfo();
        tableRouteInfo.setTableName(physicalTableRoute.getTableName());
        tableRouteInfo.setSegments(entry.getValue().getSegments());
        if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
          tableRouteInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
        }

        offlineTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableRouteInfo);
      }
      _numPrunedSegments += physicalTableRoute.getNumPrunedSegments();
    }

    for (PhysicalTableRoute physicalTableRoute : _hybridTableRoute.getRealtimeTableRoutes()) {
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : physicalTableRoute.getServerRouteInfoMap().entrySet()) {
        TableRouteInfo tableRouteInfo = new TableRouteInfo();
        tableRouteInfo.setTableName(physicalTableRoute.getTableName());
        tableRouteInfo.setSegments(entry.getValue().getSegments());
        if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
          tableRouteInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
        }

        realtimeTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableRouteInfo);
      }
      _numPrunedSegments += physicalTableRoute.getNumPrunedSegments();
    }
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();

    for (Map.Entry<ServerInstance, List<TableRouteInfo>> entry : offlineTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.OFFLINE),
          getInstanceRequest(_serverBrokerRequest, entry.getValue()));
    }

    for (Map.Entry<ServerInstance, List<TableRouteInfo>> entry : realtimeTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.REALTIME),
          getInstanceRequest(_serverBrokerRequest, entry.getValue()));
    }

    return requestMap;
  }

  private InstanceRequest getInstanceRequest(BrokerRequest brokerRequest, List<TableRouteInfo> tableRouteInfo) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(_requestId);
    instanceRequest.setCid(QueryThreadContext.getCid());
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setLogicalTableRouteInfo(tableRouteInfo);
    instanceRequest.setBrokerId(_brokerId);
    return instanceRequest;
  }
}
