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
package org.apache.pinot.core.transport;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;


public class ImplicitHybridTableRouteInfo implements TableRouteInfo {
  private final BrokerRequest _offlineBrokerRequest;
  private final BrokerRequest _realtimeBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
  private final Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;

  public ImplicitHybridTableRouteInfo(BrokerRequest offlineBrokerRequest, BrokerRequest realtimeBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable) {
    _offlineBrokerRequest = offlineBrokerRequest;
    _realtimeBrokerRequest = realtimeBrokerRequest;
    _offlineRoutingTable = offlineRoutingTable;
    _realtimeRoutingTable = realtimeRoutingTable;
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
    return _offlineRoutingTable;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return _realtimeRoutingTable;
  }

  protected static Map<ServerRoutingInstance, InstanceRequest> getRequestMapFromRoutingTable(TableType tableType,
      Map<ServerInstance, ServerRouteInfo> routingTable, BrokerRequest brokerRequest, long requestId, String brokerId,
      boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();
    for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routingTable.entrySet()) {
      ServerRoutingInstance serverRoutingInstance = entry.getKey().toServerRoutingInstance(tableType, preferTls);
      InstanceRequest instanceRequest = getInstanceRequest(requestId, brokerId, brokerRequest, entry.getValue());
      requestMap.put(serverRoutingInstance, instanceRequest);
    }
    return requestMap;
  }

  protected static InstanceRequest getInstanceRequest(long requestId, String brokerId, BrokerRequest brokerRequest,
      ServerRouteInfo segments) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setCid(QueryThreadContext.getCid());
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setSearchSegments(segments.getSegments());
    instanceRequest.setBrokerId(brokerId);
    if (CollectionUtils.isNotEmpty(segments.getOptionalSegments())) {
      // Don't set this field, i.e. leave it as null, if there is no optional segment at all, to be more backward
      // compatible, as there are places like in multi-stage query engine where this field is not set today when
      // creating the InstanceRequest.
      instanceRequest.setOptionalSegments(segments.getOptionalSegments());
    }
    return instanceRequest;
  }

  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> offlineRequestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap = null;

    if (_offlineRoutingTable != null && _offlineBrokerRequest != null) {
      offlineRequestMap =
          ImplicitHybridTableRouteInfo.getRequestMapFromRoutingTable(TableType.OFFLINE, _offlineRoutingTable,
              _offlineBrokerRequest, requestId, brokerId, preferTls);
    }

    if (_realtimeRoutingTable != null && _realtimeBrokerRequest != null) {
      realtimeRequestMap =
          ImplicitHybridTableRouteInfo.getRequestMapFromRoutingTable(TableType.REALTIME, _realtimeRoutingTable,
              _realtimeBrokerRequest, requestId, brokerId, preferTls);
    }

    if (offlineRequestMap != null) {
      requestMap = offlineRequestMap;
    }
    if (realtimeRequestMap != null) {
      if (requestMap == null) {
        requestMap = realtimeRequestMap;
      } else {
        requestMap.putAll(realtimeRequestMap);
      }
    }

    return requestMap;
  }
}
