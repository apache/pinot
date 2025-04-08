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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;


public class TableRouteUtils {
  private TableRouteUtils() {
  }

  public static Map<ServerRoutingInstance, InstanceRequest> getRequestMapFromRoutingTable(TableType tableType,
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

  public static InstanceRequest getInstanceRequest(long requestId, String brokerId, BrokerRequest brokerRequest,
      ServerRouteInfo segments) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
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

  public static Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId,
      TableRoute tableRoute, boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> offlineRequestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap = null;

    if (tableRoute.getOfflineRoutingTable() != null && tableRoute.getOfflineBrokerRequest() != null) {
      offlineRequestMap =
          TableRouteUtils.getRequestMapFromRoutingTable(TableType.OFFLINE, tableRoute.getOfflineRoutingTable(),
              tableRoute.getOfflineBrokerRequest(), requestId, brokerId, preferTls);
    }

    if (tableRoute.getRealtimeRoutingTable() != null && tableRoute.getRealtimeBrokerRequest() != null) {
      realtimeRequestMap =
          TableRouteUtils.getRequestMapFromRoutingTable(TableType.REALTIME, tableRoute.getRealtimeRoutingTable(),
              tableRoute.getRealtimeBrokerRequest(), requestId, brokerId, preferTls);
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
