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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;


public interface Route {
  @Nullable
  BrokerRequest getOfflineBrokerRequest();
  @Nullable
  BrokerRequest getRealtimeBrokerRequest();
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable();
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable();

  List<String> getUnavailableSegments();

  int getNumPrunedSegmentsTotal();

  boolean isEmpty();

  Map<ServerRoutingInstance, InstanceRequest> getOfflineRequestMap(long requestId, String brokerId, boolean preferTls);

  Map<ServerRoutingInstance, InstanceRequest> getRealtimeRequestMap(long requestId, String brokerId, boolean preferTls);

  default Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId,
      boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> offlineRequestMap =
        getOfflineRequestMap(requestId, brokerId, preferTls);
    Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap =
        getRealtimeRequestMap(requestId, brokerId, preferTls);
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
