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


/**
 * Interface representing a route for Pinot queries.
 */
public interface Route {

  /**
   * Gets the offline broker request.
   *
   * @return the offline broker request, or null if not available
   */
  @Nullable
  BrokerRequest getOfflineBrokerRequest();

  /**
   * Gets the realtime broker request.
   *
   * @return the realtime broker request, or null if not available
   */
  @Nullable
  BrokerRequest getRealtimeBrokerRequest();

  /**
   * Gets the offline routing table.
   *
   * @return the offline routing table, or null if not available
   */
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable();

  /**
   * Gets the realtime routing table.
   *
   * @return the realtime routing table, or null if not available
   */
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable();

  /**
   * Gets the list of unavailable segments.
   *
   * @return the list of unavailable segments
   */
  List<String> getUnavailableSegments();

  /**
   * Gets the total number of pruned segments.
   *
   * @return the total number of pruned segments
   */
  int getNumPrunedSegmentsTotal();

  /**
   * Checks if the route is empty.
   *
   * @return true if the route is empty, false otherwise
   */
  boolean isEmpty();

  /**
   * Gets the offline request map.
   *
   * @param requestId the request ID
   * @param brokerId the broker ID
   * @param preferTls whether to prefer TLS
   * @return the offline request map
   */
  Map<ServerRoutingInstance, InstanceRequest> getOfflineRequestMap(long requestId, String brokerId, boolean preferTls);

  /**
   * Gets the realtime request map.
   *
   * @param requestId the request ID
   * @param brokerId the broker ID
   * @param preferTls whether to prefer TLS
   * @return the realtime request map
   */
  Map<ServerRoutingInstance, InstanceRequest> getRealtimeRequestMap(long requestId, String brokerId, boolean preferTls);

  /**
   * Gets the combined request map.
   *
   * @param requestId the request ID
   * @param brokerId the broker ID
   * @param preferTls whether to prefer TLS
   * @return the combined request map
   */
  Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls);
}