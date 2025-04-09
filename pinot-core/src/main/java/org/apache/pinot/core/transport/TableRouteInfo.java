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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;


/**
 * Routing information for a table required to route plan fragments to servers.
 * RequestMap is used to submit plan fragments to servers.
 * Offline and realtime routing tables are used to keep track of which servers are executing the query.
 * getUnavailableSegments() returns the segments that are not available when calculating the routing table.
 * numPrunedSegmentsTotal() returns the number of segments that were pruned when calculating the routing table.
 */
public interface TableRouteInfo {

  /**
   * Gets the broker request for the offline table, if available.
   *
   * @return the broker request for the offline table, or null if not available
   */
  @Nullable
  BrokerRequest getOfflineBrokerRequest();

  /**
   * Gets the broker request for the realtime table, if available.
   *
   * @return the broker request for the realtime table, or null if not available
   */
  @Nullable
  BrokerRequest getRealtimeBrokerRequest();

  /**
   * Gets the routing table for the offline table, if available.
   *
   * @return a map of server instances to their route information for the offline table, or null if not available
   */
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable();

  /**
   * Gets the routing table for the realtime table, if available.
   *
   * @return a map of server instances to their route information for the realtime table, or null if not available
   */
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable();

  /**
   * Gets the combined request map for the table, including both offline and realtime requests.
   *
   * @param requestId the request ID
   * @param brokerId the broker ID
   * @param preferTls whether to prefer TLS for the requests
   * @return a map of server routing instances to their corresponding instance requests
   */
  Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls);
}
