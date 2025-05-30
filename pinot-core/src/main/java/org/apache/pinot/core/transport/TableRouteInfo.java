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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Routing information for a table required to route plan fragments to servers.
 * RequestMap is used to submit plan fragments to servers.
 * Offline and realtime routing tables are used to keep track of which servers are executing the query.
 * getUnavailableSegments() returns the segments that are not available when calculating the routing table.
 * numPrunedSegmentsTotal() returns the number of segments that were pruned when calculating the routing table.
 */
public interface TableRouteInfo {

  @Nullable
  String getOfflineTableName();

  @Nullable
  String getRealtimeTableName();

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
   * Gets the table config for the offline table, if available.
   * @return the table config for the offline table, or null if not available
   */
  @Nullable
  TableConfig getOfflineTableConfig();

  /**
   * Gets the table config for the realtime table, if available.
   * @return the table config for the realtime table, or null if not available
   */
  @Nullable
  TableConfig getRealtimeTableConfig();

  /**
   * Gets the query config for the offline table, if available.
   * @return the query config for the offline table, or null if not available
   */
  @Nullable
  QueryConfig getOfflineTableQueryConfig();

  /**
   * Gets the query config for the realtime table, if available.
   * @return the query config for the realtime table, or null if not available
   */
  @Nullable
  QueryConfig getRealtimeTableQueryConfig();

  /**
   * Get the set of servers that will execute the query on the segments of the OFFLINE table.
   * @return the set of servers that will execute the query on the segments of the OFFLINE table
   */
  Set<ServerInstance> getOfflineExecutionServers();

  /**
   * Get the set of servers that will execute the query on the segments of the REALTIME table.
   * @return the set of servers that will execute the query on the segments of the REALTIME table
   */
  Set<ServerInstance> getRealtimeExecutionServers();

  /**
   * Gets the routing table for the offline table, if available.
   * TODO: Note that this method cannot be supported for Logical Tables as the classes cannot be used to maintain a
   * map of table to segment list. Once Logical Tables are supported in GRPC request handler, this method will be
   * removed.
   * @return a map of server instances to their route information for the offline table, or null if not available
   */
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable();

  /**
   * Gets the routing table for the realtime table, if available.
   * TODO: Note that this method cannot be supported for Logical Tables as the classes cannot be used to maintain a
   * map of table to segment list. Once Logical Tables are supported in GRPC request handler, this method will be
   * removed.
   * @return a map of server instances to their route information for the realtime table, or null if not available
   */
  @Nullable
  Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable();

  /**
   * Checks if the table exists. A table exists if all required TableConfig objects are available.
   *
   * @return true if the table exists, false otherwise
   */
  boolean isExists();

  /**
   * Checks if there are both offline and realtime tables.
   *
   * @return true if the table is a hybrid table, false otherwise
   */
  boolean isHybrid();

  /**
   * Checks if the table has offline tables only
   *
   * @return true if the table has offline tables only, false otherwise
   */
  boolean isOffline();

  /**
   * Checks if the table has realtime tables only.
   *
   * @return true if the table table has realtime tables only, false otherwise
   */
  boolean isRealtime();

  /**
   * Checks if the table has at least 1 offline table. It may or may not have realtime tables as well.
   *
   * @return true if the table has at least 1 offline table, false otherwise
   */
  boolean hasOffline();

  /**
   * Checks if the table has at least 1 realtime table. It may or may not have offline tables as well.
   *
   * @return true if the table at least 1 realtime table, false otherwise
   */
  boolean hasRealtime();

  /**
   * Checks if the broker has routes for at least 1 of the physical tables.
   *
   * @return true if any route exists, false otherwise
   */
  boolean isRouteExists();

  boolean isOfflineRouteExists();

  boolean isRealtimeRouteExists();

  /**
   * Checks if all the physical tables are disabled.
   *
   * @return true if the table is disabled, false otherwise
   */
  boolean isDisabled();

  boolean isOfflineTableDisabled();

  boolean isRealtimeTableDisabled();

  @Nullable
  List<String> getDisabledTableNames();

  @Nullable
  TimeBoundaryInfo getTimeBoundaryInfo();

  /**
   * Gets the list of unavailable segments for the table.
   *
   * @return a list of unavailable segment names
   */
  List<String> getUnavailableSegments();

  /**
   * Gets the total number of pruned segments for the table.
   *
   * @return the total number of pruned segments
   */
  int getNumPrunedSegmentsTotal();

  /**
   * Gets the combined request map for the table, including both offline and realtime requests.
   *
   * @param requestId the request ID
   * @param brokerId the broker ID
   * @param preferTls whether to prefer TLS for the requests
   * @return A map of ServerRoutingInstance and InstanceRequest
   */
  Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls);
}
