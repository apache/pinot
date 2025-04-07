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
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * The HybridTable interface represents a table that consists of both offline and realtime tables.
 * An Offline and a realtime table is represented by PhysicalTable objects.
 * In the general case, a HybridTable can have many offline and realtime tables.
 * It is used to organize metadata about hybrid tables and helps with query planning in SSE.
 */
public interface TableRoute {

  void getTableConfig(TableCache tableCache);

  void checkRoutes(RoutingManager routingManager);

  /**
   * Checks if the table exists. A table exists if all required TableConfig objects are available.
   *
   * @return true if the table exists, false otherwise
   */
  boolean isExists();

  /**
   * Checks if the broker has routes for at least 1 of the physical tables.
   *
   * @return true if any route exists, false otherwise
   */
  boolean isRouteExists();

  /**
   * Checks if routes for all the offline table(s) is available in the broker.
   *
   * @return true if an offline table route exists, false otherwise
   */
  boolean isOfflineRouteExists();

  /**
   * Checks if routes for all realtime table(s) is available in the broker.
   *
   * @return true if a realtime table route exists, false otherwise
   */
  boolean isRealtimeRouteExists();

  /**
   * Checks if all the physical tables are disabled.
   *
   * @return true if the table is disabled, false otherwise
   */
  boolean isDisabled();

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

  boolean hasTimeBoundaryInfo();

  @Nullable
  TimeBoundaryInfo getTimeBoundaryInfo();

  @Nullable
  String getOfflineTableName();

  @Nullable
  String getRealtimeTableName();

  @Nullable
  TableConfig getOfflineTableConfig();

  @Nullable
  TableConfig getRealtimeTableConfig();

  List<String> getDisabledTableNames();

  void calculateRoutes(RoutingManager routingManager, BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, long requestId);

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

  @Nullable
  Map<ServerRoutingInstance, InstanceRequest> getOfflineRequestMap(long requestId, String brokerId, boolean preferTls);

  @Nullable
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
