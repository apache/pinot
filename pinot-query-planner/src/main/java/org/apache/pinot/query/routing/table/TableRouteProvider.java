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
package org.apache.pinot.query.routing.table;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * The TableRoute interface provides the metadata required to route query execution to servers. The important sources
 * of the metadata are table config, broker routing information and the broker request.
 */
public interface TableRouteProvider {
  @Nullable
  TableConfig getOfflineTableConfig();

  @Nullable
  TableConfig getRealtimeTableConfig();

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

  @Nullable
  String getOfflineTableName();

  @Nullable
  String getRealtimeTableName();

  /**
   * Checks if the broker has routes for at least 1 of the physical tables.
   *
   * @return true if any route exists, false otherwise
   */
  boolean isRouteExists();

  /**
   * Checks if all the physical tables are disabled.
   *
   * @return true if the table is disabled, false otherwise
   */
  boolean isDisabled();

  @Nullable
  List<String> getDisabledTableNames();

  @Nullable
  TimeBoundaryInfo getTimeBoundaryInfo();

  TableRouteInfo calculateRoutes(RoutingManager routingManager, long requestId);

  /**
   * Calculate the Routing Table for a query. The routing table consists of the server name and list of segments that
   * have to be queried on that server.
   * Note that the implementation is expected to signal whether the calculation was successful or not by returning a
   * null from the getter functions getOfflineRoutingTable() and getRealtimeRoutingTable().
   * @param routingManager the routing manager.
   * @param offlineBrokerRequest Broker Request for the offline table.
   * @param realtimeBrokerRequest Broker Request for the realtime table.
   * @param requestId Request ID assigned to the query.
   */
  TableRouteInfo calculateRoutes(RoutingManager routingManager, BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, long requestId);

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
}
