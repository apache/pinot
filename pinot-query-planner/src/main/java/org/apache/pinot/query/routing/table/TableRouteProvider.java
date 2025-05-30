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

import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.apache.pinot.spi.auth.request.BrokerRequest;


/**
 * The TableRoute interface provides the metadata required to route query execution to servers. The important sources
 * of the metadata are table config, broker routing information and the broker request.
 */
public interface TableRouteProvider {
  TableRouteInfo getTableRouteInfo(String tableName, TableCache tableCache,
      RoutingManager routingManager);

  /**
   * Calculate the Routing Table for a query. The routing table consists of the server name and list of segments that
   * have to be queried on that server.
   * Note that the implementation is expected to signal whether the calculation was successful or not by returning a
   * null from the getter functions getOfflineRoutingTable() and getRealtimeRoutingTable().
   * @param tableRouteInfo the table route info.
   * @param routingManager the routing manager.
   * @param offlineBrokerRequest Broker Request for the offline table.
   * @param realtimeBrokerRequest Broker Request for the realtime table.
   * @param requestId Request ID assigned to the query.
   */
  void calculateRoutes(TableRouteInfo tableRouteInfo, RoutingManager routingManager, BrokerRequest offlineBrokerRequest,
      BrokerRequest realtimeBrokerRequest, long requestId);
}
