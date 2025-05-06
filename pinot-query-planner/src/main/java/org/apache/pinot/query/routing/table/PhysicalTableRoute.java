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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class PhysicalTableRoute {
  private final Map<ServerInstance, ServerRouteInfo> _serverRouteInfoMap;
  private final List<String> _unavailableSegments;
  private final int _numPrunedSegments;
  private final String _tableName;

  public static PhysicalTableRoute from(String tableName, RoutingManager routingManager, BrokerRequest brokerRequest,
      long requestId) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    Preconditions.checkNotNull(tableType);
    // Table name with type
    RoutingTable routingTable = null;
    if (!routingManager.isTableDisabled(tableName)) {
      routingTable = routingManager.getRoutingTable(brokerRequest, tableName, requestId);
    }
    if (routingTable == null) {
      return null;
    }
    return new PhysicalTableRoute(routingTable.getServerInstanceToSegmentsMap(), tableName,
        routingTable.getUnavailableSegments(), routingTable.getNumPrunedSegments());
  }

  private PhysicalTableRoute(Map<ServerInstance, ServerRouteInfo> serverRouteInfoMap, String tableName,
      List<String> unavailableSegments, int numPrunedSegments) {
    _serverRouteInfoMap = serverRouteInfoMap;
    _tableName = tableName;
    _unavailableSegments = unavailableSegments;
    _numPrunedSegments = numPrunedSegments;
  }

  public Map<ServerInstance, ServerRouteInfo> getServerRouteInfoMap() {
    return _serverRouteInfoMap;
  }

  public String getTableName() {
    return _tableName;
  }

  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  public int getNumPrunedSegments() {
    return _numPrunedSegments;
  }
}
