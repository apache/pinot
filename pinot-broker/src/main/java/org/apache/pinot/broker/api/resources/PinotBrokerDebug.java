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
package org.apache.pinot.broker.api.resources;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.inject.Inject;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.api.services.PinotBrokerDebugService;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


@Path("/")
public class PinotBrokerDebug implements PinotBrokerDebugService {

  @Inject
  private BrokerRoutingManager _routingManager;

  @Override
  public TimeBoundaryInfo getTimeBoundary(String tableName) {
    String offlineTableName =
        TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableName));
    TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
    if (timeBoundaryInfo != null) {
      return timeBoundaryInfo;
    } else {
      throw new WebApplicationException("Cannot find time boundary for table: " + tableName, Response.Status.NOT_FOUND);
    }
  }

  @Override
  public Map<String, Map<ServerInstance, List<String>>> getRoutingTable(String tableName) {
    Map<String, Map<ServerInstance, List<String>>> result = new TreeMap<>();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      RoutingTable routingTable = _routingManager.getRoutingTable(
          CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + offlineTableName));
      if (routingTable != null) {
        result.put(offlineTableName, routingTable.getServerInstanceToSegmentsMap());
      }
    }
    if (tableType != TableType.OFFLINE) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      RoutingTable routingTable = _routingManager.getRoutingTable(
          CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + realtimeTableName));
      if (routingTable != null) {
        result.put(realtimeTableName, routingTable.getServerInstanceToSegmentsMap());
      }
    }
    if (!result.isEmpty()) {
      return result;
    } else {
      throw new WebApplicationException("Cannot find routing for table: " + tableName, Response.Status.NOT_FOUND);
    }
  }

  @Override
  public Map<ServerInstance, List<String>> getRoutingTableForQuery(String query) {
    RoutingTable routingTable = _routingManager.getRoutingTable(CalciteSqlCompiler.compileToBrokerRequest(query));
    if (routingTable != null) {
      return routingTable.getServerInstanceToSegmentsMap();
    } else {
      throw new WebApplicationException("Cannot find routing for query: " + query, Response.Status.NOT_FOUND);
    }
  }
}
