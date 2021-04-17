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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


@Api(tags = "Debug")
@Path("/")
// TODO: Add APIs to return the RoutingTable (with unavailable segments)
public class PinotBrokerDebug {
  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();
  private static final CalciteSqlCompiler CALCITE_COMPILER = new CalciteSqlCompiler();

  @Inject
  private RoutingManager _routingManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/timeBoundary/{tableName}")
  @ApiOperation(value = "Get the time boundary information for a table")
  @ApiResponses(
      value = {@ApiResponse(code = 200, message = "Time boundary information for a table"), @ApiResponse(code = 404,
          message = "Time boundary not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public TimeBoundaryInfo getTimeBoundary(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName) {
    String offlineTableName =
        TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableName));
    TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
    if (timeBoundaryInfo != null) {
      return timeBoundaryInfo;
    } else {
      throw new WebApplicationException("Cannot find time boundary for table: " + tableName, Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/routingTable/{tableName}")
  @ApiOperation(value = "Get the routing table for a table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Routing table"), @ApiResponse(code = 404,
      message = "Routing not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public Map<String, Map<ServerInstance, List<String>>> getRoutingTable(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName) {
    Map<String, Map<ServerInstance, List<String>>> result = new TreeMap<>();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      RoutingTable routingTable =
          _routingManager.getRoutingTable(PQL_COMPILER.compileToBrokerRequest("SELECT * FROM " + offlineTableName));
      if (routingTable != null) {
        result.put(offlineTableName, routingTable.getServerInstanceToSegmentsMap());
      }
    }
    if (tableType != TableType.OFFLINE) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      RoutingTable routingTable =
          _routingManager.getRoutingTable(PQL_COMPILER.compileToBrokerRequest("SELECT * FROM " + realtimeTableName));
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

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/routingTable")
  @ApiOperation(value = "Get the routing table for a query")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Routing table"), @ApiResponse(code = 404,
      message = "Routing not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public Map<ServerInstance, List<String>> getRoutingTableForQuery(
      @ApiParam(value = "Pql query (table name should have type suffix)") @QueryParam("pql") String pql) {
    RoutingTable routingTable = _routingManager.getRoutingTable(PQL_COMPILER.compileToBrokerRequest(pql));
    if (routingTable != null) {
      return routingTable.getServerInstanceToSegmentsMap();
    } else {
      throw new WebApplicationException("Cannot find routing for query: " + pql, Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/routingTable/sql")
  @ApiOperation(value = "Get the routing table for a SQL query")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Routing table"), @ApiResponse(code = 404,
      message = "Routing not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public Map<ServerInstance, List<String>> getRoutingTableForSQLQuery(
      @ApiParam(value = "SQL query (table name should have type suffix)") @QueryParam("query") String query) {
    RoutingTable routingTable = _routingManager.getRoutingTable(CALCITE_COMPILER.compileToBrokerRequest(query));
    if (routingTable != null) {
      return routingTable.getServerInstanceToSegmentsMap();
    } else {
      throw new WebApplicationException("Cannot find routing for query: " + query, Response.Status.NOT_FOUND);
    }
  }
}
