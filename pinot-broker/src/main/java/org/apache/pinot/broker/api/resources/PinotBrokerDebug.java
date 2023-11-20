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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Debug", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
// TODO: Add APIs to return the RoutingTable (with unavailable segments)
public class PinotBrokerDebug {

  // Request ID is passed to the RoutingManager to rotate the selected replica-group.
  private final AtomicLong _requestIdGenerator = new AtomicLong();

  @Inject
  private BrokerRoutingManager _routingManager;

  @Inject
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @Inject
  AccessControlFactory _accessControlFactory;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/timeBoundary/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TIME_BOUNDARY)
  @ApiOperation(value = "Get the time boundary information for a table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Time boundary information for a table"),
      @ApiResponse(code = 404, message = "Time boundary not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
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
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_ROUTING_TABLE)
  @ApiOperation(value = "Get the routing table for a table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Routing table"),
      @ApiResponse(code = 404, message = "Routing not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<String, Map<ServerInstance, List<String>>> getRoutingTable(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName) {
    Map<String, Map<ServerInstance, List<String>>> result = new TreeMap<>();
    getRoutingTable(tableName, (tableNameWithType, routingTable) -> result.put(tableNameWithType,
        routingTable.getServerInstanceToSegmentsMap(false)));
    if (!result.isEmpty()) {
      return result;
    } else {
      throw new WebApplicationException("Cannot find routing for table: " + tableName, Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/debug/routingTable/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_ROUTING_TABLE)
  @ApiOperation(value = "Get the routing table for a table, including optional segments")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Routing table"),
      @ApiResponse(code = 404, message = "Routing not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<String, Map<ServerInstance, Pair<List<String>, List<String>>>> getRoutingTableWithOptionalSegments(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName) {
    Map<String, Map<ServerInstance, Pair<List<String>, List<String>>>> result = new TreeMap<>();
    getRoutingTable(tableName, (tableNameWithType, routingTable) -> result.put(tableNameWithType,
        routingTable.getServerInstanceToSegmentsMap()));
    if (!result.isEmpty()) {
      return result;
    } else {
      throw new WebApplicationException("Cannot find routing for table: " + tableName, Response.Status.NOT_FOUND);
    }
  }

  private void getRoutingTable(String tableName, BiConsumer<String, RoutingTable> consumer) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      RoutingTable routingTable = _routingManager.getRoutingTable(
          CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + offlineTableName), getRequestId());
      if (routingTable != null) {
        consumer.accept(offlineTableName, routingTable);
      }
    }
    if (tableType != TableType.OFFLINE) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      RoutingTable routingTable = _routingManager.getRoutingTable(
          CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + realtimeTableName), getRequestId());
      if (routingTable != null) {
        consumer.accept(realtimeTableName, routingTable);
      }
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/routingTable/sql")
  @ManualAuthorization
  @ApiOperation(value = "Get the routing table for a query")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Routing table"),
      @ApiResponse(code = 404, message = "Routing not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<ServerInstance, List<String>> getRoutingTableForQuery(
      @ApiParam(value = "SQL query (table name should have type suffix)") @QueryParam("query") String query,
      @Context HttpHeaders httpHeaders) {
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
    checkAccessControl(brokerRequest, httpHeaders);
    RoutingTable routingTable = _routingManager.getRoutingTable(brokerRequest, getRequestId());
    if (routingTable != null) {
      return routingTable.getServerInstanceToSegmentsMap(false);
    } else {
      throw new WebApplicationException("Cannot find routing for query: " + query, Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/debug/routingTable/sql")
  @ManualAuthorization
  @ApiOperation(value = "Get the routing table for a query, including optional segments")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Routing table"),
      @ApiResponse(code = 404, message = "Routing not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<ServerInstance, Pair<List<String>, List<String>>> getRoutingTableForQueryWithOptionalSegments(
      @ApiParam(value = "SQL query (table name should have type suffix)") @QueryParam("query") String query,
      @Context HttpHeaders httpHeaders) {
    Map<ServerInstance, Pair<List<String>, List<String>>> result;
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
    checkAccessControl(brokerRequest, httpHeaders);
    RoutingTable routingTable = _routingManager.getRoutingTable(brokerRequest, getRequestId());
    if (routingTable != null) {
      return routingTable.getServerInstanceToSegmentsMap();
    } else {
      throw new WebApplicationException("Cannot find routing for query: " + query, Response.Status.NOT_FOUND);
    }
  }

  private void checkAccessControl(BrokerRequest brokerRequest, HttpHeaders httpHeaders) {
    // TODO: Handle nested queries
    if (brokerRequest.isSetQuerySource() && brokerRequest.getQuerySource().isSetTableName()) {
      if (!_accessControlFactory.create()
          .hasAccess(httpHeaders, TargetType.TABLE, brokerRequest.getQuerySource().getTableName(),
              Actions.Table.GET_ROUTING_TABLE)) {
        throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
      }
    } else {
      throw new WebApplicationException("Table name is not set in the query", Response.Status.BAD_REQUEST);
    }
  }

  /**
   * API to get a snapshot of ServerRoutingStatsEntry for all the servers.
   * @return String containing server name and the associated routing stats.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/debug/serverRoutingStats")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SERVER_ROUTING_STATS)
  @ApiOperation(value = "Get the routing stats for all the servers")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server routing Stats"),
      @ApiResponse(code = 404, message = "Server routing Stats not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getServerRoutingStats() {
    return _serverRoutingStatsManager.getServerRoutingStatsStr();
  }

  private long getRequestId() {
    return _requestIdGenerator.getAndIncrement();
  }
}
