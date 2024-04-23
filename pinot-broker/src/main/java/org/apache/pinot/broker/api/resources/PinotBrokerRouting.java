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
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Routing", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotBrokerRouting {

  @Inject
  BrokerRoutingManager _routingManager;

  @Inject
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.BUILD_ROUTING)
  @ApiOperation(value = "Build/rebuild the routing for a table", notes = "Build/rebuild the routing for a table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String buildRouting(
      @ApiParam(value = "Table name (with type)") @PathParam("tableName") String tableNameWithType,
      @Context HttpHeaders headers) {
    _routingManager.buildRouting(DatabaseUtils.translateTableName(tableNameWithType, headers));
    return "Success";
  }

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/refresh/{tableName}/{segmentName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.REFRESH_ROUTING)
  @ApiOperation(value = "Refresh the routing for a segment", notes = "Refresh the routing for a segment")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String refreshRouting(
      @ApiParam(value = "Table name (with type)") @PathParam("tableName") String tableNameWithType,
      @ApiParam(value = "Segment name") @PathParam("segmentName") String segmentName,
      @Context HttpHeaders headers) {
    _routingManager.refreshSegment(DatabaseUtils.translateTableName(tableNameWithType, headers), segmentName);
    return "Success";
  }

  @DELETE
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_ROUTING)
  @ApiOperation(value = "Remove the routing for a table", notes = "Remove the routing for a table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String removeRouting(
      @ApiParam(value = "Table name (with type)") @PathParam("tableName") String tableNameWithType,
      @Context HttpHeaders headers) {
    _routingManager.removeRouting(DatabaseUtils.translateTableName(tableNameWithType, headers));
    return "Success";
  }

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/resetServerRoutingStats")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RESET_SERVER_ROUTING_STATS)
  @ApiOperation(value = "Reset all server routing stats", notes = "Reset all server routing stats")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String resetServerRoutingStats() {
    _serverRoutingStatsManager.resetAllServersStats();
    return "Success";
  }

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/resetServerRoutingStats/{instanceId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RESET_SERVER_ROUTING_STATS)
  @ApiOperation(value = "Reset the given server's routing stats", notes = "Reset the given server's routing stats")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String resetServerRoutingStatsForInstance(
      @ApiParam(value = "Instance ID") @PathParam("instanceId") String instanceId) {
    if (_serverRoutingStatsManager.resetServerStats(instanceId)) {
      return "Success";
    } else {
      return "instance: " + instanceId + " not found in routing stats.";
    }
  }
}
