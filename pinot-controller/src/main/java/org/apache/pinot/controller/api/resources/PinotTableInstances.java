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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotTableInstances {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableInstances.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Path("/tables/{tableName}/instances")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_INSTANCE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table instances", notes = "List instances of the given table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getTableInstances(
      @ApiParam(value = "Table name without type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Instance type", example = "broker", allowableValues = "BROKER, SERVER") @DefaultValue("")
      @QueryParam("type") String type, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    ObjectNode ret = JsonUtils.newObjectNode();
    ret.put("tableName", tableName);
    ArrayNode brokers = JsonUtils.newArrayNode();
    ArrayNode servers = JsonUtils.newArrayNode();

    if (type == null || type.isEmpty() || type.toLowerCase().equals("broker")) {
      if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "offline");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : _pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.OFFLINE)) {
          a.add(ins);
        }
        e.set("instances", a);
        brokers.add(e);
      }
      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "realtime");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : _pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.REALTIME)) {
          a.add(ins);
        }
        e.set("instances", a);
        brokers.add(e);
      }
    }

    if (type == null || type.isEmpty() || type.toLowerCase().equals("server")) {
      if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "offline");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : _pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)) {
          a.add(ins);
        }
        e.set("instances", a);
        servers.add(e);
      }

      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "realtime");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : _pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)) {
          a.add(ins);
        }
        e.set("instances", a);
        servers.add(e);
      }
    }
    ret.set("brokers", brokers);
    ret.set("server", servers);   // Keeping compatibility with previous API, so "server" and "brokers"
    return ret.toString();
  }

  @Deprecated
  @GET
  @Path("/tables/{tableName}/livebrokers")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_BROKER)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List the brokers serving a table", notes = "List live brokers of the given table based on EV")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public List<String> getLiveBrokersForTable(
      @ApiParam(value = "Table name (with or without type)", required = true)
      @PathParam("tableName") String tableName, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    try {
      return _pinotHelixResourceManager.getLiveBrokersForTable(tableName);
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Path("/tables/livebrokers")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List tables to live brokers mappings", notes = "List tables to live brokers mappings based "
      + "on EV")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<String, List<InstanceInfo>> getLiveBrokers(@Context HttpHeaders headers,
      @ApiParam(value = "Table names (with or without type)", allowMultiple = true) @QueryParam("tables")
      List<String> tables) {
    try {
      return _pinotHelixResourceManager.getTableToLiveBrokersMapping(headers.getHeaderString(DATABASE), tables);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    }
  }
}
