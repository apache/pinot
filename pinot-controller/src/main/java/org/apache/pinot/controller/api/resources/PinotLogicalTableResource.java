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
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableUtils;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "LogicalTable", authorizations = {
    @Authorization(value = SWAGGER_AUTHORIZATION_KEY), @Authorization(value = DATABASE)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' "
            + "database context will be considered.")
}))
@Path("/")
public class PinotLogicalTableResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLogicalTableResource.class);
  private static final String DEFAULT_BROKER_TENANT = "DefaultTenant";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  AccessControlFactory _accessControlFactory;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables")
  @Authorize(targetType = TargetType.CLUSTER, paramName = "tableName", action = Actions.Cluster.GET_TABLE)
  @ApiOperation(value = "List all logical table names", notes = "Lists all logical table names")
  public List<String> listLogicalTableNames(@Context HttpHeaders headers) {
    return _pinotHelixResourceManager.getAllLogicalTableNames();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIG)
  @ApiOperation(value = "Get a logical table", notes = "Gets a logical table by name")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Logical table not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String getLogicalTable(
      @ApiParam(value = "Logical table name", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    LOGGER.info("Looking for logical table {}", tableName);
    LogicalTableConfig logicalTableConfig = _pinotHelixResourceManager.getLogicalTable(tableName);
    if (logicalTableConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Logical table not found", Response.Status.NOT_FOUND);
    }
    return logicalTableConfig.toPrettyJsonString();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables")
  @ApiOperation(value = "Add a new logical table", notes = "Adds a new logical table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully created logical table"), @ApiResponse(code = 409, message =
      "Logical table already exists"), @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @ManualAuthorization
  public SuccessResponse addLogicalTable(
      String logicalTableJsonString, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<LogicalTableConfig, Map<String, Object>> logicalTableAndUnrecognizedProps =
        getLogicalAndUnrecognizedPropertiesFromJson(logicalTableJsonString);
    LogicalTableConfig logicalTableConfig = logicalTableAndUnrecognizedProps.getLeft();
    String tableName = DatabaseUtils.translateTableName(logicalTableConfig.getTableName(), httpHeaders);
    logicalTableConfig.setTableName(tableName);

    // validate permission
    ResourceUtils.checkPermissionAndAccess(tableName, request, httpHeaders, AccessType.CREATE,
        Actions.Table.CREATE_TABLE, _accessControlFactory, LOGGER);

    SuccessResponse successResponse = addLogicalTable(logicalTableConfig);
    return new ConfigSuccessResponse(successResponse.getStatus(), logicalTableAndUnrecognizedProps.getRight());
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/logicalTables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a logical table", notes = "Updates a logical table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated schema"), @ApiResponse(code = 404, message = "Schema "
      + "not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500,
      message = "Internal error")
  })
  public SuccessResponse updateLogicalTable(
      @ApiParam(value = "Name of the logical table", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers, String logicalTableJsonString) {
    Pair<LogicalTableConfig, Map<String, Object>> logicalTableAndUnrecognizedProps =
        getLogicalAndUnrecognizedPropertiesFromJson(logicalTableJsonString);
    LogicalTableConfig logicalTableConfig = logicalTableAndUnrecognizedProps.getLeft();

    Preconditions.checkArgument(logicalTableConfig.getTableName().equals(tableName),
        "Logical table name in the request body should match the table name in the URL");

    tableName = DatabaseUtils.translateTableName(tableName, headers);
    logicalTableConfig.setTableName(tableName);

    SuccessResponse successResponse = updateLogicalTable(tableName, logicalTableConfig);
    return new ConfigSuccessResponse(successResponse.getStatus(), logicalTableAndUnrecognizedProps.getRight());
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TABLE)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a logical table", notes = "Deletes a logical table by name")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully deleted logical table"), @ApiResponse(code = 404, message =
      "Logical table not found"), @ApiResponse(code = 500, message = "Error deleting logical table")
  })
  public SuccessResponse deleteLogicalTable(
      @ApiParam(value = "Logical table name", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    if (_pinotHelixResourceManager.deleteLogicalTable(tableName)) {
      return new SuccessResponse(tableName + " logical table successfully deleted.");
    } else {
      throw new ControllerApplicationException(LOGGER, "Failed to delete logical table",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private Pair<LogicalTableConfig, Map<String, Object>> getLogicalAndUnrecognizedPropertiesFromJson(
      String logicalTableJsonString)
      throws ControllerApplicationException {
    try {
      return JsonUtils.stringToObjectAndUnrecognizedProperties(logicalTableJsonString, LogicalTableConfig.class);
    } catch (Exception e) {
      String msg =
          String.format("Invalid logical table json config: %s. Reason: %s", logicalTableJsonString, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
  }

  private SuccessResponse addLogicalTable(LogicalTableConfig logicalTableConfig) {
    String tableName = logicalTableConfig.getTableName();
    try {
      if (StringUtils.isEmpty(logicalTableConfig.getBrokerTenant())) {
        logicalTableConfig.setBrokerTenant(DEFAULT_BROKER_TENANT);
      }

      LogicalTableUtils.validateLogicalTableName(
          logicalTableConfig,
          _pinotHelixResourceManager.getAllTables(),
          _pinotHelixResourceManager.getAllBrokerTenantNames()
      );
      _pinotHelixResourceManager.addLogicalTable(logicalTableConfig);
      return new SuccessResponse(tableName + " logical table successfully added.");
    } catch (TableAlreadyExistsException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to add new logical table " + tableName + ". Reason: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private SuccessResponse updateLogicalTable(String tableName, LogicalTableConfig logicalTableConfig) {
    try {
      if (StringUtils.isEmpty(logicalTableConfig.getBrokerTenant())) {
        logicalTableConfig.setBrokerTenant(DEFAULT_BROKER_TENANT);
      }

      LogicalTableUtils.validateLogicalTableName(
          logicalTableConfig,
          _pinotHelixResourceManager.getAllTables(),
          _pinotHelixResourceManager.getAllBrokerTenantNames()
      );
      _pinotHelixResourceManager.updateLogicalTable(logicalTableConfig);
      return new SuccessResponse(logicalTableConfig.getTableName() + " logical table successfully updated.");
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to find logical table " + tableName,
          Response.Status.NOT_FOUND, e);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to update logical table " + tableName + ". Reason: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
