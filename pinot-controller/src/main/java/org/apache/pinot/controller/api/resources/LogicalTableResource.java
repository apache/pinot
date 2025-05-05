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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.data.LogicalTable;
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
public class LogicalTableResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables")
  @ApiOperation(value = "List all logical table names", notes = "Lists all logical table names")
  public List<String> listLogicalTableNames(@Context HttpHeaders headers) {
    return _pinotHelixResourceManager.getLogicalTableNames();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables/{tableName}")
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
    LogicalTable logicalTable = _pinotHelixResourceManager.getLogicalTable(tableName);
    if (logicalTable == null) {
      throw new ControllerApplicationException(LOGGER, "Logical table not found", Response.Status.NOT_FOUND);
    }
    return logicalTable.toPrettyJsonString();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/logicalTables/{tableName}")
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
      return new SuccessResponse("Logical table " + tableName + " deleted");
    } else {
      throw new ControllerApplicationException(LOGGER, "Failed to delete logical table",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
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
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    Pair<LogicalTable, Map<String, Object>> logicalTableAndUnrecognizedProps =
        getLogicalAndUnrecognizedPropertiesFromJson(logicalTableJsonString);
    LogicalTable logicalTable = logicalTableAndUnrecognizedProps.getLeft();
    validateLogicalTableName(logicalTable);
    logicalTable.setTableName(DatabaseUtils.translateTableName(logicalTable.getTableName(), headers));
    SuccessResponse successResponse = updateLogicalTable(tableName, logicalTable);
    return new ConfigSuccessResponse(successResponse.getStatus(), logicalTableAndUnrecognizedProps.getRight());
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
  public SuccessResponse addLogicalTable(
      String logicalTableJsonString, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<LogicalTable, Map<String, Object>> logicalTableAndUnrecognizedProps =
        getLogicalAndUnrecognizedPropertiesFromJson(logicalTableJsonString);
    LogicalTable logicalTable = logicalTableAndUnrecognizedProps.getLeft();
    validateLogicalTableName(logicalTable);
    String tableName = DatabaseUtils.translateTableName(logicalTable.getTableName(), httpHeaders);
    logicalTable.setTableName(tableName);
    SuccessResponse successResponse = addLogicalTable(logicalTable);
    return new ConfigSuccessResponse(successResponse.getStatus(), logicalTableAndUnrecognizedProps.getRight());
  }

  private void validateLogicalTableName(LogicalTable logicalTable) {
    if (StringUtils.isEmpty(logicalTable.getTableName())) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid logical table. Reason: 'tableName' should not be null or empty", Response.Status.BAD_REQUEST);
    }
  }

  private Pair<LogicalTable, Map<String, Object>> getLogicalAndUnrecognizedPropertiesFromJson(
      String logicalTableJsonString)
      throws ControllerApplicationException {
    try {
      return JsonUtils.stringToObjectAndUnrecognizedProperties(logicalTableJsonString, LogicalTable.class);
    } catch (Exception e) {
      String msg =
          String.format("Invalid schema config json string: %s. Reason: %s", logicalTableJsonString, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
  }

  private SuccessResponse addLogicalTable(LogicalTable logicalTable) {
    String tableName = logicalTable.getTableName();
    try {
      _pinotHelixResourceManager.addLogicalTable(logicalTable);
      return new SuccessResponse(tableName + " successfully added");
    } catch (TableAlreadyExistsException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to add new logical table " + tableName + ". Reason: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private SuccessResponse updateLogicalTable(String tableName, LogicalTable logicalTable) {
    try {
      _pinotHelixResourceManager.updateLogicalTable(logicalTable);
      return new SuccessResponse(logicalTable.getTableName() + " successfully updated");
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to find logical table " + tableName,
          Response.Status.NOT_FOUND, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to update logical table " + tableName + ". Reason: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
