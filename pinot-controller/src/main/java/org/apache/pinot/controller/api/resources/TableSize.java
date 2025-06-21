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
import java.util.HashMap;
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
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.spi.auth.core.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class TableSize {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSize.class);

  @Inject
  ControllerConf _controllerConf;
  @Inject
  TableSizeReader _tableSizeReader;

  @GET
  @Path("/tables/{tableName}/size")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SIZE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Read table sizes", notes = "Get table size details. Table size is the size of untarred "
      + "segments including replication")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public TableSizeReader.TableSizeDetails getTableSize(
      @ApiParam(value = "Table name without type", required = true, example = "myTable | myTable_OFFLINE")
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information") @DefaultValue("true") @QueryParam("verbose") boolean verbose,
      @ApiParam(value = "Include replaced segments") @DefaultValue("true")
      @QueryParam("includeReplacedSegments") boolean includeReplacedSegments,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableSizeReader.TableSizeDetails tableSizeDetails = null;
    try {
      tableSizeDetails =
          _tableSizeReader.getTableSizeDetails(tableName, _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000,
              includeReplacedSegments);
      if (!verbose) {
        if (tableSizeDetails._offlineSegments != null) {
          tableSizeDetails._offlineSegments._segments = new HashMap<>();
        }
        if (tableSizeDetails._realtimeSegments != null) {
          tableSizeDetails._realtimeSegments._segments = new HashMap<>();
        }
      }
    } catch (Throwable t) {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to read table size for %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR, t);
    }

    if (tableSizeDetails == null) {
      throw new ControllerApplicationException(LOGGER, "Table " + tableName + " not found", Response.Status.NOT_FOUND);
    }
    return tableSizeDetails;
  }
}
