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
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.IdealState;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotRealtimeTableResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRealtimeTableResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;

  @POST
  @Path("/tables/{tableName}/pauseConsumption")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Pause consumption of a realtime table", notes = "Pause the consumption of a realtime table")
  public Response pauseConsumption(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    validate(tableNameWithType);
    try {
      return Response.ok(_pinotLLCRealtimeSegmentManager.pauseConsumption(tableNameWithType)).build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("/tables/{tableName}/resumeConsumption")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Resume consumption of a realtime table", notes =
      "Resume the consumption for a realtime table. ConsumeFrom parameter indicates from which offsets "
          + "consumption should resume. If consumeFrom parameter is not provided, consumption continues based on the "
          + "offsets in segment ZK metadata, and in case the offsets are already gone, the first available offsets are "
          + "picked to minimize the data loss.")
  public Response resumeConsumption(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "smallest | largest") @QueryParam("consumeFrom") String consumeFrom) {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    validate(tableNameWithType);
    if (consumeFrom != null && !consumeFrom.equalsIgnoreCase("smallest") && !consumeFrom.equalsIgnoreCase("largest")) {
      throw new ControllerApplicationException(LOGGER,
          String.format("consumeFrom param '%s' is not valid.", consumeFrom), Response.Status.BAD_REQUEST);
    }
    try {
      return Response.ok(_pinotLLCRealtimeSegmentManager.resumeConsumption(tableNameWithType, consumeFrom)).build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("/tables/{tableName}/forceCommit")
  @ApiOperation(value = "Force commit the current consuming segments",
      notes = "Force commit the current segments in consuming state and restart consumption. "
          + "This should be used after schema/table config changes. "
          + "Please note that this is an asynchronous operation, "
          + "and 200 response does not mean it has actually been done already")
  public Response forceCommit(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    validate(tableNameWithType);
    try {
      _pinotLLCRealtimeSegmentManager.forceCommit(tableNameWithType);
      return Response.ok().build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }


  @GET
  @Path("/tables/{tableName}/pauseStatus")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Return pause status of a realtime table",
      notes = "Return pause status of a realtime table along with list of consuming segments.")
  public Response getConsumptionStatus(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    validate(tableNameWithType);
    try {
      return Response.ok().entity(_pinotLLCRealtimeSegmentManager.getPauseStatus(tableNameWithType)).build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private void validate(String tableNameWithType) {
    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
    if (idealState == null) {
      throw new ControllerApplicationException(LOGGER, String.format("Table %s not found!", tableNameWithType),
          Response.Status.NOT_FOUND);
    }
    if (!idealState.isEnabled()) {
      throw new ControllerApplicationException(LOGGER, String.format("Table %s is disabled!", tableNameWithType),
          Response.Status.BAD_REQUEST);
    }
  }
}
