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
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.ingest.InsertStatementCoordinator;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * REST API endpoints for push-based INSERT INTO statement management.
 *
 * <p>Provides endpoints to submit, monitor, and abort INSERT INTO statements. These endpoints
 * are used by the broker to coordinate push-based data ingestion through the controller.
 *
 * <p>Thread-safe: relies on the injected {@link InsertStatementCoordinator} for all state.
 */
@Api(tags = "Insert", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(
    apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in =
        ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
        description = "Bearer token based authentication")))
@Path("/insert")
public class PinotInsertResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInsertResource.class);

  @Inject
  private InsertStatementCoordinator _coordinator;

  @POST
  @Path("/execute")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Submit an INSERT INTO request", notes = "Submits an insert request for execution. "
      + "Supports idempotency via requestId and payloadHash.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Insert request accepted"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public InsertResult executeInsert(InsertRequest request, @Context HttpHeaders headers) {
    try {
      return _coordinator.submitInsert(request);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to execute insert: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("/status/{statementId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get INSERT statement status", notes = "Returns the current status of an insert statement.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statement status returned"),
      @ApiResponse(code = 404, message = "Statement not found")
  })
  public InsertResult getStatus(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId,
      @ApiParam(value = "Table name with type", required = true) @QueryParam("table") String tableNameWithType,
      @Context HttpHeaders headers) {
    if (tableNameWithType == null || tableNameWithType.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Query parameter 'table' is required",
          Response.Status.BAD_REQUEST);
    }
    try {
      return _coordinator.getStatus(statementId, tableNameWithType);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to get status for statement " + statementId + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("/abort/{statementId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Abort an INSERT statement", notes = "Aborts a running insert statement and releases "
      + "any resources it holds.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statement aborted"),
      @ApiResponse(code = 404, message = "Statement not found")
  })
  public InsertResult abortStatement(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId,
      @ApiParam(value = "Table name with type") @QueryParam("table") String tableNameWithType,
      @Context HttpHeaders headers) {
    try {
      return _coordinator.abortStatement(statementId, tableNameWithType);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to abort statement " + statementId + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List INSERT statements", notes = "Lists all insert statements for a given table.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statements listed"),
      @ApiResponse(code = 400, message = "Bad request")
  })
  public List<InsertResult> listStatements(
      @ApiParam(value = "Table name with type", required = true) @QueryParam("table") String tableNameWithType,
      @Context HttpHeaders headers) {
    if (tableNameWithType == null || tableNameWithType.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Query parameter 'table' is required",
          Response.Status.BAD_REQUEST);
    }
    try {
      return _coordinator.listStatements(tableNameWithType);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to list statements for table " + tableNameWithType + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
