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
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.ingest.InsertStatementCoordinator;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * REST API endpoints for push-based INSERT INTO statement management.
 *
 * <p>Provides endpoints to submit, monitor, and abort INSERT INTO statements. These endpoints
 * are used by the broker to coordinate push-based data ingestion through the controller.
 *
 * <p><strong>FILE-insert completion paths.</strong> The v1 design uses two completion paths for
 * FILE inserts:
 * <ul>
 *   <li>The controller's cleanup-sweep ({@code autoCompleteFileInsertIfTaskDone}) polls Minion
 *       task state and is the load-bearing completion path today. It bypasses this REST resource
 *       and transitions the manifest to VISIBLE (or ABORTED on task failure) in-process.</li>
 *   <li>The {@code /insert/complete} endpoint exists for external task observers (e.g., a future
 *       Minion-side hook) that produce the segment-names list. It rejects empty segment lists as a
 *       basic input check; deeper IdealState membership validation lives in
 *       {@link InsertStatementCoordinator#completeFileInsert}. <strong>Do NOT route the sweep
 *       through this endpoint</strong>
 *       — the empty-list rejection would block sweep auto-complete, and the sweep already runs as
 *       the leader controller (no further auth-scoping needed).</li>
 * </ul>
 *
 * <p>Thread-safe: relies on the injected {@link InsertStatementCoordinator} for all state.
 */
@Api(tags = "Insert", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(
    apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in =
        ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
        description = "Bearer token based authentication")))
@Path("/insert")
public class InsertStatementResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatementResource.class);

  @Inject
  private InsertStatementCoordinator _coordinator;

  /**
   * Short-circuits every handler in this resource when the feature flag is off. Returns HTTP 503
   * so that a client can distinguish "feature disabled on this controller" from "request failed".
   * Without this guard, the coordinator would be reachable, return NO_EXECUTOR, and still allow
   * list/abort/status enumeration against ZK.
   */
  private void checkEnabled() {
    if (!_coordinator.isStarted()) {
      throw new ControllerApplicationException(LOGGER,
          "Push-based INSERT INTO is disabled on this controller (controller.insert.enabled=false).",
          Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  /**
   * Translates an unhandled exception into the right HTTP response. {@link IllegalArgumentException}
   * is treated as a client-error (400) — the broker's retry policy must not retry these.
   * Everything else is a server-side bug or transient I/O failure (500) and is safe to retry.
   */
  private RuntimeException translateError(Exception e, String contextMessage) {
    if (e instanceof IllegalArgumentException) {
      return new ControllerApplicationException(LOGGER, contextMessage + ": " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    }
    return new ControllerApplicationException(LOGGER, contextMessage + ": " + e.getMessage(),
        Response.Status.INTERNAL_SERVER_ERROR, e);
  }

  @POST
  @Path("/execute")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.EXECUTE_INSERT)
  @Authenticate(AccessType.CREATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Submit an INSERT INTO request", notes = "Submits an insert request for execution. "
      + "Supports idempotency via requestId and payloadHash.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Insert request accepted"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public InsertResult executeInsert(InsertRequest request,
      @ApiParam(value = "Table name with type — required and must match the request body's tableName "
          + "for table-scoped authorization to bind correctly", required = true)
      @QueryParam("tableName") String tableNameForAuth,
      @Context HttpHeaders headers) {
    checkEnabled();
    if (tableNameForAuth == null || tableNameForAuth.isEmpty()) {
      throw new ControllerApplicationException(LOGGER,
          "Query parameter 'tableName' is required for table-scoped authorization", Response.Status.BAD_REQUEST);
    }
    if (request == null) {
      throw new ControllerApplicationException(LOGGER, "Request body is required",
          Response.Status.BAD_REQUEST);
    }
    if (request.getTableName() == null || request.getTableName().isEmpty()) {
      throw new ControllerApplicationException(LOGGER,
          "Request body 'tableName' is required and must match query parameter 'tableName'",
          Response.Status.BAD_REQUEST);
    }
    if (!tableNameForAuth.equals(request.getTableName())) {
      throw new ControllerApplicationException(LOGGER,
          "Query parameter 'tableName' (" + tableNameForAuth + ") must match request body's tableName ("
              + request.getTableName() + ")", Response.Status.BAD_REQUEST);
    }
    try {
      return _coordinator.submitInsert(request);
    } catch (Exception e) {
      throw translateError(e, "Failed to execute insert");
    }
  }

  @GET
  @Path("/status/{statementId}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_INSERT_STATUS)
  @Authenticate(AccessType.READ)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get INSERT statement status", notes = "Returns the current status of an insert statement.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statement status returned"),
      @ApiResponse(code = 404, message = "Statement not found")
  })
  public InsertResult getStatus(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId,
      @ApiParam(value = "Table name with type (required — pairs with @Authorize TABLE-scoped check)",
          required = true)
      @QueryParam("tableName") String tableNameWithType,
      @Context HttpHeaders headers) {
    checkEnabled();
    if (tableNameWithType == null || tableNameWithType.isEmpty()) {
      // Required for table-scoped authorization. The cross-table convenience scan was an
      // auth-bypass risk: with @Authorize(paramName="tableName") and no tableName supplied, a
      // caller with READ on any one table could enumerate statementIds for other tables.
      throw new ControllerApplicationException(LOGGER,
          "Query parameter 'tableName' is required for table-scoped authorization",
          Response.Status.BAD_REQUEST);
    }
    InsertResult result;
    try {
      result = _coordinator.getStatus(statementId, tableNameWithType);
    } catch (Exception e) {
      throw translateError(e, "Failed to get status for statement " + statementId);
    }
    // Map any state=REJECTED coordinator response to HTTP 404. The contract on
    // InsertStatementState.REJECTED says callers cannot getStatus a REJECTED result — i.e., the
    // statement either never existed (NOT_FOUND) or was rejected pre-acceptance (no manifest in
    // ZK). Surfacing 404 for ALL REJECTED outcomes honors the contract on the wire and avoids
    // 200-OK responses with state=REJECTED, which would confuse clients reading status. The
    // narrow check on errorCode==NOT_FOUND missed other REJECTED paths (e.g., a runtime bug
    // where the coordinator sets a different errorCode but state=REJECTED).
    if (result != null && InsertStatementState.REJECTED.equals(result.getState())) {
      throw new ControllerApplicationException(LOGGER,
          "Statement not found or rejected: " + statementId
              + (result.getErrorCode() != null ? " (errorCode=" + result.getErrorCode() + ")" : ""),
          Response.Status.NOT_FOUND);
    }
    return result;
  }

  @POST
  @Path("/abort/{statementId}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.ABORT_INSERT)
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Abort an INSERT statement", notes = "Aborts a running insert statement and releases "
      + "any resources it holds.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statement aborted"),
      @ApiResponse(code = 404, message = "Statement not found")
  })
  public InsertResult abortStatement(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId,
      @ApiParam(value = "Table name with type (required — pairs with @Authorize TABLE-scoped check)",
          required = true)
      @QueryParam("tableName") String tableNameWithType,
      @Context HttpHeaders headers) {
    checkEnabled();
    if (tableNameWithType == null || tableNameWithType.isEmpty()) {
      // See getStatus — same auth-bypass concern.
      throw new ControllerApplicationException(LOGGER,
          "Query parameter 'tableName' is required for table-scoped authorization",
          Response.Status.BAD_REQUEST);
    }
    try {
      return _coordinator.abortStatement(statementId, tableNameWithType);
    } catch (Exception e) {
      throw translateError(e, "Failed to abort statement " + statementId);
    }
  }

  @POST
  @Path("/complete/{statementId}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.COMPLETE_INSERT)
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Complete a file INSERT statement",
      notes = "Called by a Minion task observer when segment generation and push is done. Finalizes the segment "
          + "lineage and makes the new segments visible to queries. v1 note: SegmentGenerationAndPushTask does NOT "
          + "currently call this endpoint; the load-bearing completion path is the controller cleanup sweep's "
          + "autoCompleteFileInsertIfTaskDone poll. This endpoint exists for external task observers (e.g., a "
          + "future Minion-side hook) that produce the segment names list.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statement completed"),
      @ApiResponse(code = 404, message = "Statement not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public InsertResult completeFileInsert(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId,
      @ApiParam(value = "Table name with type (required to scope the completion to a specific table — "
          + "omitting it would let any caller with UPDATE access target a foreign tenant's manifest)",
          required = true)
      @QueryParam("tableName") String tableNameWithType,
      @ApiParam(value = "Segment names produced by the task", required = true) List<String> segmentNames,
      @Context HttpHeaders headers) {
    checkEnabled();
    if (tableNameWithType == null || tableNameWithType.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Query parameter 'tableName' is required for "
          + "/insert/complete/{statementId} to scope the completion to a specific table",
          Response.Status.BAD_REQUEST);
    }
    if (segmentNames == null || segmentNames.isEmpty()) {
      // Empty list would let an attacker with /insert/complete UPDATE access flip the manifest
      // to VISIBLE before the Minion task actually finishes, leaving the produced segments
      // orphaned with the manifest claiming visible-with-no-segments. Sweep auto-complete is
      // an internal-only path and does not call this REST endpoint.
      throw new ControllerApplicationException(LOGGER, "Request body 'segmentNames' is required and must "
          + "be a non-empty list", Response.Status.BAD_REQUEST);
    }
    try {
      return _coordinator.completeFileInsert(statementId, tableNameWithType, segmentNames);
    } catch (Exception e) {
      throw translateError(e, "Failed to complete file insert for statement " + statementId);
    }
  }

  @GET
  @Path("/list")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.LIST_INSERTS)
  @Authenticate(AccessType.READ)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List INSERT statements", notes = "Lists all insert statements for a given table.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statements listed"),
      @ApiResponse(code = 400, message = "Bad request")
  })
  public List<InsertResult> listStatements(
      @ApiParam(value = "Table name with type", required = true) @QueryParam("tableName") String tableNameWithType,
      @Context HttpHeaders headers) {
    checkEnabled();
    if (tableNameWithType == null || tableNameWithType.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Query parameter 'tableName' is required",
          Response.Status.BAD_REQUEST);
    }
    try {
      return _coordinator.listStatements(tableNameWithType);
    } catch (Exception e) {
      throw translateError(e, "Failed to list statements for table " + tableNameWithType);
    }
  }
}
