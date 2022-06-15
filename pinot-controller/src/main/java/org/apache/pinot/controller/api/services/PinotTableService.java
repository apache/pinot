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
package org.apache.pinot.controller.api.services;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.resources.ConfigSuccessResponse;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.controller.api.resources.TableAndSchemaConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.glassfish.grizzly.http.server.Request;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG,
    authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(
    apiKeyAuthDefinitions = @ApiKeyAuthDefinition(
        name = HttpHeaders.AUTHORIZATION,
        in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY)))
public interface PinotTableService {
  /**
   * URI Mappings:
   * - "/tables", "/tables/": List all the tables
   * - "/tables/{tableName}", "/tables/{tableName}/": List config for specified table.
   *
   * - "/tables/{tableName}?state={state}"
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *
   * - "/tables/{tableName}?type={type}"
   *   List all tables of specified type, type can be one of {offline|realtime}.
   *
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *   * - "/tables/{tableName}?state={state}&amp;type={type}"
   *
   *   Set the state for the specified {tableName} of specified type to the specified {state} (enable|disable|drop).
   *   Type here is type of the table, one of 'offline|realtime'.
   * {@inheritDoc}
   */

  /**
   * API to create a table. Before adding, validations will be done (min number of replicas,
   * checking offline and realtime table configs match, checking for tenants existing)
   * @param tableConfigStr the table config JSON String
   * @param typesToSkip the types to skip
   * @param httpHeaders the HTTP headers
   * @param request the Request Context
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  ConfigSuccessResponse addTable(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request);

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/recommender")
  @ApiOperation(value = "Recommend config", notes = "Recommend a config with input json")
  String recommendConfig(String inputStr);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Lists all tables in cluster", notes = "Lists all tables in cluster")
  String listTables(@ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "name|creationTime|lastModifiedTime") @QueryParam("sortType") String sortTypeStr,
      @ApiParam(value = "true|false") @QueryParam("sortAsc") @DefaultValue("true") boolean sortAsc);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Get/Enable/Disable/Drop a table", notes =
      "Get/Enable/Disable/Drop a table. If table name is the only parameter specified "
          + ", the tableconfig will be printed")
  String alterTableStateOrListTableConfig(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop") @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders httpHeaders,
      @Context Request request);

  @DELETE
  @Path("/tables/{tableName}")
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Deletes a table", notes = "Deletes a table")
  SuccessResponse deleteTable(
      @ApiParam(value = "Name of the table to delete", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr);

  @PUT
  @Path("/tables/{tableName}")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
  ConfigSuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, String tableConfigString)
      throws Exception;

  @POST
  @Path("/tables/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table", notes =
      "This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  ObjectNode checkTableConfig(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip);

  @Deprecated
  @POST
  @Path("/tables/validateTableAndSchema")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table along with specified schema", notes =
      "Deprecated. Use /tableConfigs/validate instead."
          + "Validate given table config and schema. If specified schema is null, attempt to retrieve schema using the "
          + "table name. This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  String validateTableAndSchema(TableAndSchemaConfig tableSchemaConfig,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @ApiOperation(value = "Rebalances a table (reassign instances and segments for a table)", notes = "Rebalances a "
      + "table (reassign instances and segments for a table)")
  RebalanceResult rebalance(
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to rebalance table in dry-run mode") @DefaultValue("false") @QueryParam("dryRun")
          boolean dryRun,
      @ApiParam(value = "Whether to reassign instances before reassigning segments") @DefaultValue("false")
      @QueryParam("reassignInstances") boolean reassignInstances,
      @ApiParam(value = "Whether to reassign CONSUMING segments for real-time table") @DefaultValue("false")
      @QueryParam("includeConsuming") boolean includeConsuming, @ApiParam(value =
      "Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, reassign all "
          + "segments in a round-robin fashion as if adding new segments to an empty table)") @DefaultValue("false")
  @QueryParam("bootstrap") boolean bootstrap,
      @ApiParam(value = "Whether to allow downtime for the rebalance") @DefaultValue("false") @QueryParam("downtime")
          boolean downtime, @ApiParam(value =
      "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum "
          + "number of replicas allowed to be unavailable if value is negative") @DefaultValue("1")
  @QueryParam("minAvailableReplicas") int minAvailableReplicas, @ApiParam(value =
      "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot "
          + "be achieved)") @DefaultValue("false") @QueryParam("bestEfforts") boolean bestEfforts);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/state")
  @ApiOperation(value = "Get current table state", notes = "Get current table state")
  String getTableState(
      @ApiParam(value = "Name of the table to get its state", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr);

  @GET
  @Path("/tables/{tableName}/stats")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table stats", notes = "Provides metadata info/stats about the table.")
  String getTableStats(@ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr);

  @GET
  @Path("/tables/{tableName}/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table status", notes = "Provides status of the table including ingestion status")
  String getTableStatus(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr);

  @GET
  @Path("tables/{tableName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate metadata of all segments for a table", notes = "Get the aggregate metadata"
      + " of all segments for a table")
  String getTableAggregateMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
          List<String> columns);
}
