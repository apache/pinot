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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
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
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.SCHEMA_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public interface PinotSchemaService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "List all schema names", notes = "Lists all schema names")
  String listSchemaNames();

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @ApiOperation(value = "Get a schema", notes = "Gets a schema by name")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  String getSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName);

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a schema", notes = "Deletes a schema by name")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully deleted schema"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 409, message = "Schema is in use"),
      @ApiResponse(code = 500, message = "Error deleting schema")
  })
  SuccessResponse deleteSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName);

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated schema"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  ConfigSuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Whether to reload the table if the new schema is backward compatible") @DefaultValue("false")
      @QueryParam("reload") boolean reload, FormDataMultiPart multiPart);

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated schema"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  ConfigSuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Whether to reload the table if the new schema is backward compatible") @DefaultValue("false")
      @QueryParam("reload") boolean reload, String schemaJsonString);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully created schema"),
      @ApiResponse(code = 409, message = "Schema already exists"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  ConfigSuccessResponse addSchema(
      @ApiParam(value = "Whether to override the schema if the schema exists") @DefaultValue("true")
      @QueryParam("override") boolean override, FormDataMultiPart multiPart, @Context HttpHeaders httpHeaders,
      @Context Request request);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully created schema"),
      @ApiResponse(code = 409, message = "Schema already exists"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  ConfigSuccessResponse addSchema(
      @ApiParam(value = "Whether to override the schema if the schema exists") @DefaultValue("true")
      @QueryParam("override") boolean override, String schemaJsonString, @Context HttpHeaders httpHeaders,
      @Context Request request);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully validated schema"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  String validateSchema(FormDataMultiPart multiPart);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully validated schema"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  String validateSchema(String schemaJsonString);
}
