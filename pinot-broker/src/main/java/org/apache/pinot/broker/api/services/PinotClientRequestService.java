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
package org.apache.pinot.broker.api.services;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.glassfish.jersey.server.ManagedAsync;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Query", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public interface PinotClientRequestService {

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  void processSqlQueryGet(@ApiParam(value = "Query", required = true) @QueryParam("sql") String query,
      @ApiParam(value = "Trace enabled") @QueryParam(Request.TRACE) String traceEnabled,
      @ApiParam(value = "Debug options") @QueryParam(Request.DEBUG_OPTIONS) String debugOptions,
      @Suspended AsyncResponse asyncResponse, @Context org.glassfish.grizzly.http.server.Request requestContext);

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  void processSqlQueryPost(String query, @Suspended AsyncResponse asyncResponse,
      @Context org.glassfish.grizzly.http.server.Request requestContext);
}
