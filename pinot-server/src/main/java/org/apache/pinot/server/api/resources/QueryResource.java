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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.core.query.utils.QueryIdUtils;
import org.apache.pinot.core.transport.InstanceRequestHandler;
import org.apache.pinot.server.starter.ServerInstance;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * API to cancel query running on the server, given a queryId.
 */
@Api(tags = "Query", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class QueryResource {
  @Inject
  private ServerInstance _serverInstance;

  @DELETE
  @Path("/query/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Cancel a query running on the server as identified by the queryId", notes = "No effect if "
      + "no query exists for the given queryId. Query may continue to run for a short while after calling cancel as "
      + "it's done in a non-blocking manner. The cancel API can be called multiple times.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Query not found running on the server")
  })
  public String cancelQuery(
      @ApiParam(value = "QueryId as in the format of <brokerId>_<requestId> or <brokerId>_<requestId>_(O|R)",
          required = true)
      @PathParam("queryId") String queryId) {
    try {
      InstanceRequestHandler requestHandler = _serverInstance.getInstanceRequestHandler();
      boolean queryCancelled;
      if (QueryIdUtils.hasTypeSuffix(queryId)) {
        queryCancelled = requestHandler.cancelQuery(queryId);
      } else {
        boolean offlineQueryCancelled = requestHandler.cancelQuery(QueryIdUtils.withOfflineSuffix(queryId));
        boolean realtimeQueryCancelled = requestHandler.cancelQuery(QueryIdUtils.withRealtimeSuffix(queryId));
        queryCancelled = offlineQueryCancelled | realtimeQueryCancelled;
      }
      if (queryCancelled) {
        return "Cancelled query: " + queryId;
      }
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Failed to cancel query: %s on the server due to error: %s", queryId, e.getMessage()))
          .build());
    }
    throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND).entity(String.format("Query: %s not found on the server", queryId))
            .build());
  }

  @GET
  @Path("/queries/id")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get queryIds of running queries on the server", notes = "QueryIds are in the format of "
      + "<brokerId>_<requestId>_(O|R)")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public Set<String> getRunningQueryIds() {
    try {
      return _serverInstance.getInstanceRequestHandler().getRunningQueryIds();
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to get queryIds of running queries on the server due to error: " + e.getMessage()).build());
    }
  }
}
