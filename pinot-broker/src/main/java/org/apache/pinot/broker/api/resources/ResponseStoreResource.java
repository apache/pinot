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
package org.apache.pinot.broker.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Collection;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * This resource API provides API to read cursors as well as admin function such as list, read and delete response
 * stores
 */
@Api(tags = "ResponseStore", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
    description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")))
@Path("/responseStore")
public class ResponseStoreResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResponseStoreResource.class);

  @Inject
  private PinotConfiguration _brokerConf;

  @Inject
  private BrokerMetrics _brokerMetrics;

  @Inject
  private AbstractResponseStore _responseStore;

  @Inject
  AccessControlFactory _accessControlFactory;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_RESPONSE_STORE)
  @ApiOperation(value = "Get requestIds of all response stores.", notes = "Get requestIds of all response stores")
  public Collection<CursorResponse> getResults(@Context HttpHeaders headers) {
    try {
      return _responseStore.getAllStoredResponses();
    } catch (Exception e) {
      throw new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{requestId}")
  @ApiOperation(value = "Response without ResultTable of a query")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public BrokerResponse getSqlQueryMetadata(
      @ApiParam(value = "Request ID of the query", required = true) @PathParam("requestId") String requestId,
      @Context org.glassfish.grizzly.http.server.Request requestContext) {
    try {
      if (_responseStore.exists(requestId)) {
        CursorResponse response = _responseStore.readResponse(requestId);
        AccessControl accessControl = _accessControlFactory.create();
        TableAuthorizationResult result = accessControl.authorize(
            org.apache.pinot.broker.api.resources.PinotClientRequest.makeHttpIdentity(requestContext),
            response.getTablesQueried());
        if (!result.hasAccess()) {
          throw new WebApplicationException(
              Response.status(Response.Status.FORBIDDEN).entity(result.getFailureMessage()).build());
        }
        return _responseStore.readResponse(requestId);
      } else {
        throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
            .entity(String.format("Query results for %s not found.", requestId)).build());
      }
    } catch (WebApplicationException wae) {
      throw wae;
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1L);
      throw new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }
  }

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{requestId}/results")
  @ApiOperation(value = "Get result set from the query's response store")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public void getSqlQueryResult(
      @ApiParam(value = "Request ID of the query", required = true) @PathParam("requestId") String requestId,
      @ApiParam(value = "Offset in the result set", required = true) @QueryParam("offset") int offset,
      @ApiParam(value = "Number of rows to fetch") @QueryParam("numRows") Integer numRows,
      @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Suspended AsyncResponse asyncResponse) {
    try {
      if (_responseStore.exists(requestId)) {
        CursorResponse response = _responseStore.readResponse(requestId);
        AccessControl accessControl = _accessControlFactory.create();
        TableAuthorizationResult result = accessControl.authorize(
            org.apache.pinot.broker.api.resources.PinotClientRequest.makeHttpIdentity(requestContext),
            response.getTablesQueried());
        if (!result.hasAccess()) {
          throw new WebApplicationException(
              Response.status(Response.Status.FORBIDDEN).entity(result.getFailureMessage()).build());
        }

        if (numRows == null) {
          numRows = _brokerConf.getProperty(CommonConstants.CursorConfigs.CURSOR_FETCH_ROWS,
              CommonConstants.CursorConfigs.DEFAULT_CURSOR_FETCH_ROWS);
        }

        if (numRows > CommonConstants.CursorConfigs.MAX_CURSOR_FETCH_ROWS) {
          throw new WebApplicationException(
              "Result Size greater than " + CommonConstants.CursorConfigs.MAX_CURSOR_FETCH_ROWS + " not allowed",
              Response.status(Response.Status.BAD_REQUEST).build());
        }

        asyncResponse.resume(
            PinotClientRequest.getPinotQueryResponse(_responseStore.handleCursorRequest(requestId, offset, numRows)));
      } else {
        throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
            .entity(String.format("Query results for %s not found.", requestId)).build());
      }
    } catch (WebApplicationException wae) {
      asyncResponse.resume(wae);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1L);
      asyncResponse.resume(new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build()));
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{requestId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_RESPONSE_STORE)
  @ApiOperation(value = "Delete the response store of a query", notes = "Delete the response store of a query")
  public String deleteResponse(
      @ApiParam(value = "Request ID of the query", required = true) @PathParam("requestId") String requestId,
      @Context HttpHeaders headers) {
    try {
      if (_responseStore.deleteResponse(requestId)) {
        return "Query Results for " + requestId + " deleted.";
      }
    } catch (Exception e) {
      throw new WebApplicationException(e,
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
    }

    // Query Result not found. Throw error.
    throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND).entity(String.format("Query results for %s not found.", requestId))
            .build());
  }
}
