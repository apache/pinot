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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Query")
@Path("/")
public class PinotClientRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequest.class);

  @Inject
  private BrokerRequestHandler requestHandler;

  @Inject
  private BrokerMetrics brokerMetrics;

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")})
  public void processQueryGet(
      // Query param "bql" is for backward compatibility
      @ApiParam(value = "Query", required = true) @QueryParam("bql") String query,
      @ApiParam(value = "Trace enabled") @QueryParam(Request.TRACE) String traceEnabled,
      @ApiParam(value = "Debug options") @QueryParam(Request.DEBUG_OPTIONS) String debugOptions,
      @Suspended AsyncResponse asyncResponse) {
    try {
      ObjectNode requestJson = JsonUtils.newObjectNode();
      requestJson.put(Request.PQL, query);
      if (traceEnabled != null) {
        requestJson.put(Request.TRACE, traceEnabled);
      }
      if (debugOptions != null) {
        requestJson.put(Request.DEBUG_OPTIONS, debugOptions);
      }
      BrokerResponse brokerResponse = requestHandler.handleRequest(requestJson, null, new RequestStatistics());
      asyncResponse.resume(brokerResponse.toJsonString());
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1L);
      asyncResponse.resume(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")})
  public void processQueryPost(String query, @Suspended AsyncResponse asyncResponse) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      BrokerResponse brokerResponse = requestHandler.handleRequest(requestJson, null, new RequestStatistics());
      asyncResponse.resume(brokerResponse);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing POST request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1L);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot using sql")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")})
  public void processSqlQueryGet(@ApiParam(value = "Query", required = true) @QueryParam("sql") String query,
      @ApiParam(value = "Trace enabled") @QueryParam(Request.TRACE) String traceEnabled,
      @ApiParam(value = "Debug options") @QueryParam(Request.DEBUG_OPTIONS) String debugOptions,
      @Suspended AsyncResponse asyncResponse) {
    try {
      ObjectNode requestJson = JsonUtils.newObjectNode();
      requestJson.put(Request.SQL, query);
      String queryOptions = constructSqlQueryOptions();
      requestJson.put(Request.QUERY_OPTIONS, queryOptions);
      if (traceEnabled != null) {
        requestJson.put(Request.TRACE, traceEnabled);
      }
      if (debugOptions != null) {
        requestJson.put(Request.DEBUG_OPTIONS, debugOptions);
      }
      BrokerResponse brokerResponse = requestHandler.handleRequest(requestJson, null, new RequestStatistics());
      asyncResponse.resume(brokerResponse.toJsonString());
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1L);
      asyncResponse.resume(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot using sql")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Query response"), @ApiResponse(code = 500, message = "Internal Server Error")})
  public void processSqlQueryPost(String query, @Suspended AsyncResponse asyncResponse) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      if (!requestJson.has(Request.SQL)) {
        throw new IllegalStateException("Payload is missing the query string field 'sql'");
      }
      String queryOptions = constructSqlQueryOptions();
      // the only query options as of now are sql related. do not allow any custom query options in sql endpoint
      ObjectNode sqlRequestJson = ((ObjectNode) requestJson).put(Request.QUERY_OPTIONS, queryOptions);
      BrokerResponse brokerResponse = requestHandler.handleRequest(sqlRequestJson, null, new RequestStatistics());
      asyncResponse.resume(brokerResponse.toJsonString());
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing POST request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1L);
      asyncResponse.resume(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  private String constructSqlQueryOptions() {
    return Request.QueryOptionKey.GROUP_BY_MODE + "=" + Request.SQL + ";" + Request.QueryOptionKey.RESPONSE_FORMAT + "="
        + Request.SQL;
  }
}
