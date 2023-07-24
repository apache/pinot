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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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
import java.util.Map;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Query", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotClientRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequest.class);

  @Inject
  SqlQueryExecutor _sqlQueryExecutor;

  @Inject
  private BrokerRequestHandler _requestHandler;

  @Inject
  private BrokerMetrics _brokerMetrics;

  @Inject
  private Executor _executor;

  @Inject
  private HttpConnectionManager _httpConnMgr;

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public void processSqlQueryGet(@ApiParam(value = "Query", required = true) @QueryParam("sql") String query,
      @ApiParam(value = "Trace enabled") @QueryParam(Request.TRACE) String traceEnabled,
      @ApiParam(value = "Debug options") @QueryParam(Request.DEBUG_OPTIONS) String debugOptions,
      @Suspended AsyncResponse asyncResponse, @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      ObjectNode requestJson = JsonUtils.newObjectNode();
      requestJson.put(Request.SQL, query);
      if (traceEnabled != null) {
        requestJson.put(Request.TRACE, traceEnabled);
      }
      if (debugOptions != null) {
        requestJson.put(Request.DEBUG_OPTIONS, debugOptions);
      }
      BrokerResponse brokerResponse = executeSqlQuery(requestJson, makeHttpIdentity(requestContext), true, httpHeaders);
      asyncResponse.resume(brokerResponse.toJsonString());
    } catch (WebApplicationException wae) {
      asyncResponse.resume(wae);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1L);
      asyncResponse.resume(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public void processSqlQueryPost(String query, @Suspended AsyncResponse asyncResponse,
      @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      if (!requestJson.has(Request.SQL)) {
        throw new IllegalStateException("Payload is missing the query string field 'sql'");
      }
      BrokerResponse brokerResponse =
          executeSqlQuery((ObjectNode) requestJson, makeHttpIdentity(requestContext), false, httpHeaders);
      asyncResponse.resume(brokerResponse.toJsonString());
    } catch (WebApplicationException wae) {
      asyncResponse.resume(wae);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing POST request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1L);
      asyncResponse.resume(
          new WebApplicationException(e,
              Response
                  .status(Response.Status.INTERNAL_SERVER_ERROR)
                  .entity(e.getMessage())
                  .build()));
    }
  }

  @DELETE
  @Path("query/{queryId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.CANCEL_QUERY)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Cancel a query as identified by the queryId", notes = "No effect if no query exists for the "
      + "given queryId on the requested broker. Query may continue to run for a short while after calling cancel as "
      + "it's done in a non-blocking manner. The cancel method can be called multiple times.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Query not found on the requested broker")
  })
  public String cancelQuery(
      @ApiParam(value = "QueryId as assigned by the broker", required = true) @PathParam("queryId") long queryId,
      @ApiParam(value = "Timeout for servers to respond the cancel request") @QueryParam("timeoutMs")
      @DefaultValue("3000") int timeoutMs,
      @ApiParam(value = "Return server responses for troubleshooting") @QueryParam("verbose") @DefaultValue("false")
          boolean verbose) {
    try {
      Map<String, Integer> serverResponses = verbose ? new HashMap<>() : null;
      if (_requestHandler.cancelQuery(queryId, timeoutMs, _executor, _httpConnMgr, serverResponses)) {
        String resp = "Cancelled query: " + queryId;
        if (verbose) {
          resp += " with responses from servers: " + serverResponses;
        }
        return resp;
      }
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Failed to cancel query: %s on the broker due to error: %s", queryId, e.getMessage()))
          .build());
    }
    throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND).entity(String.format("Query: %s not found on the broker", queryId))
            .build());
  }

  @GET
  @Path("queries")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_RUNNING_QUERIES)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get running queries submitted via the requested broker", notes = "The id is assigned by the "
      + "requested broker and only unique at the scope of this broker")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<Long, String> getRunningQueries() {
    try {
      return _requestHandler.getRunningQueries();
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to get running queries on the broker due to error: " + e.getMessage()).build());
    }
  }

  private BrokerResponse executeSqlQuery(ObjectNode sqlRequestJson, HttpRequesterIdentity httpRequesterIdentity,
      boolean onlyDql, HttpHeaders httpHeaders)
      throws Exception {
    SqlNodeAndOptions sqlNodeAndOptions;
    try {
      sqlNodeAndOptions = RequestUtils.parseQuery(sqlRequestJson.get(Request.SQL).asText(), sqlRequestJson);
    } catch (Exception e) {
      return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
    }
    PinotSqlType sqlType = sqlNodeAndOptions.getSqlType();
    if (onlyDql && sqlType != PinotSqlType.DQL) {
      return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR,
          new UnsupportedOperationException("Unsupported SQL type - " + sqlType + ", GET API only supports DQL.")));
    }
    switch (sqlType) {
      case DQL:
        try (RequestScope requestStatistics = Tracing.getTracer().createRequestScope()) {
          return _requestHandler.handleRequest(sqlRequestJson, sqlNodeAndOptions, httpRequesterIdentity,
              requestStatistics, httpHeaders);
        }
      case DML:
        Map<String, String> headers = new HashMap<>();
        httpRequesterIdentity.getHttpHeaders().entries()
            .forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        return _sqlQueryExecutor.executeDMLStatement(sqlNodeAndOptions, headers);
      default:
        return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR,
            new UnsupportedOperationException("Unsupported SQL type - " + sqlType)));
    }
  }

  private static HttpRequesterIdentity makeHttpIdentity(org.glassfish.grizzly.http.server.Request context) {
    Multimap<String, String> headers = ArrayListMultimap.create();
    context.getHeaderNames().forEach(key -> context.getHeaders(key).forEach(value -> headers.put(key, value)));

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);
    identity.setEndpointUrl(context.getRequestURL().toString());

    return identity;
  }
}
