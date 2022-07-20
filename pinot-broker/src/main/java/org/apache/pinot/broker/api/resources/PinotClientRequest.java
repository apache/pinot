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
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
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

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/sql")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  public void processSqlQueryGet(@ApiParam(value = "Query", required = true) @QueryParam("sql") String query,
      @ApiParam(value = "Trace enabled") @QueryParam(Request.TRACE) String traceEnabled,
      @ApiParam(value = "Debug options") @QueryParam(Request.DEBUG_OPTIONS) String debugOptions,
      @Suspended AsyncResponse asyncResponse, @Context org.glassfish.grizzly.http.server.Request requestContext) {
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
      BrokerResponse brokerResponse = executeSqlQuery(requestJson, makeHttpIdentity(requestContext), true);
      asyncResponse.resume(brokerResponse.toJsonString());
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
  public void processSqlQueryPost(String query, @Suspended AsyncResponse asyncResponse,
      @Context org.glassfish.grizzly.http.server.Request requestContext) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      if (!requestJson.has(Request.SQL)) {
        throw new IllegalStateException("Payload is missing the query string field 'sql'");
      }
      String queryOptions = constructSqlQueryOptions();
      // the only query options as of now are sql related. do not allow any custom query options in sql endpoint
      ObjectNode sqlRequestJson = ((ObjectNode) requestJson).put(Request.QUERY_OPTIONS, queryOptions);
      BrokerResponse brokerResponse = executeSqlQuery(sqlRequestJson, makeHttpIdentity(requestContext), false);
      asyncResponse.resume(brokerResponse.toJsonString());
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing POST request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1L);
      asyncResponse.resume(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  private BrokerResponse executeSqlQuery(ObjectNode sqlRequestJson, HttpRequesterIdentity httpRequesterIdentity,
      boolean onlyDql)
      throws Exception {
    SqlNodeAndOptions sqlNodeAndOptions;
    try {
      sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sqlRequestJson.get(Request.SQL).asText());
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
          return _requestHandler.handleRequest(sqlRequestJson, httpRequesterIdentity, requestStatistics);
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

  // TODO: Remove the SQL query options after releasing 0.11.0
  private String constructSqlQueryOptions() {
    return Request.QueryOptionKey.GROUP_BY_MODE + "=" + Request.SQL + ";" + Request.QueryOptionKey.RESPONSE_FORMAT + "="
        + Request.SQL;
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
