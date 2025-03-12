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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
import javax.ws.rs.core.StreamingOutput;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Controller.PINOT_QUERY_ERROR_CODE_HEADER;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Query", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
    description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")))
@Path("/")
public class PinotClientRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequest.class);

  @Inject
  PinotConfiguration _brokerConf;

  @Inject
  SqlQueryExecutor _sqlQueryExecutor;

  @Inject
  private BrokerRequestHandler _requestHandler;

  @Inject
  private BrokerMetrics _brokerMetrics;

  @Inject
  private Executor _executor;

  @Inject
  private HttpClientConnectionManager _httpConnMgr;

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
      @Suspended AsyncResponse asyncResponse, @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      ObjectNode requestJson = JsonUtils.newObjectNode();
      requestJson.put(Request.SQL, query);
      if (traceEnabled != null) {
        requestJson.put(Request.TRACE, traceEnabled);
      }
      BrokerResponse brokerResponse = executeSqlQuery(requestJson, makeHttpIdentity(requestContext), true, httpHeaders);
      asyncResponse.resume(getPinotQueryResponse(brokerResponse));
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
      @ApiParam(value = "Return a cursor instead of complete result set") @QueryParam("getCursor")
      @DefaultValue("false") boolean getCursor,
      @ApiParam(value = "Number of rows to fetch. Applicable only when getCursor is true") @QueryParam("numRows")
      @DefaultValue("0") int numRows,
      @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      if (!requestJson.has(Request.SQL)) {
        throw new IllegalStateException("Payload is missing the query string field 'sql'");
      }
      BrokerResponse brokerResponse =
          executeSqlQuery((ObjectNode) requestJson, makeHttpIdentity(requestContext), false, httpHeaders, false,
              getCursor, numRows);
      asyncResponse.resume(getPinotQueryResponse(brokerResponse));
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

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query")
  @ApiOperation(value = "Querying pinot using MultiStage Query Engine")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public void processSqlWithMultiStageQueryEngineGet(
      @ApiParam(value = "Query", required = true) @QueryParam("sql") String query,
      @Suspended AsyncResponse asyncResponse, @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      ObjectNode requestJson = JsonUtils.newObjectNode();
      requestJson.put(Request.SQL, query);
      BrokerResponse brokerResponse =
          executeSqlQuery(requestJson, makeHttpIdentity(requestContext), true, httpHeaders, true);
      asyncResponse.resume(getPinotQueryResponse(brokerResponse));
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
  @Path("query")
  @ApiOperation(value = "Querying pinot using MultiStage Query Engine")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public void processSqlWithMultiStageQueryEnginePost(String query, @Suspended AsyncResponse asyncResponse,
      @ApiParam(value = "Return a cursor instead of complete result set") @QueryParam("getCursor")
      @DefaultValue("false") boolean getCursor,
      @ApiParam(value = "Number of rows to fetch. Applicable only getCursor is true") @QueryParam("numRows")
      @DefaultValue("0") int numRows,
      @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      if (!requestJson.has(Request.SQL)) {
        throw new IllegalStateException("Payload is missing the query string field 'sql'");
      }
      BrokerResponse brokerResponse =
          executeSqlQuery((ObjectNode) requestJson, makeHttpIdentity(requestContext), false, httpHeaders, true,
              getCursor, numRows);
      asyncResponse.resume(getPinotQueryResponse(brokerResponse));
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

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("timeseries/api/v1/query_range")
  @ApiOperation(value = "Prometheus Compatible API for Pinot's Time Series Engine")
  @ManualAuthorization
  public void processTimeSeriesQueryEngine(@Suspended AsyncResponse asyncResponse,
      @QueryParam("language") String language,
      @Context org.glassfish.grizzly.http.server.Request requestCtx,
      @Context HttpHeaders httpHeaders) {
    try {
      try (RequestScope requestContext = Tracing.getTracer().createRequestScope()) {
        String queryString = requestCtx.getQueryString();
        PinotBrokerTimeSeriesResponse response = executeTimeSeriesQuery(language, queryString, requestContext);
        if (response.getErrorType() != null && !response.getErrorType().isEmpty()) {
          asyncResponse.resume(Response.serverError().entity(response).build());
          return;
        }
        asyncResponse.resume(response);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1L);
      asyncResponse.resume(Response.serverError().entity(
              new PinotBrokerTimeSeriesResponse("error", null, e.getClass().getSimpleName(), e.getMessage()))
          .build());
    }
  }

  @GET
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("timeseries/api/v1/query")
  @ApiOperation(value = "Prometheus Compatible API for Instant Queries")
  @ManualAuthorization
  public void processTimeSeriesInstantQuery(@Suspended AsyncResponse asyncResponse,
      @Context org.glassfish.grizzly.http.server.Request requestCtx,
      @Context HttpHeaders httpHeaders) {
    // TODO: Not implemented yet.
    asyncResponse.resume(Response.ok().entity("{}").build());
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query/compare")
  @ApiOperation(value = "Query Pinot using both the single-stage query engine and the multi-stage query engine and "
      + "compare the results. The 'sql' field should be set in the request JSON to run the same query on both the "
      + "query engines. Set '" + Request.SQL_V1 + "' and '" + Request.SQL_V2 + "' if the query needs to be adapted for "
      + "the two query engines.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query result comparison response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @ManualAuthorization
  public void processSqlQueryWithBothEnginesAndCompareResults(String query, @Suspended AsyncResponse asyncResponse,
      @Context org.glassfish.grizzly.http.server.Request requestContext,
      @Context HttpHeaders httpHeaders) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(query);
      String v1Query;
      String v2Query;

      if (!requestJson.has(Request.SQL)) {
        if (!requestJson.has(Request.SQL_V1) || !requestJson.has(Request.SQL_V2)) {
          throw new IllegalStateException("Payload should either contain the query string field '" + Request.SQL + "' "
              + "or both of '" + Request.SQL_V1 + "' and '" + Request.SQL_V2 + "'");
        } else {
          v1Query = requestJson.get(Request.SQL_V1).asText();
          v2Query = requestJson.get(Request.SQL_V2).asText();
        }
      } else {
        v1Query = requestJson.has(Request.SQL_V1) ? requestJson.get(Request.SQL_V1).asText()
            : requestJson.get(Request.SQL).asText();
        v2Query = requestJson.has(Request.SQL_V2) ? requestJson.get(Request.SQL_V2).asText()
            : requestJson.get(Request.SQL).asText();
      }

      ObjectNode v1RequestJson = requestJson.deepCopy();
      v1RequestJson.put(Request.SQL, v1Query);
      CompletableFuture<BrokerResponse> v1Response = CompletableFuture.supplyAsync(
          () -> {
            try {
              return executeSqlQuery(v1RequestJson, makeHttpIdentity(requestContext), true, httpHeaders, false);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          },
          _executor
      );

      ObjectNode v2RequestJson = requestJson.deepCopy();
      v2RequestJson.put(Request.SQL, v2Query);
      CompletableFuture<BrokerResponse> v2Response = CompletableFuture.supplyAsync(
          () -> {
            try {
              return executeSqlQuery(v2RequestJson, makeHttpIdentity(requestContext), true, httpHeaders, true);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          },
          _executor
      );

      CompletableFuture.allOf(v1Response, v2Response).join();

      asyncResponse.resume(getPinotQueryComparisonResponse(v1Query, v1Response.get(), v2Response.get()));
    } catch (WebApplicationException wae) {
      asyncResponse.resume(wae);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing request", e);
      asyncResponse.resume(
          new WebApplicationException(e,
              Response
                  .status(Response.Status.INTERNAL_SERVER_ERROR)
                  .entity(e.getMessage())
                  .build()));
    }
  }

  @DELETE
  @Path("query/{id}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.CANCEL_QUERY)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Cancel a query as identified by the id", notes = "No effect if no query exists for the "
      + "given id on the requested broker. Query may continue to run for a short while after calling cancel as "
      + "it's done in a non-blocking manner. The cancel method can be called multiple times.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Query not found on the requested broker")
  })
  public String cancelQuery(
      @ApiParam(value = "Query id", required = true) @PathParam("id") String id,
      @ApiParam(value = "Determines is query id is internal or provided by the client") @QueryParam("client")
      @DefaultValue("false") boolean isClient,
      @ApiParam(value = "Timeout for servers to respond the cancel request") @QueryParam("timeoutMs")
      @DefaultValue("3000") int timeoutMs,
      @ApiParam(value = "Return server responses for troubleshooting") @QueryParam("verbose") @DefaultValue("false")
      boolean verbose) {
    try (QueryThreadContext.CloseableContext closeMe = QueryThreadContext.open()) {
      Map<String, Integer> serverResponses = verbose ? new HashMap<>() : null;
      if (isClient) {
        long reqId = _requestHandler.getRequestIdByClientId(id).orElse(-1L);
        QueryThreadContext.setIds(reqId, id);
        if (_requestHandler.cancelQueryByClientId(id, timeoutMs, _executor, _httpConnMgr, serverResponses)) {
          String resp = "Cancelled client query: " + id;
          if (verbose) {
            resp += " with responses from servers: " + serverResponses;
          }
          return resp;
        }
      } else {
        long reqId = Long.parseLong(id);
        if (_requestHandler.cancelQuery(reqId, timeoutMs, _executor, _httpConnMgr, serverResponses)) {
          QueryThreadContext.setIds(reqId, id);
          String resp = "Cancelled query: " + id;
          if (verbose) {
            resp += " with responses from servers: " + serverResponses;
          }
          return resp;
        }
      }
    } catch (NumberFormatException e) {
      Response.status(Response.Status.BAD_REQUEST).entity(String.format("Invalid internal query id: %s", id));
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Failed to cancel query: %s on the broker due to error: %s", id, e.getMessage()))
          .build());
    }
    throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND).entity(String.format("Query: %s not found on the broker", id))
            .build());
  }

  @GET
  @Path("queries")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_RUNNING_QUERY)
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
    return executeSqlQuery(sqlRequestJson, httpRequesterIdentity, onlyDql, httpHeaders, false);
  }

  private BrokerResponse executeSqlQuery(ObjectNode sqlRequestJson, HttpRequesterIdentity httpRequesterIdentity,
      boolean onlyDql, HttpHeaders httpHeaders, boolean forceUseMultiStage)
      throws Exception {
    return executeSqlQuery(sqlRequestJson, httpRequesterIdentity, onlyDql, httpHeaders, forceUseMultiStage, false, 0);
  }

  private BrokerResponse executeSqlQuery(ObjectNode sqlRequestJson, HttpRequesterIdentity httpRequesterIdentity,
      boolean onlyDql, HttpHeaders httpHeaders, boolean forceUseMultiStage, boolean getCursor, int numRows)
      throws Exception {
    long requestArrivalTimeMs = System.currentTimeMillis();
    SqlNodeAndOptions sqlNodeAndOptions;
    try {
      sqlNodeAndOptions = RequestUtils.parseQuery(sqlRequestJson.get(Request.SQL).asText(), sqlRequestJson);
    } catch (Exception e) {
      return new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage());
    }
    if (forceUseMultiStage) {
      sqlNodeAndOptions.setExtraOptions(ImmutableMap.of(Request.QueryOptionKey.USE_MULTISTAGE_ENGINE, "true"));
    }
    if (getCursor) {
      if (numRows == 0) {
        numRows = _brokerConf.getProperty(CommonConstants.CursorConfigs.CURSOR_FETCH_ROWS,
            CommonConstants.CursorConfigs.DEFAULT_CURSOR_FETCH_ROWS);
      }
      sqlNodeAndOptions.setExtraOptions(
          ImmutableMap.of(Request.QueryOptionKey.GET_CURSOR, "true", Request.QueryOptionKey.CURSOR_NUM_ROWS,
              Integer.toString(numRows)));
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.CURSOR_QUERIES_GLOBAL, 1);
    }
    PinotSqlType sqlType = sqlNodeAndOptions.getSqlType();
    if (onlyDql && sqlType != PinotSqlType.DQL) {
      return new BrokerResponseNative(QueryErrorCode.SQL_PARSING,
          "Unsupported SQL type - " + sqlType + ", this API only supports DQL.");
    }
    switch (sqlType) {
      case DQL:
        try (RequestScope requestContext = Tracing.getTracer().createRequestScope()) {
          requestContext.setRequestArrivalTimeMillis(requestArrivalTimeMs);
          return _requestHandler.handleRequest(sqlRequestJson, sqlNodeAndOptions, httpRequesterIdentity, requestContext,
              httpHeaders);
        } catch (Exception e) {
          LOGGER.error("Error handling DQL request:\n{}", sqlRequestJson, e);
          throw e;
        }
      case DML:
        try {
          Map<String, String> headers = new HashMap<>();
          httpRequesterIdentity.getHttpHeaders().entries()
              .forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
          return _sqlQueryExecutor.executeDMLStatement(sqlNodeAndOptions, headers);
        } catch (Exception e) {
          LOGGER.error("Error handling DML request:\n{}", sqlRequestJson, e);
          throw e;
        }
      default:
        return new BrokerResponseNative(QueryErrorCode.SQL_PARSING, "Unsupported SQL type - " + sqlType);
    }
  }

  private PinotBrokerTimeSeriesResponse executeTimeSeriesQuery(String language, String queryString,
      RequestContext requestContext) {
    return _requestHandler.handleTimeSeriesRequest(language, queryString, requestContext);
  }

  public static HttpRequesterIdentity makeHttpIdentity(org.glassfish.grizzly.http.server.Request context) {
    Multimap<String, String> headers = ArrayListMultimap.create();
    context.getHeaderNames().forEach(key -> context.getHeaders(key).forEach(value -> headers.put(key, value)));

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);
    identity.setEndpointUrl(context.getRequestURL().toString());

    return identity;
  }

  /**
   * Generate Response object from the BrokerResponse object with 'X-Pinot-Error-Code' header value
   *
   * If the query is successful the 'X-Pinot-Error-Code' header value is set to -1
   * otherwise, the first error code of the broker response exception array will become the header value
   *
   * @param brokerResponse
   * @return Response
   * @throws Exception
   */
  @VisibleForTesting
  public static Response getPinotQueryResponse(BrokerResponse brokerResponse)
      throws Exception {
    int queryErrorCodeHeaderValue = -1; // default value of the header.
    List<QueryProcessingException> exceptions = brokerResponse.getExceptions();
    if (!exceptions.isEmpty()) {
      // set the header value as first exception error code value.
      queryErrorCodeHeaderValue = exceptions.get(0).getErrorCode();
    }

    // returning the Response with OK status and header value.
    return Response.ok()
        .header(PINOT_QUERY_ERROR_CODE_HEADER, queryErrorCodeHeaderValue)
        .entity((StreamingOutput) brokerResponse::toOutputStream).type(MediaType.APPLICATION_JSON)
        .build();
  }

  @VisibleForTesting
  static Response getPinotQueryComparisonResponse(String query, BrokerResponse v1Response, BrokerResponse v2Response) {
    ObjectNode response = JsonUtils.newObjectNode();
    response.set("v1Response", JsonUtils.objectToJsonNode(v1Response));
    response.set("v2Response", JsonUtils.objectToJsonNode(v2Response));
    response.set("comparisonAnalysis", JsonUtils.objectToJsonNode(
        analyzeQueryResultDifferences(query, v1Response, v2Response)));

    return Response.ok()
        .header(PINOT_QUERY_ERROR_CODE_HEADER, -1)
        .entity(response).type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Given a query and the responses from the single-stage and multi-stage query engines, analyzes the differences
   * between the responses and returns a list of differences. Currently, the method only compares the column names,
   * column types, number of rows in the result set, and the aggregation values for aggregation-only queries.
   *
   * TODO: Add more comparison logic for different query types. This would require handling edge cases with group
   *       trimming, non-deterministic results for order by queries with limits etc.
   */
  private static List<String> analyzeQueryResultDifferences(String query, BrokerResponse v1Response,
      BrokerResponse v2Response) {
    List<String> differences = new ArrayList<>();

    if (v1Response.getExceptionsSize() != 0 || v2Response.getExceptionsSize() != 0) {
      differences.add("Exception encountered while running the query on one or both query engines");
      return differences;
    }

    if (v1Response.getResultTable() == null && v2Response.getResultTable() == null) {
      return differences;
    }

    if (v1Response.getResultTable() == null) {
      differences.add("v1 response has an empty result table");
      return differences;
    }

    if (v2Response.getResultTable() == null) {
      differences.add("v2 response has an empty result table");
      return differences;
    }

    DataSchema.ColumnDataType[] v1ResponseTypes = v1Response.getResultTable().getDataSchema().getColumnDataTypes();
    DataSchema.ColumnDataType[] v2ResponseTypes = v2Response.getResultTable().getDataSchema().getColumnDataTypes();

    if (v1ResponseTypes.length != v2ResponseTypes.length) {
      differences.add("Mismatch in number of columns returned. v1: " + v1ResponseTypes.length
          + ", v2: " + v2ResponseTypes.length);
      return differences;
    }

    String[] v1ColumnNames = v1Response.getResultTable().getDataSchema().getColumnNames();
    String[] v2ColumnNames = v2Response.getResultTable().getDataSchema().getColumnNames();
    for (int i = 0; i < v1ResponseTypes.length; i++) {
      if (v1ResponseTypes[i] != v2ResponseTypes[i]) {
        String columnName = v1ColumnNames[i].equals(v2ColumnNames[i])
            ? v1ColumnNames[i]
            : v1ColumnNames[i] + " / " + v2ColumnNames[i];
        differences.add("Mismatch in column data type for column with name " + columnName
            + ". v1 type: " + v1ResponseTypes[i] + ", v2 type: " + v2ResponseTypes[i]);
      }
    }

    if (v1Response.getNumRowsResultSet() != v2Response.getNumRowsResultSet()) {
      differences.add("Mismatch in number of rows returned. v1: " + v1Response.getNumRowsResultSet()
          + ", v2: " + v2Response.getNumRowsResultSet());
      return differences;
    }

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    if (QueryContextUtils.isAggregationQuery(queryContext) && queryContext.getGroupByExpressions() == null) {
      // Aggregation-only query with a single row output
      for (int i = 0; i < v1ColumnNames.length; i++) {
        if (!Objects.equals(v1Response.getResultTable().getRows().get(0)[i],
            v2Response.getResultTable().getRows().get(0)[i])) {
          differences.add("Mismatch in aggregation value for " + v1ColumnNames[i]
              + ". v1 value: " + v1Response.getResultTable().getRows().get(0)[i]
              + ", v2 value: " + v2Response.getResultTable().getRows().get(0)[i]);
        }
      }
    }

    return differences;
  }
}
