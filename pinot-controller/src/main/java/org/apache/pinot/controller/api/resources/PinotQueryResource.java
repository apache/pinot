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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.ApiOperation;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.IOUtils;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.parser.utils.ParserUtils;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.tsdb.planner.TimeSeriesQueryEnvironment;
import org.apache.pinot.tsdb.planner.TimeSeriesTableMetadataProvider;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PinotQueryResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotQueryResource.class);

  @Inject
  SqlQueryExecutor _sqlQueryExecutor;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  AccessControlFactory _accessControlFactory;

  @Inject
  ControllerConf _controllerConf;

  @POST
  @Path("sql")
  @ManualAuthorization // performed by broker
  public StreamingOutput handlePostSql(String requestJsonStr, @Context HttpHeaders httpHeaders) {
    JsonNode requestJson;
    try {
      requestJson = JsonUtils.stringToJsonNode(requestJsonStr);
    } catch (Exception e) {
      return constructQueryExceptionResponse(QueryErrorCode.JSON_PARSING, e.getMessage());
    }
    if (!requestJson.has("sql")) {
      return constructQueryExceptionResponse(QueryErrorCode.JSON_PARSING,
        "JSON Payload is missing the query string field 'sql'");
    }
    String sqlQuery = requestJson.get("sql").asText();
    String traceEnabled = "false";
    if (requestJson.has("trace")) {
      traceEnabled = requestJson.get("trace").toString();
    }
    String queryOptions = null;
    if (requestJson.has("queryOptions")) {
      queryOptions = requestJson.get("queryOptions").asText();
    }
    return executeSqlQueryCatching(httpHeaders, sqlQuery, traceEnabled, queryOptions);
  }

  @GET
  @Path("sql")
  @ManualAuthorization
  public StreamingOutput handleGetSql(@QueryParam("sql") String sqlQuery, @QueryParam("trace") String traceEnabled,
    @QueryParam("queryOptions") String queryOptions, @Context HttpHeaders httpHeaders) {
    return executeSqlQueryCatching(httpHeaders, sqlQuery, traceEnabled, queryOptions);
  }

  @GET
  @Path("/timeseries/api/v1/query_range")
  @ManualAuthorization
  @ApiOperation(value = "Prometheus Compatible API for Pinot's Time Series Engine")
  public StreamingOutput handleTimeSeriesQueryRange(@QueryParam("language") String language,
    @QueryParam("query") String query, @QueryParam("start") String start, @QueryParam("end") String end,
    @QueryParam("step") String step, @Context HttpHeaders httpHeaders) {
    return executeTimeSeriesQueryCatching(httpHeaders, language, query, start, end, step);
  }

  @POST
  @Path("validateMultiStageQuery")
  public MultiStageQueryValidationResponse validateMultiStageQuery(String requestJsonStr,
      @Context HttpHeaders httpHeaders) {
    JsonNode requestJson;
    try {
      requestJson = JsonUtils.stringToJsonNode(requestJsonStr);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while parsing request {}", e.getMessage());
      return new MultiStageQueryValidationResponse(false, "Failed to parse request JSON: " + e.getMessage(), null);
    }
    if (!requestJson.has("sql")) {
      return new MultiStageQueryValidationResponse(false, "JSON Payload is missing the query string field 'sql'", null);
    }
    String sqlQuery = requestJson.get("sql").asText();
    Map<String, String> queryOptionsMap = RequestUtils.parseQuery(sqlQuery).getOptions();
    String database = DatabaseUtils.extractDatabaseFromQueryRequest(queryOptionsMap, httpHeaders);
    try (QueryEnvironment.CompiledQuery compiledQuery = new QueryEnvironment(database,
        _pinotHelixResourceManager.getTableCache(), null).compile(sqlQuery)) {
      return new MultiStageQueryValidationResponse(true, null, null);
    } catch (QueryException e) {
      LOGGER.info("Caught exception while compiling multi-stage query: {}", e.getMessage());
      return new MultiStageQueryValidationResponse(false, e.getMessage(), e.getErrorCode());
    }
  }

  public static class MultiStageQueryValidationResponse {
    private final boolean _compiledSuccessfully;
    private final String _errorMessage;
    private final QueryErrorCode _errorCode;

    public MultiStageQueryValidationResponse(boolean compiledSuccessfully, @Nullable String errorMessage,
        @Nullable QueryErrorCode errorCode) {
      _compiledSuccessfully = compiledSuccessfully;
      _errorMessage = errorMessage;
      _errorCode = errorCode;
    }

    public boolean isCompiledSuccessfully() {
      return _compiledSuccessfully;
    }

    @Nullable
    public String getErrorMessage() {
      return _errorMessage;
    }

    @Nullable
    public QueryErrorCode getErrorCode() {
      return _errorCode;
    }
  }

  private StreamingOutput executeSqlQueryCatching(HttpHeaders httpHeaders, String sqlQuery, String traceEnabled,
      String queryOptions) {
    try {
      return executeSqlQuery(httpHeaders, sqlQuery, traceEnabled, queryOptions);
    } catch (ProcessingException pe) {
      LOGGER.error("Caught exception while processing get request {}", pe.getMessage());
      return constructQueryExceptionResponse(QueryErrorCode.fromErrorCode(pe.getErrorCode()), pe.getMessage());
    } catch (QueryException ex) {
      LOGGER.warn("Caught exception while processing get request {}", ex.getMessage());
      return constructQueryExceptionResponse(ex.getErrorCode(), ex.getMessage());
    } catch (WebApplicationException wae) {
      LOGGER.error("Caught exception while processing get request", wae);
      throw wae;
    } catch (Exception e) {
      LOGGER.error("Caught unknown exception while processing get request", e);
      return constructQueryExceptionResponse(QueryErrorCode.INTERNAL, e.getMessage());
    }
  }

  private StreamingOutput executeSqlQuery(@Context HttpHeaders httpHeaders, String sqlQuery, String traceEnabled,
      @Nullable String queryOptions)
      throws Exception {
    LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, sqlQuery);
    SqlNodeAndOptions sqlNodeAndOptions;
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery);
    Map<String, String> options = sqlNodeAndOptions.getOptions();
    if (queryOptions != null) {
      Map<String, String> optionsFromString = RequestUtils.getOptionsFromString(queryOptions);
      sqlNodeAndOptions.setExtraOptions(optionsFromString);
    }

    // Determine which engine to used based on query options.
    boolean isMse = Boolean.parseBoolean(options.get(QueryOptionKey.USE_MULTISTAGE_ENGINE));
    boolean isMseEnabled = _controllerConf.getProperty(
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_ENABLED);
    if (isMse && !isMseEnabled) {
      throw QueryErrorCode.INTERNAL.asException("V2 Multi-Stage query engine not enabled.");
    }

    PinotSqlType sqlType = sqlNodeAndOptions.getSqlType();
    switch (sqlType) {
      case DQL:
        return isMse
            ? getMultiStageQueryResponse(sqlQuery, queryOptions, httpHeaders, traceEnabled)
            : getQueryResponse(sqlQuery, sqlNodeAndOptions.getSqlNode(), traceEnabled, queryOptions, httpHeaders);
      case DML:
        Map<String, String> headers = extractHeaders(httpHeaders);
        return output -> {
          try (OutputStream os = output) {
            _sqlQueryExecutor.executeDMLStatement(sqlNodeAndOptions, headers).toOutputStream(os);
          }
        };
      default:
        throw QueryErrorCode.INTERNAL.asException("Unsupported SQL type - " + sqlType);
    }
  }

  private StreamingOutput getMultiStageQueryResponse(String query, String queryOptions, HttpHeaders httpHeaders,
      String traceEnabled) {

    // Validate data access
    // we don't have a cross table access control rule so only ADMIN can make request to multi-stage engine.
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasAccess(AccessType.READ, httpHeaders, "/sql")) {
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }

    Map<String, String> queryOptionsMap = RequestUtils.parseQuery(query).getOptions();
    if (queryOptions != null) {
      queryOptionsMap.putAll(RequestUtils.getOptionsFromString(queryOptions));
    }
    String database = DatabaseUtils.extractDatabaseFromQueryRequest(queryOptionsMap, httpHeaders);
    List<String> tableNames = getTableNames(query, database);
    List<String> instanceIds = getInstanceIds(query, tableNames, database);
    String instanceId = selectRandomInstanceId(instanceIds);
    return sendRequestToBroker(query, instanceId, traceEnabled, queryOptions, httpHeaders);
  }

  private List<String> getTableNames(String query, String database) {
    QueryEnvironment queryEnvironment =
        new QueryEnvironment(database, _pinotHelixResourceManager.getTableCache(), null);
    List<String> tableNames;

    try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(query)) {
      tableNames = new ArrayList<>(compiledQuery.getTableNames());
    } catch (QueryException e) {
      if (e.getErrorCode() != QueryErrorCode.UNKNOWN) {
        throw e;
      } else {
        throw new QueryException(QueryErrorCode.SQL_PARSING, e);
      }
    } catch (Exception e) {
      throw QueryErrorCode.SQL_PARSING.asException("Unable to find table for this query", e);
    }
    return tableNames;
  }

  private List<String> getInstanceIds(String query, List<String> tableNames, String database) {
    List<String> instanceIds;
    if (!tableNames.isEmpty()) {
      // find the unions of all the broker tenant tags of the queried tables.
      Set<String> brokerTenantsUnion = getBrokerTenants(tableNames, database);
      if (brokerTenantsUnion.isEmpty()) {
        throw QueryErrorCode.BROKER_REQUEST_SEND.asException("Unable to find broker tenant for tables: " + tableNames);
      }
      instanceIds = findCommonBrokerInstances(brokerTenantsUnion);
      if (instanceIds.isEmpty()) {
        // No common broker found for table tenants
        LOGGER.error("Unable to find a common broker instance for table tenants. Tables: {}, Tenants: {}", tableNames,
            brokerTenantsUnion);
        throw QueryErrorCode.BROKER_RESOURCE_MISSING.asException(
            "Unable to find a common broker instance for table tenants. Tables: " + tableNames + ", Tenants: "
                + brokerTenantsUnion);
      }
    } else {
      // TODO fail these queries going forward. Added this logic to take care of tautologies like BETWEEN 0 and -1.
      instanceIds = _pinotHelixResourceManager.getAllBrokerInstances();
      LOGGER.error("Unable to find table name from SQL {} thus dispatching to random broker.", query);
    }
    return instanceIds;
  }

  private StreamingOutput getQueryResponse(String query, @Nullable SqlNode sqlNode, String traceEnabled,
      String queryOptions, HttpHeaders httpHeaders) {
    // Get resource table name.
    String tableName;
    Map<String, String> queryOptionsMap = RequestUtils.parseQuery(query).getOptions();
    if (queryOptions != null) {
      queryOptionsMap.putAll(RequestUtils.getOptionsFromString(queryOptions));
    }
    String database;
    try {
      database = DatabaseUtils.extractDatabaseFromQueryRequest(queryOptionsMap, httpHeaders);
    } catch (DatabaseConflictException e) {
      throw QueryErrorCode.QUERY_VALIDATION.asException(e);
    }
    try {
      String inputTableName =
          sqlNode != null ? RequestUtils.getTableNames(CalciteSqlParser.compileSqlNodeToPinotQuery(sqlNode)).iterator()
              .next() : CalciteSqlCompiler.compileToBrokerRequest(query).getQuerySource().getTableName();
      tableName = _pinotHelixResourceManager.getActualTableName(inputTableName, database);
    } catch (Exception e) {
      LOGGER.error("Caught exception while compiling query: {}", query, e);

      // Check if the query is a v2 supported query
      if (ParserUtils.canCompileWithMultiStageEngine(query, database, _pinotHelixResourceManager.getTableCache())) {
        throw QueryErrorCode.SQL_PARSING.asException(
            "It seems that the query is only supported by the multi-stage query engine, please retry the query using "
                + "the multi-stage query engine "
                + "(https://docs.pinot.apache.org/developers/advanced/v2-multi-stage-query-engine)");
      } else {
        throw QueryErrorCode.SQL_PARSING.asException(e);
      }
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Validate data access
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasAccess(rawTableName, AccessType.READ, httpHeaders, Actions.Table.QUERY)) {
      throw QueryErrorCode.ACCESS_DENIED.asException();
    }

    // Get brokers for the resource table.
    List<String> instanceIds = _pinotHelixResourceManager.getBrokerInstancesFor(rawTableName);
    String instanceId = selectRandomInstanceId(instanceIds);
    return sendRequestToBroker(query, instanceId, traceEnabled, queryOptions, httpHeaders);
  }

  // given a list of tables, returns the list of tableConfigs
  private Set<String> getBrokerTenants(List<String> tableNames, String database) {
    Set<String> brokerTenants = new HashSet<>(tableNames.size());
    List<String> tablesNotFound = new ArrayList<>(tableNames.size());
    for (String tableName : tableNames) {
      boolean found = false;
      String actualTableName = _pinotHelixResourceManager.getActualTableName(tableName, database);
      if (_pinotHelixResourceManager.hasRealtimeTable(actualTableName)) {
        brokerTenants.add(Objects.requireNonNull(_pinotHelixResourceManager.getRealtimeTableConfig(actualTableName))
            .getTenantConfig().getBroker());
        found = true;
      }
      if (_pinotHelixResourceManager.hasOfflineTable(actualTableName)) {
        brokerTenants.add(Objects.requireNonNull(_pinotHelixResourceManager.getOfflineTableConfig(actualTableName))
            .getTenantConfig().getBroker());
        found = true;
      }

      if (!found) {
        actualTableName = _pinotHelixResourceManager.getActualLogicalTableName(tableName, database);
        LogicalTableConfig logicalTableConfig =
            _pinotHelixResourceManager.getLogicalTableConfig(actualTableName);
        if (logicalTableConfig != null) {
          brokerTenants.add(logicalTableConfig.getBrokerTenant());
          found = true;
        }
      }

      if (!found) {
        tablesNotFound.add(tableName);
      }
    }

    if (!tablesNotFound.isEmpty()) {
      throw QueryErrorCode.TABLE_DOES_NOT_EXIST.asException(
          "Unable to find table in cluster, table does not exist for tables: " + tablesNotFound);
    }
    return brokerTenants;
  }

  private String selectRandomInstanceId(List<String> instanceIds) {
    if (instanceIds.isEmpty()) {
      throw QueryErrorCode.BROKER_RESOURCE_MISSING.asException("No broker found for query");
    }

    instanceIds.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
    if (instanceIds.isEmpty()) {
      throw QueryErrorCode.BROKER_INSTANCE_MISSING.asException("No online broker found for query");
    }

    // Send query to a random broker.
    return instanceIds.get(ThreadLocalRandom.current().nextInt(instanceIds.size()));
  }

  private List<String> findCommonBrokerInstances(Set<String> brokerTenants) {
    Stream<InstanceConfig> brokerInstanceConfigs = _pinotHelixResourceManager.getAllBrokerInstanceConfigs().stream();
    for (String brokerTenant : brokerTenants) {
      brokerInstanceConfigs = brokerInstanceConfigs.filter(
          instanceConfig -> instanceConfig.containsTag(TagNameUtils.getBrokerTagForTenant(brokerTenant)));
    }
    return brokerInstanceConfigs.map(InstanceConfig::getInstanceName).collect(Collectors.toList());
  }

  private StreamingOutput sendRequestToBroker(String query, String instanceId, String traceEnabled, String queryOptions,
      HttpHeaders httpHeaders) {
    InstanceConfig instanceConfig = getInstanceConfig(instanceId);
    String hostName = getHost(instanceConfig);
    String protocol = _controllerConf.getControllerBrokerProtocol();
    int port = getPort(instanceConfig);
    String url = getQueryURL(protocol, hostName, port);
    ObjectNode requestJson = getRequestJson(query, traceEnabled, queryOptions);

    // Forward client-supplied headers
    Map<String, String> headers = extractHeaders(httpHeaders);

    return sendRequestRaw(url, "POST", query, requestJson, headers);
  }

  private ObjectNode getRequestJson(String query, String traceEnabled, String queryOptions) {
    ObjectNode requestJson = JsonUtils.newObjectNode();
    requestJson.put("sql", query);
    if (traceEnabled != null && !traceEnabled.isEmpty()) {
      requestJson.put("trace", traceEnabled);
    }
    if (queryOptions != null && !queryOptions.isEmpty()) {
      requestJson.put("queryOptions", queryOptions);
    }
    return requestJson;
  }

  private String getQueryURL(String protocol, String hostName, int port) {
    return String.format("%s://%s:%d/query/sql", protocol, hostName, port);
  }

  public void sendRequestRaw(String urlStr, String method, String requestStr, Map<String, String> headers,
    OutputStream outputStream) {
    HttpURLConnection conn = null;
    try {
      LOGGER.info("Sending {} request to: {}", method, urlStr);
      final URL url = new URL(urlStr);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(method);
      conn.setRequestProperty("Accept-Encoding", "gzip");
      conn.setRequestProperty("http.keepAlive", "true");
      conn.setRequestProperty("default", "true");

      // Set headers
      if (headers != null) {
        headers.forEach(conn::setRequestProperty);
      }

      // Handle POST request body
      if ("POST".equalsIgnoreCase(method)) {
        conn.setDoOutput(true);
        final byte[] requestBytes = requestStr.getBytes(StandardCharsets.UTF_8);
        conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));

        try (final OutputStream os = new BufferedOutputStream(conn.getOutputStream())) {
          os.write(requestBytes);
          os.flush();
        }
      }

      final int responseCode = conn.getResponseCode();

      if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
        throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
      } else if (responseCode != HttpURLConnection.HTTP_OK) {
        InputStream errorStream = conn.getErrorStream();
        String errorMessage = errorStream != null ? IOUtils.toString(errorStream, StandardCharsets.UTF_8) : "Unknown";
        throw new IOException("HTTP error code: " + responseCode + ". Root Cause: " + errorMessage);
      }
      IOUtils.copy(conn.getInputStream(), outputStream);
    } catch (final Exception ex) {
      LOGGER.error("Caught exception while sending {} request", method, ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  public StreamingOutput sendRequestRaw(String url, String method, String query, ObjectNode requestJson,
    Map<String, String> headers) {
    return outputStream -> {
      final long startTime = System.currentTimeMillis();
      sendRequestRaw(url, method, requestJson.toString(), headers, outputStream);
      final long queryTime = System.currentTimeMillis() - startTime;
      LOGGER.info("Query: {} Time: {}ms", query, queryTime);
    };
  }

  private static StreamingOutput constructQueryExceptionResponse(QueryErrorCode errorCode, String message) {
    return outputStream -> {
      try (OutputStream os = outputStream) {
        new BrokerResponseNative(errorCode, message).toOutputStream(os);
      }
    };
  }

  private StreamingOutput executeTimeSeriesQueryCatching(HttpHeaders httpHeaders, String language, String query,
    String start, String end, String step) {
    try {
      return executeTimeSeriesQuery(httpHeaders, language, query, start, end, step);
    } catch (ProcessingException pe) {
      LOGGER.error("Caught exception while processing timeseries request {}", pe.getMessage());
      return constructQueryExceptionResponse(QueryErrorCode.fromErrorCode(pe.getErrorCode()), pe.getMessage());
    } catch (QueryException ex) {
      LOGGER.warn("Caught exception while processing timeseries request {}", ex.getMessage());
      return constructQueryExceptionResponse(ex.getErrorCode(), ex.getMessage());
    } catch (WebApplicationException wae) {
      LOGGER.error("Caught exception while processing timeseries request", wae);
      throw wae;
    } catch (Exception e) {
      LOGGER.error("Caught unknown exception while processing timeseries request", e);
      return constructQueryExceptionResponse(QueryErrorCode.INTERNAL, e.getMessage());
    }
  }

  private String retrieveBrokerForTimeSeriesQuery(String query, String language, String start, String end) {
    TimeSeriesLogicalPlanner planner = TimeSeriesQueryEnvironment.buildLogicalPlanner(language, _controllerConf);
    TimeSeriesLogicalPlanResult planResult = planner.plan(
        new RangeTimeSeriesRequest(language, query, Integer.parseInt(start), Long.parseLong(end),
            60L, Duration.ofMinutes(1), 100, 100, ""),
        new TimeSeriesTableMetadataProvider(_pinotHelixResourceManager.getTableCache()));
    String tableName = planner.getTableName(planResult);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    List<String> instanceIds = _pinotHelixResourceManager.getBrokerInstancesFor(rawTableName);
    return selectRandomInstanceId(instanceIds);
  }

  private StreamingOutput executeTimeSeriesQuery(HttpHeaders httpHeaders, String language, String query,
      String start, String end, String step) throws Exception {
    LOGGER.debug("Language: {}, Query: {}, Start: {}, End: {}, Step: {}", language, query, start, end, step);
    String instanceId = retrieveBrokerForTimeSeriesQuery(query, language, start, end);
    return sendTimeSeriesRequestToBroker(language, query, start, end, step, instanceId, httpHeaders);
  }

  private StreamingOutput sendTimeSeriesRequestToBroker(String language, String query, String start, String end,
    String step, String instanceId, HttpHeaders httpHeaders) {
    InstanceConfig instanceConfig = getInstanceConfig(instanceId);
    String hostName = getHost(instanceConfig);
    String protocol = _controllerConf.getControllerBrokerProtocol();
    int port = getPort(instanceConfig);
    String url = getTimeSeriesQueryURL(protocol, hostName, port, language, query, start, end, step);

    // Forward client-supplied headers
    Map<String, String> headers = extractHeaders(httpHeaders);

    return sendRequestRaw(url, "GET", query, JsonUtils.newObjectNode(), headers);
  }

  private String getTimeSeriesQueryURL(String protocol, String hostName, int port, String language, String query,
    String start, String end, String step) {
    try {
      URIBuilder uriBuilder = new URIBuilder().setScheme(protocol).setHost(hostName).setPort(port)
        .setPath("/timeseries/api/v1/query_range").addParameter("language", language);
      // Add optional parameters
      if (query != null && !query.isEmpty()) {
        uriBuilder.addParameter("query", query);
      }
      if (start != null && !start.isEmpty()) {
        uriBuilder.addParameter("start", start);
      }
      if (end != null && !end.isEmpty()) {
        uriBuilder.addParameter("end", end);
      }
      if (step != null && !step.isEmpty()) {
        uriBuilder.addParameter("step", step);
      }
      return uriBuilder.build().toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to build timeseries query URL", e);
    }
  }

  private Map<String, String> extractHeaders(HttpHeaders httpHeaders) {
    return httpHeaders.getRequestHeaders().entrySet().stream()
      .filter(entry -> !entry.getValue().isEmpty())
      .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get(0)));
  }

  private InstanceConfig getInstanceConfig(String instanceId) {
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
    if (instanceConfig == null) {
      LOGGER.error("Instance {} not found", instanceId);
      throw QueryErrorCode.INTERNAL.asException();
    }
    return instanceConfig;
  }

  private String getHost(InstanceConfig instanceConfig) {
    String hostName = instanceConfig.getHostName();
    // Backward-compatible with legacy hostname of format 'Broker_<hostname>'
    if (hostName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
      hostName = hostName.substring(CommonConstants.Helix.BROKER_INSTANCE_PREFIX_LENGTH);
    }
    return hostName;
  }

  private int getPort(InstanceConfig instanceConfig) {
    return _controllerConf.getControllerBrokerPortOverride() > 0 ? _controllerConf.getControllerBrokerPortOverride()
      : Integer.parseInt(instanceConfig.getPort());
  }
}
