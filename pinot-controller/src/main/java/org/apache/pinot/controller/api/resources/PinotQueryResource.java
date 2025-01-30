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
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.exception.QueryException;
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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.exception.QException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
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
      return constructQueryExceptionResponse(QueryException.JSON_PARSING_ERROR_CODE, e.getMessage());
    }
    if (!requestJson.has("sql")) {
      return constructQueryExceptionResponse(QueryException.getException(QueryException.JSON_PARSING_ERROR,
          "JSON Payload is missing the query string field 'sql'"));
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

  private StreamingOutput executeSqlQueryCatching(HttpHeaders httpHeaders, String sqlQuery, String traceEnabled,
      String queryOptions) {
    try {
      return executeSqlQuery(httpHeaders, sqlQuery, traceEnabled, queryOptions);
    } catch (QException e) {
      LOGGER.error("Caught query exception while processing post request", e);
      return constructQueryExceptionResponse(e);
    } catch (ProcessingException pe) {
      LOGGER.error("Caught exception while processing get request {}", pe.getMessage());
      return constructQueryExceptionResponse(pe);
    } catch (WebApplicationException wae) {
      LOGGER.error("Caught exception while processing get request", wae);
      throw wae;
    } catch (Exception e) {
      LOGGER.error("Caught unknown exception while processing get request", e);
      return constructQueryExceptionResponse(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    }
  }

  private StreamingOutput executeSqlQuery(@Context HttpHeaders httpHeaders, String sqlQuery, String traceEnabled,
      @Nullable String queryOptions)
      throws Exception {
    LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, sqlQuery);
    SqlNodeAndOptions sqlNodeAndOptions;
    try {
      sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery);
    } catch (SqlCompilationException ex) {
      throw QueryException.getException(QueryException.SQL_PARSING_ERROR, ex);
    }
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
      throw QueryException.getException(QueryException.INTERNAL_ERROR, "V2 Multi-Stage query engine not enabled.");
    }

    PinotSqlType sqlType = sqlNodeAndOptions.getSqlType();
    switch (sqlType) {
      case DQL:
        return isMse
            ? getMultiStageQueryResponse(sqlQuery, queryOptions, httpHeaders, traceEnabled)
            : getQueryResponse(sqlQuery, sqlNodeAndOptions.getSqlNode(), traceEnabled, queryOptions, httpHeaders);
      case DML:
        Map<String, String> headers = httpHeaders.getRequestHeaders().entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .map(entry -> Pair.of(entry.getKey(), entry.getValue().get(0)))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return output -> {
          try (OutputStream os = output) {
            _sqlQueryExecutor.executeDMLStatement(sqlNodeAndOptions, headers).toOutputStream(os);
          }
        };
      default:
        throw QueryException.getException(QueryException.INTERNAL_ERROR, "Unsupported SQL type - " + sqlType);
    }
  }

  private StreamingOutput getMultiStageQueryResponse(String query, String queryOptions, HttpHeaders httpHeaders,
      String traceEnabled)
      throws QException {

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
    try {
      tableNames = queryEnvironment.getTableNamesForQuery(query);
    } catch (QException e) {
      if (e.getErrorCode() != QueryException.UNKNOWN_ERROR_CODE) {
        throw e;
      } else {
        throw new QException(QException.SQL_PARSING_ERROR_CODE, e);
      }
    } catch (Exception e) {
      throw new QException(QException.SQL_PARSING_ERROR_CODE, e);
    }
    return tableNames;
  }

  private List<String> getInstanceIds(String query, List<String> tableNames, String database) {
    List<String> instanceIds;
    if (!tableNames.isEmpty()) {
      List<TableConfig> tableConfigList = getListTableConfigs(tableNames, database);
      if (tableConfigList == null || tableConfigList.isEmpty()) {
        throw new QException(QException.TABLE_DOES_NOT_EXIST_ERROR_CODE,
            "Unable to find table in cluster, table does not exist");
      }

      // find the unions of all the broker tenant tags of the queried tables.
      Set<String> brokerTenantsUnion = getBrokerTenantsUnion(tableConfigList);
      if (brokerTenantsUnion.isEmpty()) {
        throw new QException(QException.BROKER_RESOURCE_MISSING_ERROR_CODE,
            "Unable to find broker tenant for tables: " + tableNames);
      }
      instanceIds = findCommonBrokerInstances(brokerTenantsUnion);
      if (instanceIds.isEmpty()) {
        // No common broker found for table tenants
        throw new QException(QException.BROKER_RESOURCE_MISSING_ERROR_CODE,
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
      throw new QException(QException.QUERY_VALIDATION_ERROR_CODE, e);
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
        throw new QException(QException.SQL_PARSING_ERROR_CODE, new Exception(
            "It seems that the query is only supported by the multi-stage query engine, please retry the query using "
                + "the multi-stage query engine "
                + "(https://docs.pinot.apache.org/developers/advanced/v2-multi-stage-query-engine)"));
      } else {
        throw new QException(QException.SQL_PARSING_ERROR_CODE, e);
      }
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Validate data access
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasAccess(rawTableName, AccessType.READ, httpHeaders, Actions.Table.QUERY)) {
      throw new QException(QException.ACCESS_DENIED_ERROR_CODE, "Permission denied");
    }

    // Get brokers for the resource table.
    List<String> instanceIds = _pinotHelixResourceManager.getBrokerInstancesFor(rawTableName);
    String instanceId = selectRandomInstanceId(instanceIds);
    return sendRequestToBroker(query, instanceId, traceEnabled, queryOptions, httpHeaders);
  }

  // given a list of tables, returns the list of tableConfigs
  private List<TableConfig> getListTableConfigs(List<String> tableNames, String database) {
    List<TableConfig> allTableConfigList = new ArrayList<>();
    for (String tableName : tableNames) {
      String actualTableName = _pinotHelixResourceManager.getActualTableName(tableName, database);
      List<TableConfig> tableConfigList = new ArrayList<>();
      if (_pinotHelixResourceManager.hasRealtimeTable(actualTableName)) {
        tableConfigList.add(Objects.requireNonNull(_pinotHelixResourceManager.getRealtimeTableConfig(actualTableName)));
      }
      if (_pinotHelixResourceManager.hasOfflineTable(actualTableName)) {
        tableConfigList.add(Objects.requireNonNull(_pinotHelixResourceManager.getOfflineTableConfig(actualTableName)));
      }
      if (tableConfigList.isEmpty()) {
        return null;
      }
      allTableConfigList.addAll(tableConfigList);
    }
    return allTableConfigList;
  }

  private String selectRandomInstanceId(List<String> instanceIds) {
    if (instanceIds.isEmpty()) {
      throw new QException(QException.BROKER_RESOURCE_MISSING_ERROR_CODE, "No broker found for query");
    }

    instanceIds.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
    if (instanceIds.isEmpty()) {
      throw new QException(QException.BROKER_INSTANCE_MISSING_ERROR_CODE, "No online broker found for query");
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

  // return the union of brokerTenants from the tables list.
  private Set<String> getBrokerTenantsUnion(List<TableConfig> tableConfigList) {
    Set<String> tableBrokerTenants = new HashSet<>();
    for (TableConfig tableConfig : tableConfigList) {
      tableBrokerTenants.add(tableConfig.getTenantConfig().getBroker());
    }
    return tableBrokerTenants;
  }

  private StreamingOutput sendRequestToBroker(String query, String instanceId, String traceEnabled, String queryOptions,
      HttpHeaders httpHeaders) {
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
    if (instanceConfig == null) {
      LOGGER.error("Instance {} not found", instanceId);
      throw new QException(QException.INTERNAL_ERROR_CODE, "Instance " + instanceId + " not found");
    }

    String hostName = instanceConfig.getHostName();
    // Backward-compatible with legacy hostname of format 'Broker_<hostname>'
    if (hostName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
      hostName = hostName.substring(CommonConstants.Helix.BROKER_INSTANCE_PREFIX_LENGTH);
    }

    String protocol = _controllerConf.getControllerBrokerProtocol();
    int port = _controllerConf.getControllerBrokerPortOverride() > 0 ? _controllerConf.getControllerBrokerPortOverride()
        : Integer.parseInt(instanceConfig.getPort());
    String url = getQueryURL(protocol, hostName, port);
    ObjectNode requestJson = getRequestJson(query, traceEnabled, queryOptions);

    // forward client-supplied headers
    Map<String, String> headers =
        httpHeaders.getRequestHeaders().entrySet().stream().filter(entry -> !entry.getValue().isEmpty())
            .map(entry -> Pair.of(entry.getKey(), entry.getValue().get(0)))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    return sendRequestRaw(url, query, requestJson, headers);
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

  public void sendPostRaw(String urlStr, String requestStr, Map<String, String> headers, OutputStream outputStream) {
    HttpURLConnection conn = null;
    try {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Sending a post request to the server - " + urlStr);
      }

      final URL url = new URL(urlStr);
      conn = (HttpURLConnection) url.openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      // conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

      conn.setRequestProperty("Accept-Encoding", "gzip");

      final String string = requestStr;
      final byte[] requestBytes = string.getBytes(StandardCharsets.UTF_8);
      conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
      conn.setRequestProperty("http.keepAlive", String.valueOf(true));
      conn.setRequestProperty("default", String.valueOf(true));

      if (headers != null && !headers.isEmpty()) {
        final Set<Entry<String, String>> entries = headers.entrySet();
        for (final Entry<String, String> entry : entries) {
          conn.setRequestProperty(entry.getKey(), entry.getValue());
        }
      }

      try (final OutputStream os = new BufferedOutputStream(conn.getOutputStream())) {
        os.write(requestBytes);
        os.flush();
      }
      final int responseCode = conn.getResponseCode();

      if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
        throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
      } else if (responseCode != HttpURLConnection.HTTP_OK) {
        InputStream errorStream = conn.getErrorStream();
        throw new IOException(
            "Failed : HTTP error code : " + responseCode + ". Root Cause: " + (errorStream != null ? IOUtils.toString(
                errorStream, StandardCharsets.UTF_8) : "Unknown"));
      }
      IOUtils.copy(conn.getInputStream(), outputStream);
    } catch (final Exception ex) {
      LOGGER.error("Caught exception while sending query request", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  public StreamingOutput sendRequestRaw(String url, String query, ObjectNode requestJson, Map<String, String> headers) {
    return outputStream -> {
      final long startTime = System.currentTimeMillis();
      sendPostRaw(url, requestJson.toString(), headers, outputStream);

      final long queryTime = System.currentTimeMillis() - startTime;
      LOGGER.info("Query: {} Time: {}", query, queryTime);
    };
  }

private static StreamingOutput constructQueryExceptionResponse(int errorCode, String message) {
    return outputStream -> {
      try (OutputStream os = outputStream) {
        new BrokerResponseNative(errorCode, message).toOutputStream(os);
      }
    };
  }

  private static StreamingOutput constructQueryExceptionResponse(ProcessingException pe) {
    return constructQueryExceptionResponse(pe.getErrorCode(), pe.getMessage());
  }

  private static StreamingOutput constructQueryExceptionResponse(QException ex) {
    return constructQueryExceptionResponse(ex.getErrorCode(), ex.getMessage());
  }
}
