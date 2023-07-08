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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.config.table.TableConfig;
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
  private static final Random RANDOM = new Random();

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
  public String handlePostSql(String requestJsonStr, @Context HttpHeaders httpHeaders) {
    try {
      JsonNode requestJson = JsonUtils.stringToJsonNode(requestJsonStr);
      String sqlQuery = requestJson.get("sql").asText();
      String traceEnabled = "false";
      if (requestJson.has("trace")) {
        traceEnabled = requestJson.get("trace").toString();
      }
      String queryOptions = null;
      if (requestJson.has("queryOptions")) {
        queryOptions = requestJson.get("queryOptions").asText();
      }
      LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, sqlQuery);
      return executeSqlQuery(httpHeaders, sqlQuery, traceEnabled, queryOptions, "/sql");
    } catch (ProcessingException pe) {
      LOGGER.error("Caught exception while processing post request {}", pe.getMessage());
      return pe.getMessage();
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing post request", e);
      return QueryException.getException(QueryException.INTERNAL_ERROR, e).toString();
    }
  }

  @GET
  @Path("sql")
  @ManualAuthorization
  public String handleGetSql(@QueryParam("sql") String sqlQuery, @QueryParam("trace") String traceEnabled,
      @QueryParam("queryOptions") String queryOptions, @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, sqlQuery);
      return executeSqlQuery(httpHeaders, sqlQuery, traceEnabled, queryOptions, "/sql");
    } catch (ProcessingException pe) {
      LOGGER.error("Caught exception while processing get request {}", pe.getMessage());
      return pe.getMessage();
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing get request", e);
      return QueryException.getException(QueryException.INTERNAL_ERROR, e).toString();
    }
  }

  private String executeSqlQuery(@Context HttpHeaders httpHeaders, String sqlQuery, String traceEnabled,
      @Nullable String queryOptions, String endpointUrl)
      throws Exception {
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
    if (Boolean.parseBoolean(options.get(QueryOptionKey.USE_MULTISTAGE_ENGINE))) {
      if (_controllerConf.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED,
          CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_ENABLED)) {
        return getMultiStageQueryResponse(sqlQuery, queryOptions, httpHeaders, endpointUrl, traceEnabled);
      } else {
        throw new UnsupportedOperationException("V2 Multi-Stage query engine not enabled. "
            + "Please see https://docs.pinot.apache.org/ for instruction to enable V2 engine.");
      }
    } else {
      PinotSqlType sqlType = sqlNodeAndOptions.getSqlType();
      switch (sqlType) {
        case DQL:
          return getQueryResponse(sqlQuery, sqlNodeAndOptions.getSqlNode(), traceEnabled, queryOptions, httpHeaders);
        case DML:
          Map<String, String> headers =
              httpHeaders.getRequestHeaders().entrySet().stream().filter(entry -> !entry.getValue().isEmpty())
                  .map(entry -> Pair.of(entry.getKey(), entry.getValue().get(0)))
                  .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
          return _sqlQueryExecutor.executeDMLStatement(sqlNodeAndOptions, headers).toJsonString();
        default:
          throw new UnsupportedOperationException("Unsupported SQL type - " + sqlType);
      }
    }
  }

  private String getMultiStageQueryResponse(String query, String queryOptions, HttpHeaders httpHeaders,
      String endpointUrl, String traceEnabled) {

    // Validate data access
    // we don't have a cross table access control rule so only ADMIN can make request to multi-stage engine.
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasAccess(null, AccessType.READ, httpHeaders, endpointUrl)) {
      return QueryException.ACCESS_DENIED_ERROR.toString();
    }

    QueryEnvironment queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(_pinotHelixResourceManager.getTableCache())), null, null);
    List<String> tableNames = queryEnvironment.getTableNamesForQuery(query);
    String brokerTenant;
    if (tableNames.size() != 0) {
      List<TableConfig> tableConfigList = getListTableConfigs(tableNames);
      if (tableConfigList == null || tableConfigList.size() == 0) {
        return QueryException.getException(QueryException.TABLE_DOES_NOT_EXIST_ERROR,
            new Exception("Unable to find table in cluster, table does not exist")).toString();
      }

      // When routing a query, there should be at least one common broker tenant for the table. However, the server
      // tenants can be completely disjoint. The leaf stages which access segments will be processed on the respective
      // server tenants for each table. The intermediate stages can be processed in either or all of the server tenants
      // belonging to the tables.
      brokerTenant = getCommonBrokerTenant(tableConfigList);
      if (brokerTenant == null) {
        return QueryException.getException(QueryException.BROKER_REQUEST_SEND_ERROR, new Exception(
            String.format("Unable to dispatch multistage query with multiple tables : %s " + "on different tenant",
                tableNames))).toString();
      }
    } else {
      // TODO fail these queries going forward. Added this logic to take care of tautologies like BETWEEN 0 and -1.
      List<String> allBrokerList = new ArrayList<>(_pinotHelixResourceManager.getAllBrokerTenantNames());
      brokerTenant = allBrokerList.get(RANDOM.nextInt(allBrokerList.size()));
      LOGGER.error("Unable to find table name from SQL {} thus dispatching to random broker.", query);
    }
    List<String> instanceIds = new ArrayList<>(_pinotHelixResourceManager.getAllInstancesForBrokerTenant(brokerTenant));

    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_RESOURCE_MISSING_ERROR.toString();
    }

    instanceIds.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_INSTANCE_MISSING_ERROR.toString();
    }

    // Send query to a random broker.
    String instanceId = instanceIds.get(RANDOM.nextInt(instanceIds.size()));
    return sendRequestToBroker(query, instanceId, traceEnabled, queryOptions, httpHeaders);
  }

  private String getQueryResponse(String query, @Nullable SqlNode sqlNode, String traceEnabled, String queryOptions,
      HttpHeaders httpHeaders) {
    // Get resource table name.
    String tableName;
    try {
      String inputTableName =
          sqlNode != null ? RequestUtils.getTableNames(CalciteSqlParser.compileSqlNodeToPinotQuery(sqlNode)).iterator()
              .next() : CalciteSqlCompiler.compileToBrokerRequest(query).getQuerySource().getTableName();
      tableName = _pinotHelixResourceManager.getActualTableName(inputTableName);
    } catch (Exception e) {
      LOGGER.error("Caught exception while compiling query: {}", query, e);
      return QueryException.getException(QueryException.SQL_PARSING_ERROR, e).toString();
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Validate data access
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasDataAccess(httpHeaders, rawTableName)) {
      return QueryException.ACCESS_DENIED_ERROR.toString();
    }

    // Get brokers for the resource table.
    List<String> instanceIds = _pinotHelixResourceManager.getBrokerInstancesFor(rawTableName);
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_RESOURCE_MISSING_ERROR.toString();
    }

    // Retain only online brokers.
    instanceIds.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_INSTANCE_MISSING_ERROR.toString();
    }

    // Send query to a random broker.
    String instanceId = instanceIds.get(RANDOM.nextInt(instanceIds.size()));
    return sendRequestToBroker(query, instanceId, traceEnabled, queryOptions, httpHeaders);
  }

  // given a list of tables, returns the list of tableConfigs
  private List<TableConfig> getListTableConfigs(List<String> tableNames) {
    List<TableConfig> allTableConfigList = new ArrayList<>();
    for (String tableName : tableNames) {
      List<TableConfig> tableConfigList = new ArrayList<>();
      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        tableConfigList.add(Objects.requireNonNull(_pinotHelixResourceManager.getRealtimeTableConfig(tableName)));
      }
      if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
        tableConfigList.add(Objects.requireNonNull(_pinotHelixResourceManager.getOfflineTableConfig(tableName)));
      }
      if (tableConfigList.size() == 0) {
        return null;
      }
      allTableConfigList.addAll(tableConfigList);
    }
    return allTableConfigList;
  }

  // return the brokerTenant if all table configs point to the same broker, else returns null
  private String getCommonBrokerTenant(List<TableConfig> tableConfigList) {
    Set<String> tableBrokers = new HashSet<>();
    for (TableConfig tableConfig : tableConfigList) {
      tableBrokers.add(tableConfig.getTenantConfig().getBroker());
    }
    if (tableBrokers.size() != 1) {
      return null;
    }
    return (String) (tableBrokers.toArray()[0]);
  }

  private String sendRequestToBroker(String query, String instanceId, String traceEnabled, String queryOptions,
      HttpHeaders httpHeaders) {
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
    if (instanceConfig == null) {
      LOGGER.error("Instance {} not found", instanceId);
      return QueryException.INTERNAL_ERROR.toString();
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

  public String sendPostRaw(String urlStr, String requestStr, Map<String, String> headers) {
    HttpURLConnection conn = null;
    try {
      /*if (LOG.isInfoEnabled()){
        LOGGER.info("Sending a post request to the server - " + urlStr);
      }

      if (LOG.isDebugEnabled()){
        LOGGER.debug("The request is - " + requestStr);
      }*/

      LOGGER.info("url string passed is : " + urlStr);
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

      //GZIPOutputStream zippedOutputStream = new GZIPOutputStream(conn.getOutputStream());
      final OutputStream os = new BufferedOutputStream(conn.getOutputStream());
      os.write(requestBytes);
      os.flush();
      os.close();
      final int responseCode = conn.getResponseCode();

      /*if (LOG.isInfoEnabled()){
        LOGGER.info("The http response code is " + responseCode);
      }*/
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("Failed : HTTP error code : " + responseCode + ". Root Cause: "
            + IOUtils.toString(conn.getErrorStream(), StandardCharsets.UTF_8));
      }
      final byte[] bytes = drain(new BufferedInputStream(conn.getInputStream()));

      final String output = new String(bytes, StandardCharsets.UTF_8);
      /*if (LOG.isDebugEnabled()){
        LOGGER.debug("The response from the server is - " + output);
      }*/
      return output;
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

  byte[] drain(InputStream inputStream)
      throws IOException {
    try {
      final byte[] buf = new byte[1024];
      int len;
      final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      while ((len = inputStream.read(buf)) > 0) {
        byteArrayOutputStream.write(buf, 0, len);
      }
      return byteArrayOutputStream.toByteArray();
    } finally {
      inputStream.close();
    }
  }

  public String sendRequestRaw(String url, String query, ObjectNode requestJson, Map<String, String> headers) {
    try {
      final long startTime = System.currentTimeMillis();
      final String pinotResultString = sendPostRaw(url, requestJson.toString(), headers);

      final long queryTime = System.currentTimeMillis() - startTime;
      LOGGER.info("Query: " + query + " Time: " + queryTime);

      return pinotResultString;
    } catch (final Exception ex) {
      LOGGER.error("Caught exception in sendQueryRaw", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    }
  }
}
