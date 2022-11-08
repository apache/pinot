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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
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
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing post request", e);
      return QueryException.getException(QueryException.INTERNAL_ERROR, e).toString();
    }
  }

  @GET
  @Path("sql")
  public String handleGetSql(@QueryParam("sql") String sqlQuery, @QueryParam("trace") String traceEnabled,
      @QueryParam("queryOptions") String queryOptions, @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.debug("Trace: {}, Running query: {}", traceEnabled, sqlQuery);
      return executeSqlQuery(httpHeaders, sqlQuery, traceEnabled, queryOptions, "/sql");
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing get request", e);
      return QueryException.getException(QueryException.INTERNAL_ERROR, e).toString();
    }
  }

  private String executeSqlQuery(@Context HttpHeaders httpHeaders, String sqlQuery, String traceEnabled,
      @Nullable String queryOptions, String endpointUrl)
      throws Exception {
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sqlQuery);
    Map<String, String> options = sqlNodeAndOptions.getOptions();
    if (queryOptions != null) {
      Map<String, String> optionsFromString = RequestUtils.getOptionsFromString(queryOptions);
      sqlNodeAndOptions.setExtraOptions(optionsFromString);
    }

    // Determine which engine to used based on query options.
    if (Boolean.parseBoolean(options.get(QueryOptionKey.USE_MULTISTAGE_ENGINE))) {
      if (_controllerConf.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED,
          CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_ENABLED)) {
        return getMultiStageQueryResponse(sqlQuery, queryOptions, httpHeaders, endpointUrl);
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
      String endpointUrl) {

    // Validate data access
    // we don't have a cross table access control rule so only ADMIN can make request to multi-stage engine.
    AccessControl accessControl = _accessControlFactory.create();
    if (!accessControl.hasAccess(null, AccessType.READ, httpHeaders, endpointUrl)) {
      return QueryException.ACCESS_DENIED_ERROR.toString();
    }

    // Get brokers, only DEFAULT tenant is supported for now.
    // TODO: implement logic that only allows executing query where all accessed tables are within the same tenant.
    List<String> instanceIds = new ArrayList<>(_pinotHelixResourceManager.getAllInstancesForBrokerTenant(
        TagNameUtils.DEFAULT_TENANT_NAME));
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_RESOURCE_MISSING_ERROR.toString();
    }

    instanceIds.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
    if (instanceIds.isEmpty()) {
      return QueryException.BROKER_INSTANCE_MISSING_ERROR.toString();
    }

    // Send query to a random broker.
    String instanceId = instanceIds.get(RANDOM.nextInt(instanceIds.size()));
    return sendRequestToBroker(query, instanceId, "false", queryOptions, httpHeaders);
  }

  private String getQueryResponse(String query, @Nullable SqlNode sqlNode, String traceEnabled, String queryOptions,
      HttpHeaders httpHeaders) {
    // Get resource table name.
    String tableName;
    try {
      String inputTableName =
          sqlNode != null ? RequestUtils.getTableName(CalciteSqlParser.compileSqlNodeToPinotQuery(sqlNode))
              : CalciteSqlCompiler.compileToBrokerRequest(query).getQuerySource().getTableName();
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
