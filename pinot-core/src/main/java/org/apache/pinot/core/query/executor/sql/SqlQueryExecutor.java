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
package org.apache.pinot.core.query.executor.sql;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;
import org.apache.pinot.sql.parsers.parser.SqlShowDatabases;
import org.apache.pinot.sql.parsers.parser.SqlShowSchemas;
import org.apache.pinot.sql.parsers.parser.SqlShowTables;


/**
 * SqlQueryExecutor executes all SQL queries including DQL, DML, DCL, DDL.
 *
 */
public class SqlQueryExecutor {
  private final String _controllerUrl;
  private final HelixManager _helixManager;

  /**
   * Fetch the lead controller from helix, HA is not guaranteed.
   * @param helixManager is used to query leader controller from helix.
   */
  public SqlQueryExecutor(HelixManager helixManager) {
    _helixManager = helixManager;
    _controllerUrl = null;
  }

  /**
   * Recommended to provide the controller vip or service name for access.
   * @param controllerUrl controller service name for sending minion task requests
   */
  public SqlQueryExecutor(String controllerUrl) {
    _controllerUrl = controllerUrl;
    _helixManager = null;
  }

  private static String getControllerBaseUrl(HelixManager helixManager) {
    String instanceId = LeadControllerUtils.getHelixClusterLeader(helixManager);
    if (instanceId == null) {
      throw new RuntimeException("Unable to locate the leader pinot controller, please retry later...");
    }

    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
    ExtraInstanceConfig extraInstanceConfig = new ExtraInstanceConfig(helixDataAccessor.getProperty(
        keyBuilder.instanceConfig(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + instanceId)));
    String controllerBaseUrl = extraInstanceConfig.getComponentUrl();
    if (controllerBaseUrl == null) {
      throw new RuntimeException("Unable to extract the base url from the leader pinot controller");
    }
    return controllerBaseUrl;
  }

  /**
   * Execute DML Statement
   *
   * @param sqlNodeAndOptions Parsed DML object
   * @param headers extra headers map for minion task submission
   * @return BrokerResponse is the DML executed response
   */
  public BrokerResponse executeDMLStatement(SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable Map<String, String> headers) {
    DataManipulationStatement statement = DataManipulationStatementParser.parse(sqlNodeAndOptions);
    BrokerResponseNative result = new BrokerResponseNative();
    switch (statement.getExecutionType()) {
      case MINION:
        AdhocTaskConfig taskConf = statement.generateAdhocTaskConfig();
        try {
          Map<String, String> tableToTaskIdMap = getMinionClient().executeTask(taskConf, headers);
          List<Object[]> rows = new ArrayList<>();
          tableToTaskIdMap.forEach((key, value) -> rows.add(new Object[]{key, value}));
          result.setResultTable(new ResultTable(statement.getResultSchema(), rows));
        } catch (Exception e) {
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      case HTTP:
        try {
          result.setResultTable(new ResultTable(statement.getResultSchema(), statement.execute()));
        } catch (Exception e) {
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      default:
        result.addException(
            new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, "Unsupported statement: " + statement));
        break;
    }
    return result;
  }

  /**
   * Execute metadata (SHOW/USE) statements.
   */
  public BrokerResponse executeMetadataStatement(SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable Map<String, String> headers) {
    BrokerResponseNative result = new BrokerResponseNative();
    try {
      ResultTable resultTable =
          buildMetadataResult(sqlNodeAndOptions.getSqlNode(), sqlNodeAndOptions.getOptions(), headers);
      result.setResultTable(resultTable);
    } catch (Exception e) {
      result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
    }
    return result;
  }

  private MinionClient getMinionClient() {
    // NOTE: using null auth provider here as auth headers injected by caller in "executeDMLStatement()"
    if (_helixManager != null) {
      return new MinionClient(getControllerBaseUrl(_helixManager), null);
    }
    return new MinionClient(_controllerUrl, null);
  }

  private ResultTable buildMetadataResult(SqlNode sqlNode, Map<String, String> options,
      @Nullable Map<String, String> headers)
      throws Exception {
    if (sqlNode instanceof SqlShowDatabases) {
      return toSingleStringColumnResult("databaseName", fetchDatabases(headers));
    }
    if (sqlNode instanceof SqlShowTables) {
      SqlShowTables showTables = (SqlShowTables) sqlNode;
      String database = resolveDatabase(showTables.getDatabaseName(), options, headers);
      List<String> tables = stripDatabasePrefix(fetchTables(database, headers), database);
      return toSingleStringColumnResult("tableName", tables);
    }
    if (sqlNode instanceof SqlShowSchemas) {
      SqlShowSchemas showSchemas = (SqlShowSchemas) sqlNode;
      String database = resolveDatabase(null, options, headers);
      List<String> schemas = stripDatabasePrefix(fetchSchemas(database, headers), database);
      String likePattern = getLikePattern(showSchemas.getLikePattern());
      schemas = applyLikeFilter(schemas, likePattern);
      return toSingleStringColumnResult("schemaName", schemas);
    }
    throw new UnsupportedOperationException("Unsupported METADATA SqlKind - " + sqlNode.getKind());
  }

  private ResultTable toSingleStringColumnResult(String columnName, List<String> values) {
    DataSchema dataSchema = new DataSchema(new String[]{columnName}, new ColumnDataType[]{ColumnDataType.STRING});
    List<Object[]> rows = values.stream().map(value -> new Object[]{value}).collect(Collectors.toList());
    return new ResultTable(dataSchema, rows);
  }

  private String resolveDatabase(@Nullable SqlIdentifier explicitDatabase, Map<String, String> options,
      @Nullable Map<String, String> headers)
      throws DatabaseConflictException {
    String databaseFromSql = explicitDatabase != null ? explicitDatabase.toString() : null;
    String databaseFromOptions = options.get(CommonConstants.DATABASE);
    String databaseFromHeaders = getHeaderValue(headers, CommonConstants.DATABASE);

    if (databaseFromSql != null) {
      if (databaseFromOptions != null && !databaseFromSql.equalsIgnoreCase(databaseFromOptions)) {
        throw new DatabaseConflictException(
            "Database name '" + databaseFromSql + "' from statement does not match database name '"
                + databaseFromOptions + "' from query options and database name '" + databaseFromHeaders
                + "' from request header");
      }
      if (databaseFromHeaders != null && !databaseFromSql.equalsIgnoreCase(databaseFromHeaders)) {
        throw new DatabaseConflictException(
            "Database name '" + databaseFromSql + "' from statement does not match database name '"
                + databaseFromHeaders + "' from request header and database name '" + databaseFromOptions
                + "' from query options");
      }
      return databaseFromSql;
    }

    if (databaseFromOptions != null && databaseFromHeaders != null
        && !databaseFromOptions.equalsIgnoreCase(databaseFromHeaders)) {
      throw new DatabaseConflictException("Database name '" + databaseFromHeaders + "' from request header does not "
          + "match database name '" + databaseFromOptions + "' from query options");
    }

    return databaseFromOptions != null ? databaseFromOptions
        : databaseFromHeaders != null ? databaseFromHeaders : CommonConstants.DEFAULT_DATABASE;
  }

  private List<String> stripDatabasePrefix(List<String> names, String database) {
    if (names.isEmpty()) {
      return names;
    }
    return names.stream()
        .map(name -> DatabaseUtils.isPartOfDatabase(name, database)
            ? DatabaseUtils.removeDatabasePrefix(name, database)
            : name)
        .collect(Collectors.toList());
  }

  private List<String> applyLikeFilter(List<String> values, @Nullable String likePattern) {
    if (StringUtils.isEmpty(likePattern)) {
      return values;
    }
    Pattern pattern = buildLikeRegex(likePattern);
    return values.stream().filter(value -> pattern.matcher(value).matches()).collect(Collectors.toList());
  }

  private Pattern buildLikeRegex(String likePattern) {
    StringBuilder regex = new StringBuilder();
    boolean escaped = false;
    for (char c : likePattern.toCharArray()) {
      if (escaped) {
        regex.append(Pattern.quote(String.valueOf(c)));
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
  }

  private String getLikePattern(@Nullable SqlLiteral likeLiteral) {
    return likeLiteral == null ? null : likeLiteral.toValue();
  }

  private List<String> fetchDatabases(@Nullable Map<String, String> headers)
      throws IOException {
    String response = sendGetRequest("/databases", headers);
    JsonNode jsonNode = JsonUtils.stringToJsonNode(response);
    List<String> databases = new ArrayList<>();
    jsonNode.forEach(node -> databases.add(node.asText()));
    return databases;
  }

  private List<String> fetchTables(String database, @Nullable Map<String, String> headers)
      throws IOException {
    Map<String, String> requestHeaders = new HashMap<>();
    if (headers != null) {
      requestHeaders.putAll(headers);
    }
    requestHeaders.put(CommonConstants.DATABASE, database);
    String response = sendGetRequest("/tables", requestHeaders);
    JsonNode jsonNode = JsonUtils.stringToJsonNode(response);
    JsonNode tablesNode = jsonNode.has("tables") ? jsonNode.get("tables") : jsonNode;
    List<String> tables = new ArrayList<>();
    tablesNode.forEach(node -> tables.add(node.asText()));
    return tables;
  }

  private List<String> fetchSchemas(String database, @Nullable Map<String, String> headers)
      throws IOException {
    Map<String, String> requestHeaders = new HashMap<>();
    if (headers != null) {
      requestHeaders.putAll(headers);
    }
    requestHeaders.put(CommonConstants.DATABASE, database);
    String response = sendGetRequest("/schemas", requestHeaders);
    JsonNode jsonNode = JsonUtils.stringToJsonNode(response);
    List<String> schemas = new ArrayList<>();
    jsonNode.forEach(node -> schemas.add(node.asText()));
    return schemas;
  }

  private String sendGetRequest(String path, @Nullable Map<String, String> headers)
      throws IOException {
    String baseUrl = _helixManager != null ? getControllerBaseUrl(_helixManager) : _controllerUrl;
    if (baseUrl == null) {
      throw new IOException("Controller URL is not configured for metadata query");
    }
    String urlString = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) + path : baseUrl + path;
    HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
    connection.setRequestMethod("GET");
    if (headers != null) {
      headers.forEach(connection::setRequestProperty);
    }
    int responseCode = connection.getResponseCode();
    try (InputStream inputStream =
        responseCode >= 200 && responseCode < 300 ? connection.getInputStream() : connection.getErrorStream()) {
      String response =
          inputStream != null ? IOUtils.toString(inputStream, StandardCharsets.UTF_8) : StringUtils.EMPTY;
      if (responseCode >= 200 && responseCode < 300) {
        return response;
      }
      throw new IOException(
          "Failed to fetch metadata from controller: HTTP " + responseCode + " response: " + response);
    }
  }

  private String getHeaderValue(@Nullable Map<String, String> headers, String key) {
    if (headers == null || headers.isEmpty()) {
      return null;
    }
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  }
}
