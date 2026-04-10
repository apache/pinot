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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminTransport;
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
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;
import org.apache.pinot.sql.parsers.parser.SqlShowDatabases;
import org.apache.pinot.sql.parsers.parser.SqlShowSchemas;
import org.apache.pinot.sql.parsers.parser.SqlShowTables;


/**
 * SqlQueryExecutor executes all SQL queries including DQL, DML, DCL, DDL.
 */
public class SqlQueryExecutor {
  // Controller URL cached to avoid ZK reads on every metadata query.
  // Refreshed after TTL expires or after a failed request.
  private static final long CONTROLLER_URL_CACHE_TTL_MS = 30_000L;

  private final String _controllerUrl;
  private final HelixManager _helixManager;

  // Guarded by synchronized(this) when _helixManager != null
  private volatile String _cachedControllerUrl;
  private volatile long _cachedControllerUrlExpiry;

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

  /**
   * Returns the controller base URL, using a cached value when possible to avoid ZK reads on each metadata query.
   * Falls back to a fresh ZK lookup when the cached value is expired or invalidated after a request failure.
   */
  private synchronized String resolveControllerUrl() {
    if (_helixManager == null) {
      return _controllerUrl;
    }
    long now = System.currentTimeMillis();
    if (_cachedControllerUrl == null || now >= _cachedControllerUrlExpiry) {
      _cachedControllerUrl = getControllerBaseUrl(_helixManager);
      _cachedControllerUrlExpiry = now + CONTROLLER_URL_CACHE_TTL_MS;
    }
    return _cachedControllerUrl;
  }

  /** Invalidates the cached controller URL so the next call re-fetches from ZK. */
  private synchronized void invalidateControllerUrlCache() {
    _cachedControllerUrl = null;
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
   * Execute metadata (SHOW) statements.
   */
  public BrokerResponse executeMetadataStatement(SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable Map<String, String> headers) {
    BrokerResponseNative result = new BrokerResponseNative();
    Map<String, String> requestHeaders = copyHeaders(headers);
    try {
      ResultTable resultTable =
          buildMetadataResult(sqlNodeAndOptions.getSqlNode(), sqlNodeAndOptions.getOptions(), requestHeaders);
      result.setResultTable(resultTable);
    } catch (Exception e) {
      result.addException(
          new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getClass().getSimpleName() + ": "
              + (e.getMessage() != null ? e.getMessage() : "(no message)")));
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

  private ResultTable buildMetadataResult(SqlNode sqlNode, Map<String, String> options, Map<String, String> headers)
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
    List<Object[]> rows = new ArrayList<>(values.size());
    for (String value : values) {
      rows.add(new Object[]{value});
    }
    return new ResultTable(dataSchema, rows);
  }

  private String resolveDatabase(@Nullable SqlIdentifier explicitDatabase, Map<String, String> options,
      Map<String, String> headers)
      throws DatabaseConflictException {
    String databaseFromSql = explicitDatabase != null ? explicitDatabase.toString() : null;
    String databaseFromOptions = options.get(CommonConstants.DATABASE);
    String databaseFromHeaders = getHeaderValue(headers, CommonConstants.DATABASE);

    if (databaseFromSql != null) {
      if (databaseFromOptions != null && !databaseFromSql.equalsIgnoreCase(databaseFromOptions)) {
        StringBuilder message = new StringBuilder("Database name '").append(databaseFromSql)
            .append("' from statement does not match database name '").append(databaseFromOptions)
            .append("' from query options");
        if (databaseFromHeaders != null) {
          message.append(" (request header database name: '").append(databaseFromHeaders).append("')");
        }
        throw new DatabaseConflictException(message.toString());
      }
      if (databaseFromHeaders != null && !databaseFromSql.equalsIgnoreCase(databaseFromHeaders)) {
        StringBuilder message = new StringBuilder("Database name '").append(databaseFromSql)
            .append("' from statement does not match database name '").append(databaseFromHeaders)
            .append("' from request header");
        if (databaseFromOptions != null) {
          message.append(" (query options database name: '").append(databaseFromOptions).append("')");
        }
        throw new DatabaseConflictException(message.toString());
      }
      return databaseFromSql;
    }

    if (databaseFromOptions != null && databaseFromHeaders != null
        && !databaseFromOptions.equalsIgnoreCase(databaseFromHeaders)) {
      throw new DatabaseConflictException(
          "Database name mismatch between query options ('" + databaseFromOptions + "') and request header ('"
              + databaseFromHeaders + "')");
    }

    return databaseFromOptions != null ? databaseFromOptions
        : databaseFromHeaders != null ? databaseFromHeaders : CommonConstants.DEFAULT_DATABASE;
  }

  private List<String> stripDatabasePrefix(List<String> names, String database) {
    if (names.isEmpty()) {
      return names;
    }
    List<String> result = new ArrayList<>(names.size());
    for (String name : names) {
      result.add(DatabaseUtils.isPartOfDatabase(name, database)
          ? DatabaseUtils.removeDatabasePrefix(name, database) : name);
    }
    return result;
  }

  private List<String> applyLikeFilter(List<String> values, @Nullable String likePattern) {
    if (StringUtils.isEmpty(likePattern)) {
      return values;
    }
    Pattern pattern = buildLikeRegex(likePattern);
    List<String> result = new ArrayList<>();
    for (String value : values) {
      if (pattern.matcher(value).matches()) {
        result.add(value);
      }
    }
    return result;
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

  private List<String> fetchDatabases(Map<String, String> headers)
      throws IOException {
    try (PinotAdminClient adminClient = createAdminClient(headers)) {
      return adminClient.getDatabaseClient().listDatabaseNames();
    } catch (Exception e) {
      invalidateControllerUrlCache();
      throw e instanceof IOException ? (IOException) e
          : new IOException("Failed to fetch databases from controller", e);
    }
  }

  private List<String> fetchTables(String database, Map<String, String> headers)
      throws IOException {
    Map<String, String> requestHeaders = copyHeaders(headers);
    requestHeaders.put(CommonConstants.DATABASE, database);
    try (PinotAdminClient adminClient = createAdminClient(requestHeaders)) {
      return adminClient.getTableClient().listTables(null, null, null);
    } catch (Exception e) {
      invalidateControllerUrlCache();
      throw e instanceof IOException ? (IOException) e : new IOException("Failed to fetch tables from controller", e);
    }
  }

  private List<String> fetchSchemas(String database, Map<String, String> headers)
      throws IOException {
    Map<String, String> requestHeaders = copyHeaders(headers);
    requestHeaders.put(CommonConstants.DATABASE, database);
    try (PinotAdminClient adminClient = createAdminClient(requestHeaders)) {
      return adminClient.getSchemaClient().listSchemaNames();
    } catch (Exception e) {
      invalidateControllerUrlCache();
      throw e instanceof IOException ? (IOException) e : new IOException("Failed to fetch schemas from controller", e);
    }
  }

  private static final class ControllerEndpoint {
    private final String _scheme;
    private final String _controllerAddress;

    private ControllerEndpoint(String scheme, String controllerAddress) {
      _scheme = scheme;
      _controllerAddress = controllerAddress;
    }
  }

  private ControllerEndpoint toControllerEndpoint(String baseUrl)
      throws IOException {
    String normalizedBaseUrl = StringUtils.stripEnd(baseUrl.trim(), "/");
    String uriString = normalizedBaseUrl.contains("://")
        ? normalizedBaseUrl
        : CommonConstants.HTTP_PROTOCOL + "://" + normalizedBaseUrl;

    URI uri = URI.create(uriString);
    String scheme = StringUtils.defaultIfBlank(uri.getScheme(), CommonConstants.HTTP_PROTOCOL);
    String authority = uri.getAuthority();
    if (StringUtils.isBlank(authority)) {
      throw new IOException("Invalid controller URL: " + baseUrl);
    }

    // PinotAdminTransport expects "host:port" (or "host:port/<basePath>") and uses a separate scheme property.
    String path = StringUtils.stripEnd(StringUtils.defaultString(uri.getPath()), "/");
    String controllerAddress = StringUtils.isBlank(path) || "/".equals(path) ? authority : authority + path;

    return new ControllerEndpoint(scheme, controllerAddress);
  }

  private PinotAdminClient createAdminClient(Map<String, String> headers)
      throws IOException {
    String baseUrl = resolveControllerUrl();
    if (StringUtils.isBlank(baseUrl)) {
      throw new IOException("Controller URL is not configured for metadata query");
    }

    ControllerEndpoint controllerEndpoint = toControllerEndpoint(baseUrl);

    Properties properties = new Properties();
    properties.setProperty(PinotAdminTransport.ADMIN_TRANSPORT_SCHEME, controllerEndpoint._scheme);
    return new PinotAdminClient(controllerEndpoint._controllerAddress, properties, headers);
  }

  /**
   * Returns a case-insensitive copy of the given headers map.
   * All downstream lookups (e.g. {@link #getHeaderValue}) rely on the case-insensitive ordering
   * of the returned TreeMap, so the map type must not be changed.
   */
  private Map<String, String> copyHeaders(@Nullable Map<String, String> headers) {
    Map<String, String> requestHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (headers != null && !headers.isEmpty()) {
      requestHeaders.putAll(headers);
    }
    return requestHeaders;
  }

  private String getHeaderValue(Map<String, String> headers, String key) {
    if (headers.isEmpty()) {
      return null;
    }
    return headers.get(key);
  }
}
