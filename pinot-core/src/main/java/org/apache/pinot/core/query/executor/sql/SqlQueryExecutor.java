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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;
import org.apache.pinot.sql.parsers.dml.InsertIntoValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SqlQueryExecutor executes all SQL queries including DQL, DML, DCL, DDL.
 *
 */
public class SqlQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlQueryExecutor.class);

  private final String _controllerUrl;
  private final HelixManager _helixManager;
  private volatile InsertExecutor _insertExecutor;

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
   * Sets the local InsertExecutor for push-based INSERT. When set, the executor is called
   * directly instead of via HTTP to the controller. This is used when the SqlQueryExecutor
   * runs inside the controller process.
   */
  public void setInsertExecutor(InsertExecutor insertExecutor) {
    _insertExecutor = insertExecutor;
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
      case PUSH:
        try {
          InsertResult insertResult = executePushInsert(statement, headers);
          List<Object[]> pushRows = new ArrayList<>();
          pushRows.add(new Object[]{
              insertResult.getStatementId(),
              String.valueOf(insertResult.getState()),
              insertResult.getMessage()
          });
          result.setResultTable(new ResultTable(statement.getResultSchema(), pushRows));
        } catch (Exception e) {
          result.addException(
              new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      default:
        result.addException(
            new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, "Unsupported statement: " + statement));
        break;
    }
    return result;
  }

  private InsertResult executePushInsert(DataManipulationStatement statement,
      @Nullable Map<String, String> headers)
      throws Exception {
    if (!(statement instanceof InsertIntoValues)) {
      throw new IllegalStateException("PUSH execution type requires InsertIntoValues statement");
    }
    InsertIntoValues insertStmt = (InsertIntoValues) statement;

    InsertRequest request = buildInsertRequest(insertStmt);

    // If a local executor is available (controller-side), call it directly
    InsertExecutor localExecutor = _insertExecutor;
    if (localExecutor != null) {
      LOGGER.info("Executing push-based INSERT locally via InsertExecutor");
      return localExecutor.execute(request);
    }

    // Otherwise, POST to controller /insert/execute (broker-side)
    String controllerBaseUrl = getControllerUrl();
    String url = controllerBaseUrl + "/insert/execute";
    String payload = JsonUtils.objectToString(request);

    LOGGER.info("Submitting push-based INSERT to controller: {}", url);

    Map<String, String> requestHeaders = new HashMap<>();
    requestHeaders.put("Content-Type", "application/json");
    requestHeaders.put("accept", "application/json");
    if (headers != null) {
      requestHeaders.putAll(headers);
    }

    String responseStr =
        org.apache.pinot.common.utils.http.HttpClient.wrapAndThrowHttpException(
            org.apache.pinot.common.utils.http.HttpClient.getInstance()
                .sendJsonPostRequest(new java.net.URL(url).toURI(), payload, requestHeaders))
            .getResponse();
    JsonNode responseJson = JsonUtils.stringToJsonNode(responseStr);
    return JsonUtils.jsonNodeToObject(responseJson, InsertResult.class);
  }

  private static InsertRequest buildInsertRequest(InsertIntoValues insertStmt) {
    List<GenericRow> genericRows = new ArrayList<>();
    List<String> columns = insertStmt.getColumns();
    for (List<Object> rowValues : insertStmt.getRows()) {
      GenericRow genericRow = new GenericRow();
      if (columns.isEmpty()) {
        throw new IllegalStateException(
            "INSERT INTO ... VALUES requires an explicit column list, "
                + "e.g. INSERT INTO myTable (col1, col2) VALUES (1, 'a')");
      }
      for (int i = 0; i < columns.size(); i++) {
        genericRow.putValue(columns.get(i), rowValues.get(i));
      }
      genericRows.add(genericRow);
    }

    TableType tableType = null;
    String tableTypeStr = insertStmt.getTableType();
    if (tableTypeStr != null) {
      tableType = TableType.valueOf(tableTypeStr.toUpperCase());
    }

    // Compute a stable payload hash for idempotency when requestId is set
    String payloadHash = computePayloadHash(insertStmt.getTableName(), columns, insertStmt.getRows(),
        insertStmt.getOptions());

    return new InsertRequest.Builder()
        .setTableName(insertStmt.getTableName())
        .setTableType(tableType)
        .setInsertType(InsertType.ROW)
        .setRows(genericRows)
        .setRequestId(insertStmt.getRequestId())
        .setPayloadHash(payloadHash)
        .setOptions(insertStmt.getOptions())
        .build();
  }

  /**
   * Computes a stable SHA-256 hash from the table name, column list, row values, and options.
   * This ensures that identical INSERT statements produce the same hash for idempotency.
   */
  private static String computePayloadHash(String tableName, List<String> columns,
      List<List<Object>> rows, Map<String, String> options) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(tableName.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);

      for (String col : columns) {
        digest.update(col.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
      }
      digest.update((byte) 1);

      for (List<Object> row : rows) {
        for (Object val : row) {
          digest.update(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
        }
        digest.update((byte) 2);
      }

      if (options != null && !options.isEmpty()) {
        // Sort keys for deterministic ordering
        for (Map.Entry<String, String> entry : new TreeMap<>(options).entrySet()) {
          digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
          digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
        }
      }

      byte[] hashBytes = digest.digest();
      StringBuilder sb = new StringBuilder(hashBytes.length * 2);
      for (byte b : hashBytes) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }

  private String getControllerUrl() {
    if (_controllerUrl != null) {
      return _controllerUrl;
    }
    if (_helixManager != null) {
      return getControllerBaseUrl(_helixManager);
    }
    throw new RuntimeException("No controller URL configured");
  }

  private MinionClient getMinionClient() {
    // NOTE: using null auth provider here as auth headers injected by caller in "executeDMLStatement()"
    if (_helixManager != null) {
      return new MinionClient(getControllerBaseUrl(_helixManager), null);
    }
    return new MinionClient(_controllerUrl, null);
  }
}
