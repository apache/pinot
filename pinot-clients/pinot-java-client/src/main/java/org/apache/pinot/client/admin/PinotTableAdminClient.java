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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Client for table administration operations.
 * Provides methods to create, update, delete, and manage Pinot tables.
 */
public class PinotTableAdminClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableAdminClient.class);

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotTableAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  /**
   * Lists all tables in the cluster.
   *
   * @param tableType Filter by table type (realtime, offline, dimension)
   * @param taskType Filter by task type
   * @param sortType Sort by (name, creationTime, lastModifiedTime)
   * @return List of table names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listTables(@Nullable String tableType, @Nullable String taskType,
      @Nullable String sortType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (taskType != null) {
      queryParams.put("taskType", taskType);
    }
    if (sortType != null) {
      queryParams.put("sortType", sortType);
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/tables", queryParams, _headers);
    return _transport.parseStringArray(response, "tables");
  }

  /**
   * Gets the configuration for a specific table.
   *
   * @param tableName Name of the table
   * @return Table configuration as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTableConfig(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName, null, _headers);
    return response.toString();
  }

  /**
   * Gets the configuration for a specific table and type (OFFLINE/REALTIME).
   *
   * @param tableName Name of the table (without type suffix)
   * @param tableType Table type string (e.g., "OFFLINE" or "REALTIME")
   * @return Table configuration as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTableConfig(String tableName, @Nullable String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets a specific table view (idealstate/externalview) for a table.
   */
  public String getTableView(String tableName, String view, @Nullable String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("tableType", tableType);
    }
    if (queryParams.isEmpty()) {
      queryParams = null;
    }
    String normalizedView = view.toLowerCase();
    String path;
    if ("idealstate".equals(normalizedView)) {
      path = "/tables/" + tableName + "/idealstate";
    } else if ("externalview".equals(normalizedView)) {
      path = "/tables/" + tableName + "/externalview";
    } else {
      // Fall back to legacy view path for any other custom views.
      path = "/tables/" + tableName + "/view/" + view;
    }
    JsonNode response = _transport.executeGet(_controllerAddress, path, queryParams, _headers);
    return response.toString();
  }

  /**
   * Estimates heap usage for an upsert table.
   *
   * @param cardinality Cardinality of primary key
   * @param primaryKeySize Primary key size in bytes
   * @param numPartitions Number of partitions
   * @param tableAndSchemaConfigJson TableAndSchemaConfig payload
   */
  public String estimateUpsertHeapUsage(long cardinality, int primaryKeySize, int numPartitions,
      String tableAndSchemaConfigJson)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("cardinality", String.valueOf(cardinality));
    queryParams.put("primaryKeySize", String.valueOf(primaryKeySize));
    queryParams.put("numPartitions", String.valueOf(numPartitions));

    JsonNode response = _transport.executePost(_controllerAddress, "/upsert/estimateHeapUsage",
        tableAndSchemaConfigJson, queryParams, _headers);
    return response.toString();
  }

  /**
   * Builds the ingestion URL for ingestFromFile. Intended for callers that need a raw URL for multipart upload.
   */
  public String buildIngestFromFileUrl(String tableNameWithType, Map<String, String> batchConfigMap) {
    String batchConfigMapStr =
        batchConfigMap.entrySet().stream().map(e -> "\"" + e.getKey() + "\":\"" + e.getValue() + "\"")
            .collect(java.util.stream.Collectors.joining(",", "{", "}"));
    String baseUrl = _transport.getScheme() + "://" + _controllerAddress;
    return baseUrl + "/ingestFromFile?tableNameWithType=" + tableNameWithType + "&batchConfigMapStr="
        + URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8);
  }

  /**
   * Builds the ingestion URL for ingestFromURI.
   */
  public String buildIngestFromUriUrl(String tableNameWithType, Map<String, String> batchConfigMap, String sourceUri) {
    String batchConfigMapStr =
        batchConfigMap.entrySet().stream().map(e -> "\"" + e.getKey() + "\":\"" + e.getValue() + "\"")
            .collect(java.util.stream.Collectors.joining(",", "{", "}"));
    String baseUrl = _transport.getScheme() + "://" + _controllerAddress;
    return baseUrl + "/ingestFromURI?tableNameWithType=" + tableNameWithType + "&batchConfigMapStr="
        + URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8) + "&sourceURIStr="
        + URLEncoder.encode(sourceUri, StandardCharsets.UTF_8);
  }

  /**
   * Creates a new table with the specified configuration.
   *
   * @param tableConfig Table configuration as JSON string
   * @param validationTypesToSkip Comma-separated list of validation types to skip
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String createTable(String tableConfig, @Nullable String validationTypesToSkip)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }

    JsonNode response = _transport.executePost(_controllerAddress, "/tables", tableConfig, queryParams, _headers);
    return response.toString();
  }

  /**
   * Updates the configuration of an existing table.
   *
   * @param tableName Name of the table
   * @param tableConfig New table configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateTableConfig(String tableName, String tableConfig)
      throws PinotAdminException {
    return updateTableConfig(tableName, tableConfig, null);
  }

  /**
   * Updates the configuration of an existing table with optional validation skip parameter.
   *
   * @param tableName Name of the table
   * @param tableConfig New table configuration as JSON string
   * @param validationTypesToSkip Comma-separated list of validation types to skip
   */
  public String updateTableConfig(String tableName, String tableConfig, @Nullable String validationTypesToSkip)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }
    JsonNode response = _transport.executePut(_controllerAddress, "/tables/" + tableName, tableConfig,
        queryParams.isEmpty() ? null : queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes a table.
   *
   * @param tableName Name of the table to delete
   * @param tableType Table type (realtime or offline), optional
   * @param retentionPeriod Retention period string (e.g. 12h, 3d), optional
   * @param ignoreActiveTasks Whether to ignore active tasks, optional
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteTable(String tableName, @Nullable String tableType, @Nullable String retentionPeriod,
      @Nullable Boolean ignoreActiveTasks)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (retentionPeriod != null) {
      queryParams.put("retention", retentionPeriod);
    }
    if (ignoreActiveTasks != null) {
      queryParams.put("ignoreActiveTasks", String.valueOf(ignoreActiveTasks));
    }
    if (queryParams.isEmpty()) {
      queryParams = null;
    }
    JsonNode response = _transport.executeDelete(_controllerAddress, "/tables/" + tableName, queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes a table without additional options.
   */
  public String deleteTable(String tableName)
      throws PinotAdminException {
    return deleteTable(tableName, null, null, null);
  }

  /**
   * Validates a table configuration without applying it.
   *
   * @param tableConfig Table configuration to validate as JSON string
   * @return Validation response
   * @throws PinotAdminException If the request fails
   */
  public String validateTableConfig(String tableConfig)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/validate", tableConfig, null, _headers);
    return response.toString();
  }

  /**
   * Rebalances a table (reassigns instances and segments).
   *
   * @param tableName Name of the table to rebalance
   * @param noDowntime Whether to allow rebalance without downtime
   * @param rebalanceMode Rebalance mode (default or specific)
   * @param minReplicasToKeepAfterRebalance Minimum replicas to keep after rebalance
   * @return Rebalance result
   * @throws PinotAdminException If the request fails
   */
  public String rebalanceTable(String tableName, boolean noDowntime, @Nullable String rebalanceMode,
      @Nullable Integer minReplicasToKeepAfterRebalance)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("noDowntime", String.valueOf(noDowntime));
    if (rebalanceMode != null) {
      queryParams.put("rebalanceMode", rebalanceMode);
    }
    if (minReplicasToKeepAfterRebalance != null) {
      queryParams.put("minReplicasToKeepAfterRebalance", String.valueOf(minReplicasToKeepAfterRebalance));
    }

    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/rebalance", null,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Cancels all rebalance jobs for a table.
   *
   * @param tableName Name of the table
   * @return List of cancelled job IDs
   * @throws PinotAdminException If the request fails
   */
  public List<String> cancelRebalance(String tableName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeDelete(_controllerAddress, "/tables/" + tableName + "/rebalance", null, _headers);
    return _transport.parseStringArray(response, "jobIds");
  }

  /**
   * Gets the current state of a table.
   *
   * @param tableName Name of the table
   * @param tableType Table type (realtime or offline)
   * @return Table state
   * @throws PinotAdminException If the request fails
   */
  public String getTableState(String tableName, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("type", tableType);

    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/state", queryParams, _headers);
    return response.get("state").asText();
  }

  /**
   * Enables or disables a table.
   *
   * @param tableName Name of the table
   * @param tableType Table type (realtime or offline)
   * @param enabled Whether to enable or disable the table
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String setTableState(String tableName, @Nullable String tableType, boolean enabled)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    queryParams.put("state", enabled ? "enable" : "disable");

    JsonNode response = _transport.executePut(_controllerAddress, "/tables/" + tableName + "/state",
        null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets statistics for a table.
   *
   * @param tableName Name of the table
   * @return Table statistics
   * @throws PinotAdminException If the request fails
   */
  public String getTableStats(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/stats", null, _headers);
    return response.toString();
  }

  /**
   * Gets the status of a table including ingestion status.
   *
   * @param tableName Name of the table
   * @return Table status
   * @throws PinotAdminException If the request fails
   */
  public String getTableStatus(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/status", null, _headers);
    return response.toString();
  }

  /**
   * Gets the broker or server instances for a table.
   *
   * @param tableName Name of the table
   * @param instanceType Instance type (broker or server)
   */
  public String getTableInstances(String tableName, @Nullable String instanceType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (instanceType != null) {
      queryParams.put("type", instanceType);
    }
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/instances",
        queryParams.isEmpty() ? null : queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets the size breakdown for a table.
   *
   * @param tableName Name of the table
   * @return Table size response as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTableSize(String tableName)
      throws PinotAdminException {
    return getTableSizeDetails(tableName).toString();
  }

  /**
   * Gets the reported table size in bytes.
   *
   * @param tableName Name of the table
   * @return Reported size in bytes
   * @throws PinotAdminException If the request fails
   */
  public long getReportedTableSizeInBytes(String tableName)
      throws PinotAdminException {
    JsonNode response = getTableSizeDetails(tableName);
    JsonNode reportedSize = response.get("reportedSizeInBytes");
    return reportedSize != null ? reportedSize.asLong(0L) : 0L;
  }

  private JsonNode getTableSizeDetails(String tableName)
      throws PinotAdminException {
    return _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/size", null, _headers);
  }

  /**
   * Gets aggregate metadata for all segments of a table.
   *
   * @param tableName Name of the table
   * @return Table metadata
   * @throws PinotAdminException If the request fails
   */
  public String getTableMetadata(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/metadata", null, _headers);
    return response.toString();
  }

  /**
   * Gets aggregate valid document IDs metadata for all segments of a table.
   *
   * @param tableName Name of the table
   * @return Valid document IDs metadata
   * @throws PinotAdminException If the request fails
   */
  public String getTableValidDocIdsMetadata(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/validDocIdsMetadata",
        null, _headers);
    return response.toString();
  }

  /**
   * Gets aggregate index details for all segments of a table.
   *
   * @param tableName Name of the table
   * @return Table indexes
   * @throws PinotAdminException If the request fails
   */
  public String getTableIndexes(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/indexes", null, _headers);
    return response.toString();
  }

  /**
   * Pauses consumption for a realtime table.
   */
  public String pauseConsumption(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/pauseConsumption",
        null, null, _headers);
    return response.toString();
  }

  /**
   * Retrieves aggregate metadata for a table, optionally scoped to specific columns.
   *
   * @param tableNameWithType Table name with type suffix
   * @param columns Comma-separated list of columns to aggregate (optional)
   * @return Aggregate metadata as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getAggregateMetadata(String tableNameWithType, @Nullable String columns)
      throws PinotAdminException {
    Map<String, String> queryParams = null;
    if (columns != null && !columns.isEmpty()) {
      queryParams = Map.of("columns", columns);
    }
    JsonNode response = _transport.executeGet(_controllerAddress,
        "/tables/" + tableNameWithType + "/aggregateMetadata", queryParams, _headers);
    return response.toString();
  }

  /**
   * Retrieves valid doc ids metadata for a table.
   *
   * @param tableNameWithType Table name with type suffix
   * @param validDocIdsType ValidDocIdsType enum name
   * @return Valid doc ids metadata as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getValidDocIdsMetadata(String tableNameWithType, String validDocIdsType)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("validDocIdsType", validDocIdsType);
    JsonNode response = _transport.executeGet(_controllerAddress,
        "/tables/" + tableNameWithType + "/validDocIdsMetadata", queryParams, _headers);
    return response.toString();
  }

  /**
   * Resumes consumption for a realtime table.
   * @param tableName table name (no type suffix)
   * @param consumeFrom optional offset criteria, e.g. "smallest", "largest", "lastConsumed"
   */
  public String resumeConsumption(String tableName, @Nullable String consumeFrom)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (consumeFrom != null) {
      queryParams.put("consumeFrom", consumeFrom);
    }
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/resumeConsumption",
        null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets pause status details for a realtime table.
   */
  public String getPauseStatus(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/pauseStatus",
        null, _headers);
    return response.toString();
  }

  /**
   * Sets the time boundary for a hybrid table based on offline segments' metadata.
   *
   * @param tableName Name of the hybrid table (without type suffix)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String setTimeBoundary(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/timeBoundary", null,
        null, _headers);
    return response.toString();
  }

  /**
   * Forces committing consuming segments for a realtime table.
   */
  public String forceCommit(String tableName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executePost(_controllerAddress, "/tables/" + tableName + "/forceCommit", null, null, _headers);
    return response.toString();
  }

  /**
   * Forces committing consuming segments with batch controls.
   *
   * @param tableName raw table name
   * @param batchSize batch size for status checks
   * @param batchStatusCheckIntervalSec interval between status checks
   * @param batchStatusCheckTimeoutSec timeout for status checks
   */
  public String forceCommit(String tableName, int batchSize, int batchStatusCheckIntervalSec,
      int batchStatusCheckTimeoutSec)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("batchSize", String.valueOf(batchSize));
    queryParams.put("batchStatusCheckIntervalSec", String.valueOf(batchStatusCheckIntervalSec));
    queryParams.put("batchStatusCheckTimeoutSec", String.valueOf(batchStatusCheckTimeoutSec));
    JsonNode response =
        _transport.executePost(_controllerAddress, "/tables/" + tableName + "/forceCommit", null, queryParams,
            _headers);
    return response.toString();
  }

  /**
   * Gets the status for a force-commit job.
   */
  public String getForceCommitJobStatus(String jobId)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("jobId", jobId);
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tables/forceCommitStatus", queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes the time boundary for a hybrid table.
   *
   * @param tableName Name of the hybrid table (without type suffix)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteTimeBoundary(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/tables/" + tableName + "/timeBoundary",
        null, _headers);
    return response.toString();
  }

  /**
   * Gets recommended configuration for a table.
   *
   * @param inputJson Input configuration for recommendation
   * @return Recommended configuration
   * @throws PinotAdminException If the request fails
   */
  public String recommendConfig(String inputJson)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/tables/recommender", inputJson, null, _headers);
    return response.asText();
  }

  /**
   * Gets external view for a table resource.
   */
  public String getExternalView(String tableResourceName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableResourceName + "/externalview",
        null, _headers);
    return response.toString();
  }

  /**
   * Gets ideal state for a table resource.
   */
  public String getIdealState(String tableResourceName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableResourceName + "/idealstate",
        null, _headers);
    return response.toString();
  }

  /**
   * Gets consuming segments info for a table.
   */
  public String getConsumingSegmentsInfo(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tables/" + tableName + "/consumingSegmentsInfo",
        null, _headers);
    return response.toString();
  }

  /**
   * Advanced rebalance API mirroring controller REST query params.
   */
  public String rebalanceTable(String tableName, String tableType, boolean dryRun, boolean reassignInstances,
      boolean includeConsuming, boolean downtime, int minAvailableReplicas)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("type", tableType);
    queryParams.put("dryRun", String.valueOf(dryRun));
    queryParams.put("reassignInstances", String.valueOf(reassignInstances));
    queryParams.put("includeConsuming", String.valueOf(includeConsuming));
    queryParams.put("downtime", String.valueOf(downtime));
    queryParams.put("minAvailableReplicas", String.valueOf(minAvailableReplicas));
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/rebalance", null,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * =========================
   * TableConfigs APIs
   * =========================
   */

  /**
   * Lists all TableConfigs (raw table names) in the cluster.
   */
  public List<String> listTableConfigs()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tableConfigs", null, _headers);
    if (!response.isArray()) {
      throw new PinotAdminException("Unexpected response for listTableConfigs: " + response);
    }
    List<String> result = new ArrayList<>();
    for (JsonNode n : response) {
      result.add(n.asText());
    }
    return result;
  }

  /**
   * Gets the TableConfigs (schema + offline/real-time configs) for a raw table name.
   */
  public String getTableConfigs(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tableConfigs/" + tableName, null, _headers);
    return response.toString();
  }

  /**
   * Creates TableConfigs (schema + table configs). Supports skipping validations and ignoring active tasks.
   *
   * @param tableConfigsJson TableConfigs JSON payload
   * @param validationTypesToSkip Optional comma-separated validation types to skip (ALL|TASK|UPSERT)
   * @param ignoreActiveTasks Optional flag to ignore active tasks during creation
   */
  public String createTableConfigs(String tableConfigsJson, @Nullable String validationTypesToSkip,
      @Nullable Boolean ignoreActiveTasks)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }
    if (ignoreActiveTasks != null) {
      queryParams.put("ignoreActiveTasks", String.valueOf(ignoreActiveTasks));
    }
    if (queryParams.isEmpty()) {
      queryParams = null;
    }
    JsonNode response =
        _transport.executePost(_controllerAddress, "/tableConfigs", tableConfigsJson, queryParams, _headers);
    return response.toString();
  }

  /**
   * Updates TableConfigs for a raw table. Supports skipping validations, reload, and forcing schema update.
   *
   * @param tableName Raw table name
   * @param tableConfigsJson Updated TableConfigs JSON payload
   * @param validationTypesToSkip Optional comma-separated validation types to skip (ALL|TASK|UPSERT)
   * @param reload Whether to reload the table if schema change is backward compatible
   * @param forceTableSchemaUpdate Whether to force schema update
   */
  public String updateTableConfigs(String tableName, String tableConfigsJson, @Nullable String validationTypesToSkip,
      boolean reload, boolean forceTableSchemaUpdate)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }
    queryParams.put("reload", String.valueOf(reload));
    queryParams.put("forceTableSchemaUpdate", String.valueOf(forceTableSchemaUpdate));
    JsonNode response = _transport.executePut(_controllerAddress, "/tableConfigs/" + tableName, tableConfigsJson,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes TableConfigs for a raw table name. Optionally ignore active tasks during cleanup.
   */
  public String deleteTableConfigs(String tableName, @Nullable Boolean ignoreActiveTasks)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (ignoreActiveTasks != null) {
      queryParams.put("ignoreActiveTasks", String.valueOf(ignoreActiveTasks));
    }
    if (queryParams.isEmpty()) {
      queryParams = null;
    }
    JsonNode response =
        _transport.executeDelete(_controllerAddress, "/tableConfigs/" + tableName, queryParams, _headers);
    return response.toString();
  }

  /**
   * Validates a TableConfigs payload without applying it.
   *
   * @param tableConfigsJson TableConfigs JSON payload
   * @param validationTypesToSkip Optional comma-separated validation types to skip (ALL|TASK|UPSERT)
   */
  public String validateTableConfigs(String tableConfigsJson, @Nullable String validationTypesToSkip)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }
    JsonNode response =
        _transport.executePost(_controllerAddress, "/tableConfigs/validate", tableConfigsJson, queryParams, _headers);
    return response.toString();
  }

  // Async versions of key methods

  /**
   * Lists all tables in the cluster (async).
   */
  public CompletableFuture<List<String>> listTablesAsync(@Nullable String tableType, @Nullable String taskType,
      @Nullable String sortType) {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (taskType != null) {
      queryParams.put("taskType", taskType);
    }
    if (sortType != null) {
      queryParams.put("sortType", sortType);
    }

    return _transport.executeGetAsync(_controllerAddress, "/tables", queryParams, _headers)
        .thenApply(response -> _transport.parseStringArraySafe(response, "tables"));
  }

  /**
   * Gets the configuration for a specific table (async).
   */
  public CompletableFuture<String> getTableConfigAsync(String tableName) {
    return _transport.executeGetAsync(_controllerAddress, "/tables/" + tableName, null, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Creates a new table with the specified configuration (async).
   */
  public CompletableFuture<String> createTableAsync(String tableConfig, @Nullable String validationTypesToSkip) {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }

    return _transport.executePostAsync(_controllerAddress, "/tables", tableConfig, queryParams, _headers)
        .thenApply(JsonNode::toString);
  }
}
