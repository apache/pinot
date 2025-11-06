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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
  public List<String> listTables(String tableType, String taskType,
      String sortType)
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
   * Creates a new table with the specified configuration.
   *
   * @param tableConfig Table configuration as JSON string
   * @param validationTypesToSkip Comma-separated list of validation types to skip
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String createTable(String tableConfig, String validationTypesToSkip)
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
    JsonNode response = _transport.executePut(_controllerAddress, "/tables/" + tableName, tableConfig, null, _headers);
    return response.toString();
  }

  /**
   * Deletes a table.
   *
   * @param tableName Name of the table to delete
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteTable(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/tables/" + tableName, null, _headers);
    return response.toString();
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
  public String rebalanceTable(String tableName, boolean noDowntime, String rebalanceMode,
      Integer minReplicasToKeepAfterRebalance)
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
  public String setTableState(String tableName, String tableType, boolean enabled)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("type", tableType);

    JsonNode response = _transport.executePut(_controllerAddress, "/tables/" + tableName + "/state",
        enabled ? "enable" : "disable", queryParams, _headers);
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

  // Async versions of key methods

  /**
   * Lists all tables in the cluster (async).
   */
  public CompletableFuture<List<String>> listTablesAsync(String tableType, String taskType,
      String sortType) {
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
  public CompletableFuture<String> createTableAsync(String tableConfig, String validationTypesToSkip) {
    Map<String, String> queryParams = new HashMap<>();
    if (validationTypesToSkip != null) {
      queryParams.put("validationTypesToSkip", validationTypesToSkip);
    }

    return _transport.executePostAsync(_controllerAddress, "/tables", tableConfig, queryParams, _headers)
        .thenApply(JsonNode::toString);
  }
}
