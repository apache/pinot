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
 * Client for tenant administration operations.
 * Provides methods to create, update, delete, and manage Pinot tenants.
 */
public class PinotTenantAdminClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTenantAdminClient.class);

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotTenantAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  /**
   * Lists all tenants in the cluster.
   *
   * @return List of tenant names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listTenants()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants", null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("tenants"), List.class);
  }

  /**
   * Creates a new tenant.
   *
   * @param tenantConfig Tenant configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String createTenant(String tenantConfig)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/tenants", tenantConfig, null, _headers);
    return response.toString();
  }

  /**
   * Updates an existing tenant.
   *
   * @param tenantConfig Updated tenant configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateTenant(String tenantConfig)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/tenants", tenantConfig, null, _headers);
    return response.toString();
  }

  /**
   * Gets instances for a specific tenant.
   *
   * @param tenantName Name of the tenant
   * @param tenantType Tenant type (server or broker)
   * @param tableType Table type (offline or realtime)
   * @return Tenant instances as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTenantInstances(String tenantName, String tenantType, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tenantType != null) {
      queryParams.put("type", tenantType);
    }
    if (tableType != null) {
      queryParams.put("tableType", tableType);
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants/" + tenantName, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets instances for a specific tenant (server type by default).
   *
   * @param tenantName Name of the tenant
   * @return Tenant instances as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTenantInstances(String tenantName)
      throws PinotAdminException {
    return getTenantInstances(tenantName, null, null);
  }

  /**
   * Enables or disables a tenant.
   *
   * @param tenantName Name of the tenant
   * @param tenantType Tenant type (server or broker)
   * @param state State to set (enable or disable)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String setTenantState(String tenantName, String tenantType, String state)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tenantType != null) {
      queryParams.put("type", tenantType);
    }
    queryParams.put("state", state);

    JsonNode response =
        _transport.executePut(_controllerAddress, "/tenants/" + tenantName, null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets tables on a server or broker tenant.
   *
   * @param tenantName Name of the tenant
   * @param tenantType Tenant type (server or broker)
   * @param withTableProperties Whether to include table properties
   * @return Tenant tables as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTenantTables(String tenantName, String tenantType, boolean withTableProperties)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tenantType != null) {
      queryParams.put("type", tenantType);
    }
    queryParams.put("withTableProperties", String.valueOf(withTableProperties));

    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants/" + tenantName + "/tables",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets the instance partitions of a tenant.
   *
   * @param tenantName Name of the tenant
   * @param instancePartitionType Instance partition type (OFFLINE, CONSUMING, COMPLETED)
   * @return Instance partitions as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getInstancePartitions(String tenantName, String instancePartitionType)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("instancePartitionType", instancePartitionType);

    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants/" + tenantName + "/instancePartitions",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Updates an instance partition for a server type in a tenant.
   *
   * @param tenantName Name of the tenant
   * @param instancePartitionType Instance partition type (OFFLINE, CONSUMING, COMPLETED)
   * @param instancePartitionsConfig Instance partitions configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateInstancePartitions(String tenantName, String instancePartitionType,
      String instancePartitionsConfig)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("instancePartitionType", instancePartitionType);

    JsonNode response = _transport.executePut(_controllerAddress, "/tenants/" + tenantName + "/instancePartitions",
        instancePartitionsConfig, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets tenant metadata information.
   *
   * @param tenantName Name of the tenant
   * @param tenantType Tenant type (optional)
   * @return Tenant metadata as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTenantMetadata(String tenantName, String tenantType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tenantType != null) {
      queryParams.put("type", tenantType);
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants/" + tenantName + "/metadata",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Changes tenant state.
   *
   * @param tenantName Name of the tenant
   * @param tenantType Tenant type (optional)
   * @param state New state (enable, disable, drop)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String changeTenantState(String tenantName, String tenantType, String state)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tenantType != null) {
      queryParams.put("type", tenantType);
    }
    queryParams.put("state", state);

    JsonNode response = _transport.executePut(_controllerAddress, "/tenants/" + tenantName + "/metadata",
        null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes a tenant.
   *
   * @param tenantName Name of the tenant
   * @param tenantType Tenant type (SERVER or BROKER)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteTenant(String tenantName, String tenantType)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("type", tenantType);

    JsonNode response = _transport.executeDelete(_controllerAddress, "/tenants/" + tenantName, queryParams, _headers);
    return response.toString();
  }

  /**
   * Cancels a running tenant rebalance job.
   *
   * @param jobId Job ID of the rebalance job
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String cancelRebalance(String jobId)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/tenants/rebalance/" + jobId, null, _headers);
    return response.toString();
  }

  /**
   * Rebalances all tables that are part of the tenant.
   *
   * @param tenantName Name of the tenant
   * @param degreeOfParallelism Number of table rebalance jobs allowed to run at the same time
   * @param includeTableTypes Comma-separated list of table types to include (optional)
   * @param excludeTableTypes Comma-separated list of table types to exclude (optional)
   * @param rebalanceMode Rebalance mode (optional)
   * @return Rebalance result
   * @throws PinotAdminException If the request fails
   */
  public String rebalanceTenant(String tenantName, int degreeOfParallelism, String includeTableTypes,
      String excludeTableTypes, String rebalanceMode)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("degreeOfParallelism", String.valueOf(degreeOfParallelism));
    if (includeTableTypes != null) {
      queryParams.put("includeTableTypes", includeTableTypes);
    }
    if (excludeTableTypes != null) {
      queryParams.put("excludeTableTypes", excludeTableTypes);
    }
    if (rebalanceMode != null) {
      queryParams.put("rebalanceMode", rebalanceMode);
    }

    JsonNode response = _transport.executePost(_controllerAddress, "/tenants/" + tenantName + "/rebalance",
        null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets detailed stats of a tenant rebalance operation.
   *
   * @param jobId Tenant rebalance job ID
   * @return Rebalance status as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getRebalanceStatus(String jobId)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants/rebalanceStatus/" + jobId, null, _headers);
    return response.toString();
  }

  /**
   * Gets list of rebalance jobs for a tenant.
   *
   * @param tenantName Name of the tenant
   * @return Rebalance jobs as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getRebalanceJobs(String tenantName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tenants/" + tenantName + "/rebalanceJobs",
        null, _headers);
    return response.toString();
  }

  // Async versions of key methods

  /**
   * Lists all tenants in the cluster (async).
   */
  public CompletableFuture<List<String>> listTenantsAsync() {
    return _transport.executeGetAsync(_controllerAddress, "/tenants", null, _headers)
        .thenApply(response -> PinotAdminTransport.getObjectMapper().convertValue(response.get("tenants"), List.class));
  }

  /**
   * Creates a new tenant (async).
   */
  public CompletableFuture<String> createTenantAsync(String tenantConfig) {
    return _transport.executePostAsync(_controllerAddress, "/tenants", tenantConfig, null, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Gets tenant metadata information (async).
   */
  public CompletableFuture<String> getTenantMetadataAsync(String tenantName, String tenantType) {
    Map<String, String> queryParams = new HashMap<>();
    if (tenantType != null) {
      queryParams.put("type", tenantType);
    }

    return _transport.executeGetAsync(_controllerAddress, "/tenants/" + tenantName + "/metadata", queryParams, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Deletes a tenant (async).
   */
  public CompletableFuture<String> deleteTenantAsync(String tenantName, String tenantType) {
    Map<String, String> queryParams = Map.of("type", tenantType);

    return _transport.executeDeleteAsync(_controllerAddress, "/tenants/" + tenantName, queryParams, _headers)
        .thenApply(JsonNode::toString);
  }
}
