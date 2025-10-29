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
 * Client for instance administration operations.
 * Provides methods to create, update, delete, and manage Pinot instances.
 */
public class PinotInstanceAdminClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceAdminClient.class);

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotInstanceAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  /**
   * Lists all instances in the cluster.
   *
   * @return List of instance names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listInstances()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/instances", null, _headers);
    return _transport.parseStringArray(response, "instances");
  }

  /**
   * Lists all live instances in the cluster.
   *
   * @return List of live instance names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listLiveInstances()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/liveinstances", null, _headers);
    return _transport.parseStringArray(response, "liveInstances");
  }

  /**
   * Gets information about a specific instance.
   *
   * @param instanceName Name of the instance
   * @return Instance information as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getInstance(String instanceName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/instances/" + instanceName, null, _headers);
    return response.toString();
  }

  /**
   * Creates a new instance.
   *
   * @param instanceConfig Instance configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String createInstance(String instanceConfig)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/instances", instanceConfig, null, _headers);
    return response.toString();
  }

  /**
   * Enables or disables an instance.
   *
   * @param instanceName Name of the instance
   * @param enabled Whether to enable or disable the instance
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String setInstanceState(String instanceName, boolean enabled)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/instances/" + instanceName + "/state",
        enabled ? "enable" : "disable", null, _headers);
    return response.toString();
  }

  /**
   * Enables, disables, or drops an instance.
   *
   * @param instanceName Name of the instance
   * @param state State to set (enable, disable, or drop)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String setInstanceState(String instanceName, String state)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/instances/" + instanceName + "/state",
        state, null, _headers);
    return response.toString();
  }

  /**
   * Drops (deletes) an instance.
   *
   * @param instanceName Name of the instance to drop
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String dropInstance(String instanceName)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/instances/" + instanceName, null, _headers);
    return response.toString();
  }

  /**
   * Updates an existing instance.
   *
   * @param instanceName Name of the instance to update
   * @param instanceConfig New instance configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateInstance(String instanceName, String instanceConfig)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/instances/" + instanceName, instanceConfig,
        null, _headers);
    return response.toString();
  }

  /**
   * Updates the tags of a specific instance.
   *
   * @param instanceName Name of the instance
   * @param tagUpdateRequest Tag update request as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateInstanceTags(String instanceName, String tagUpdateRequest)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/instances/" + instanceName + "/updateTags",
        tagUpdateRequest, null, _headers);
    return response.toString();
  }

  /**
   * Updates the tables served by a broker instance in the broker resource.
   *
   * @param instanceName Name of the broker instance
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateBrokerResource(String instanceName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executePost(_controllerAddress, "/instances/" + instanceName + "/updateBrokerResource",
            null, null, _headers);
    return response.toString();
  }

  /**
   * Validates whether it's safe to drop the given instances.
   *
   * @param instanceNames Comma-separated list of instance names to validate
   * @return Validation response as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String validateDropInstances(String instanceNames)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("instanceNames", instanceNames);

    JsonNode response = _transport.executeGet(_controllerAddress, "/instances/dropInstance/validate",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Validates whether it's safe to update the tags of the given instances.
   *
   * @param instanceNames Comma-separated list of instance names to validate
   * @param newTags New tags to assign
   * @return Validation response as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String validateUpdateInstanceTags(String instanceNames, String newTags)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("instanceNames", instanceNames);
    queryParams.put("newTags", newTags);

    JsonNode response = _transport.executePost(_controllerAddress, "/instances/updateTags/validate",
        null, queryParams, _headers);
    return response.toString();
  }

  // Async versions of key methods

  /**
   * Lists all instances in the cluster (async).
   */
  public CompletableFuture<List<String>> listInstancesAsync() {
    return _transport.executeGetAsync(_controllerAddress, "/instances", null, _headers)
        .thenApply(response -> _transport.parseStringArraySafe(response, "instances"));
  }

  /**
   * Gets information about a specific instance (async).
   */
  public CompletableFuture<String> getInstanceAsync(String instanceName) {
    return _transport.executeGetAsync(_controllerAddress, "/instances/" + instanceName, null, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Creates a new instance (async).
   */
  public CompletableFuture<String> createInstanceAsync(String instanceConfig) {
    return _transport.executePostAsync(_controllerAddress, "/instances", instanceConfig, null, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Enables or disables an instance (async).
   */
  public CompletableFuture<String> setInstanceStateAsync(String instanceName, boolean enabled) {
    return _transport.executePutAsync(_controllerAddress, "/instances/" + instanceName + "/state",
            enabled ? "enable" : "disable", null, _headers)
        .thenApply(JsonNode::toString);
  }
}
