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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.spi.utils.JsonUtils;

/**
 * Client for cluster-level administration operations.
 */
public class ClusterAdminClient extends BaseServiceAdminClient {

  public ClusterAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    super(transport, controllerAddress, headers);
  }

  public String updateClusterConfig(String configJson)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/cluster/configs", configJson, null, _headers);
    return response.toString();
  }

  /**
   * Updates a single cluster configuration entry.
   */
  public String updateClusterConfig(String configName, @Nullable String configValue)
      throws PinotAdminException {
    try {
      return updateClusterConfig(
          PinotAdminTransport.getObjectMapper().writeValueAsString(Collections.singletonMap(configName, configValue)));
    } catch (JsonProcessingException e) {
      throw new PinotAdminException("Failed to serialize cluster config update for: " + configName, e);
    }
  }

  /**
   * Gets all cluster configurations.
   */
  public String getClusterConfigs()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/cluster/configs", null, _headers);
    return response.toString();
  }

  public String deleteClusterConfig(String configName)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/cluster/configs/" + configName, null,
        _headers);
    return response.toString();
  }

  /**
   * Runs a periodic task cluster-wide.
   */
  public String runPeriodicTask(String taskName)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("taskname", taskName);
    JsonNode response = _transport.executeGet(_controllerAddress, "/periodictask/run", queryParams, _headers);
    return response.toString();
  }

  /**
   * Runs a periodic task for a specific table and type.
   */
  public String runPeriodicTask(String taskName, @Nullable String tableName, @Nullable String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("taskname", taskName);
    if (tableName != null) {
      queryParams.put("tableName", tableName);
    }
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    JsonNode response = _transport.executeGet(_controllerAddress, "/periodictask/run", queryParams, _headers);
    return response.toString();
  }

  /**
   * Fetches controller application configurations.
   */
  public PinotAppConfigs getAppConfigs()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/appconfigs", null, _headers);
    try {
      return JsonUtils.jsonNodeToObject(response, PinotAppConfigs.class);
    } catch (IOException e) {
      throw new PinotAdminException("Failed to deserialize controller app configs", e);
    }
  }
}
