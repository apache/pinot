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
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Client for cluster-level administration operations.
 */
public class PinotClusterAdminClient {
  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotClusterAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  public String updateClusterConfig(String configJson)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/cluster/configs", configJson, null, _headers);
    return response.toString();
  }

  /**
   * Gets all cluster configurations.
   */
  public String getClusterConfigs()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/cluster/configs", null, _headers);
    return response.toString();
  }

  /**
   * Gets a specific cluster config by name.
   */
  public String getClusterConfig(String configName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/cluster/configs/" + configName, null, _headers);
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
    Map<String, String> queryParams = new java.util.HashMap<>();
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
   * Gets table rebalance job status by job id.
   */
  public String getRebalanceStatus(String jobId)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/rebalanceStatus/" + jobId, null, _headers);
    return response.toString();
  }

  /**
   * Cancels a query by client id.
   */
  public String cancelQueryByClientId(String clientQueryId)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/clientQuery/" + clientQueryId, null,
        _headers);
    return response.toString();
  }

  /**
   * Fetches controller application configurations.
   */
  public String getAppConfigs()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/appconfigs", null, _headers);
    return response.toString();
  }
}
