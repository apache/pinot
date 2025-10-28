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
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Admin client for user management endpoints.
 */
public class PinotUserAdminClient {
  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  PinotUserAdminClient(PinotAdminTransport transport, String controllerAddress, Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  public String createUser(String userConfigJson)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/users", userConfigJson, null, _headers);
    return response.toString();
  }

  public String getUser(String username, String componentType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("component", componentType);
    JsonNode response = _transport.executeGet(_controllerAddress, "/users/" + username, queryParams, _headers);
    return response.toString();
  }

  public String updateUser(String username, String componentType, boolean passwordChanged, String userConfigJson)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("component", componentType);
    queryParams.put("passwordChanged", String.valueOf(passwordChanged));
    JsonNode response =
        _transport.executePut(_controllerAddress, "/users/" + username, userConfigJson, queryParams, _headers);
    return response.toString();
  }

  public String deleteUser(String username, @Nullable String componentType)
      throws PinotAdminException {
    Map<String, String> queryParams = null;
    if (componentType != null) {
      queryParams = Map.of("component", componentType);
    }
    JsonNode response = _transport.executeDelete(_controllerAddress, "/users/" + username, queryParams, _headers);
    return response.toString();
  }
}
