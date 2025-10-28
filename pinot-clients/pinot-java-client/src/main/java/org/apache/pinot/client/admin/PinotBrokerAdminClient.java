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
 * Client for broker-related administration operations.
 */
public class PinotBrokerAdminClient {
  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotBrokerAdminClient(PinotAdminTransport transport, String controllerAddress, Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  public String getBrokers(@Nullable String state)
      throws PinotAdminException {
    Map<String, String> queryParams = state != null ? Map.of("state", state) : null;
    JsonNode response = _transport.executeGet(_controllerAddress, "/brokers", queryParams, _headers);
    return response.toString();
  }

  public String getBrokerTenants(@Nullable String state)
      throws PinotAdminException {
    Map<String, String> queryParams = state != null ? Map.of("state", state) : null;
    JsonNode response = _transport.executeGet(_controllerAddress, "/brokers/tenants", queryParams, _headers);
    return response.toString();
  }

  public String getBrokerTenant(String tenant, @Nullable String state)
      throws PinotAdminException {
    Map<String, String> queryParams = state != null ? Map.of("state", state) : null;
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/brokers/tenants/" + tenant, queryParams, _headers);
    return response.toString();
  }

  public String getBrokerTables(@Nullable String state)
      throws PinotAdminException {
    Map<String, String> queryParams = state != null ? Map.of("state", state) : null;
    JsonNode response = _transport.executeGet(_controllerAddress, "/brokers/tables", queryParams, _headers);
    return response.toString();
  }

  public String getBrokerTable(String tableName, @Nullable String tableType, @Nullable String state)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (state != null) {
      queryParams.put("state", state);
    }
    Map<String, String> paramsToSend = queryParams.isEmpty() ? null : queryParams;
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/brokers/tables/" + tableName, paramsToSend, _headers);
    return response.toString();
  }
}
