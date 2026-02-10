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

/**
 * Client for logical table administration operations.
 */
public class PinotLogicalTableAdminClient {
  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final java.util.Map<String, String> _headers;

  public PinotLogicalTableAdminClient(PinotAdminTransport transport, String controllerAddress,
      java.util.Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  public String createLogicalTable(String logicalTableConfigJson)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/logicalTables", logicalTableConfigJson, null,
        _headers);
    System.out.println("createLogicalTable response: " + response.toString());
    return response.toString();
  }

  public String updateLogicalTable(String logicalTableName, String logicalTableConfigJson)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/logicalTables/" + logicalTableName,
        logicalTableConfigJson, null, _headers);
    return response.toString();
  }

  public String deleteLogicalTable(String logicalTableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/logicalTables/" + logicalTableName, null,
        _headers);
    return response.toString();
  }

  public String getLogicalTable(String logicalTableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/logicalTables/" + logicalTableName, null,
        _headers);
    return response.toString();
  }

  public String listLogicalTables()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/logicalTables", null, _headers);
    return response.toString();
  }
}
