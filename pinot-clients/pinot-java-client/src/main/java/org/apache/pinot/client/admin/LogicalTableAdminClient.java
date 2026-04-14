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
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.JsonUtils;

/**
 * Client for logical table administration operations.
 */
public class LogicalTableAdminClient extends BaseServiceAdminClient {

  public LogicalTableAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    super(transport, controllerAddress, headers);
  }

  public String createLogicalTable(String logicalTableConfigJson)
      throws PinotAdminException {
    return createLogicalTable(logicalTableConfigJson, null);
  }

  public String createLogicalTable(String logicalTableConfigJson, @Nullable Map<String, String> headers)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/logicalTables", logicalTableConfigJson, null,
        mergeHeaders(headers));
    return response.toString();
  }

  public String updateLogicalTable(String logicalTableName, String logicalTableConfigJson)
      throws PinotAdminException {
    return updateLogicalTable(logicalTableName, logicalTableConfigJson, null);
  }

  public String updateLogicalTable(String logicalTableName, String logicalTableConfigJson,
      @Nullable Map<String, String> headers)
      throws PinotAdminException {
    JsonNode response = _transport.executePut(_controllerAddress, "/logicalTables/" + logicalTableName,
        logicalTableConfigJson, null, mergeHeaders(headers));
    return response.toString();
  }

  public String deleteLogicalTable(String logicalTableName)
      throws PinotAdminException {
    return deleteLogicalTable(logicalTableName, null);
  }

  public String deleteLogicalTable(String logicalTableName, @Nullable Map<String, String> headers)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/logicalTables/" + logicalTableName, null,
        mergeHeaders(headers));
    return response.toString();
  }

  public String getLogicalTable(String logicalTableName)
      throws PinotAdminException {
    return getLogicalTable(logicalTableName, (Map<String, String>) null);
  }

  public String getLogicalTable(String logicalTableName, @Nullable Map<String, String> headers)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/logicalTables/" + logicalTableName, null,
        mergeHeaders(headers));
    return response.toString();
  }

  /**
   * Gets a logical table configuration.
   */
  public LogicalTableConfig getLogicalTableConfig(String logicalTableName)
      throws PinotAdminException {
    return getLogicalTableConfig(logicalTableName, null);
  }

  /**
   * Gets a logical table configuration with optional extra headers.
   */
  public LogicalTableConfig getLogicalTableConfig(String logicalTableName, @Nullable Map<String, String> headers)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/logicalTables/" + logicalTableName, null,
        mergeHeaders(headers));
    try {
      return JsonUtils.jsonNodeToObject(response, LogicalTableConfig.class);
    } catch (IOException e) {
      throw new PinotAdminException("Failed to deserialize logical table: " + logicalTableName, e);
    }
  }

  public String listLogicalTables()
      throws PinotAdminException {
    return listLogicalTables(null);
  }

  public String listLogicalTables(@Nullable Map<String, String> headers)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/logicalTables", null, mergeHeaders(headers));
    return response.toString();
  }
}
