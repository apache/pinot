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

/**
 * Client for Zookeeper utility endpoints exposed by the controller.
 */
public class PinotZookeeperAdminClient {
  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotZookeeperAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  public String putData(String path, String payload, int expectedVersion, int accessOption)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("path", path);
    queryParams.put("expectedVersion", String.valueOf(expectedVersion));
    queryParams.put("accessOption", String.valueOf(accessOption));
    JsonNode response =
        _transport.executePut(_controllerAddress, "/zk/put", payload, queryParams, _headers);
    return response.toString();
  }

  public String putDataWithQueryParam(String path, String data, int expectedVersion, int accessOption)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("path", path);
    queryParams.put("data", data);
    queryParams.put("expectedVersion", String.valueOf(expectedVersion));
    queryParams.put("accessOption", String.valueOf(accessOption));
    JsonNode response =
        _transport.executePut(_controllerAddress, "/zk/put", null, queryParams, _headers);
    return response.toString();
  }

  public String getData(String path)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("path", path);
    JsonNode response = _transport.executeGet(_controllerAddress, "/zk/get", queryParams, _headers);
    return response.toString();
  }

  public String getChildren(String path)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("path", path);
    JsonNode response = _transport.executeGet(_controllerAddress, "/zk/getChildren", queryParams, _headers);
    return response.toString();
  }

  public String putChildren(String path, String childrenJson, int expectedVersion, int accessOption)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("path", path);
    queryParams.put("expectedVersion", String.valueOf(expectedVersion));
    queryParams.put("accessOption", String.valueOf(accessOption));
    JsonNode response =
        _transport.executePut(_controllerAddress, "/zk/putChildren", childrenJson, queryParams, _headers);
    return response.toString();
  }

  public String delete(String path)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("path", path);
    JsonNode response = _transport.executeDelete(_controllerAddress, "/zk/delete", queryParams, _headers);
    return response.toString();
  }
}
