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
package org.apache.pinot.connector.flink.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotControllerClient extends HttpClient {

  public enum TableType {
    REALTIME("realtime"), OFFLINE("offline");

    private final String _type;

    TableType(String t) {
      _type = t;
    }

    @Override
    @JsonValue
    public String toString() {
      return _type;
    }

    @JsonCreator
    public static TableType valueOfByType(String type) {
      return valueOf(type.toUpperCase());
    }
  }

  public static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerClient.class);
  public static final String TYPE = "type";
  public static final String DEFAULT_ADDRESS = "http://localhost:9000";
  protected static final String CONTROLLER_PREFIX = "Controller_";

  private final String _address;

  public PinotControllerClient() {
    this(DEFAULT_ADDRESS);
  }

  public PinotControllerClient(String address) {
    super();
    _address = address;
  }

  @Override
  public String getAddress() {
    return _address;
  }

  public List<String> getControllerInstances(MultivaluedMap<String, Object> headers) {
    Response res = get("/instances", headers, new HashMap<>());

    // instances not found
    if (res.getStatus() != 200 || res.getEntity() == null) {
      return new ArrayList<>();
    }

    final Map<String, Object> resEntityMap = res.readEntity(Map.class);
    List<String> instancesStr = (List<String>) resEntityMap.get("instances");
    if (instancesStr == null || instancesStr.isEmpty()) {
      return new ArrayList<>();
    }
    List<String> controllerInstancesStr = new ArrayList<>();
    for (String instanceStr : instancesStr) {
      // the instance is in the format of Controller_host_port e.g.
      // Controller_streampinot-prod02-phx2_5983
      // trim the prefix and replace the underscore with colon
      if (instanceStr.startsWith(CONTROLLER_PREFIX)) {
        String address = instanceStr.substring(CONTROLLER_PREFIX.length());
        StringBuffer buffer = new StringBuffer(address);
        address = buffer.reverse().toString().replaceFirst("_", ":");
        address = new StringBuffer(address).reverse().toString();
        controllerInstancesStr.add(address);
      }
    }
    return controllerInstancesStr;
  }

  public String getSchemaStrFromController(String schemaName, MultivaluedMap<String, Object> headers)
      throws Exception {
    LOGGER.info("get Pinot schema for {}", schemaName);
    Response res = get("/schemas/" + schemaName, headers, new HashMap<>());

    // schema not found
    if (res.getStatus() != 200 || res.getEntity() == null) {
      return null;
    }

    final Map<String, Object> resEntityMap = res.readEntity(Map.class);
    return JsonUtils.objectToString(resEntityMap);
  }

  public String getPinotConfigStrFromController(String tableName, TableType tableType,
      MultivaluedMap<String, Object> headers)
      throws Exception {
    LOGGER.info("get {} Pinot table config for {}", tableType.toString(), tableName);
    Map<String, List<String>> queryParams = new HashMap<>();
    setTableType(queryParams, tableType);
    Response res = get("/tables/" + tableName, headers, queryParams);

    // Pinot controller returns 200 status code and empty response body for non existing TableConfig
    // status code is already checked
    if (res.getEntity() == null) {
      return null;
    }
    final Map<String, Object> resEntityMap = res.readEntity(Map.class);
    if (resEntityMap == null || resEntityMap.isEmpty()) {
      return null;
    }
    final Object configFromController = resEntityMap.get(tableType.toString().toUpperCase());
    return configFromController != null ? JsonUtils.objectToString(configFromController) : null;
  }

  static void setTableType(Map<String, List<String>> queryParams, TableType tableType) {
    queryParams.put(TYPE, Arrays.asList(tableType.toString()));
  }
}
