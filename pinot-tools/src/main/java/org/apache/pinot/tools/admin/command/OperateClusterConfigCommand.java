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
package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "OperateClusterConfig")
public class OperateClusterConfigCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperateClusterConfigCommand.class.getName());

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "http port for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-controllerProtocol", required = false, metaVar = "<String>", usage = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @Option(name = "-user", required = false, metaVar = "<String>", usage = "Username for basic auth.")
  private String _user;

  @Option(name = "-password", required = false, metaVar = "<String>", usage = "Password for basic auth.")
  private String _password;

  @Option(name = "-authToken", required = false, metaVar = "<String>", usage = "Http auth token.")
  private String _authToken;

  @Option(name = "-config", metaVar = "<string>", usage = "Cluster config to operate.")
  private String _config;

  @Option(name = "-operation", required = true, metaVar = "<string>",
      usage = "Operation to take for Cluster config, currently support GET/ADD/UPDATE/DELETE.")
  private String _operation;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "DeleteClusterConfig";
  }

  @Override
  public String toString() {
    String toString =
        "Operate ClusterConfig -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
            + " -controllerPort " + _controllerPort + " -operation " + _operation;
    if (_config != null) {
      toString += " -config " + _config;
    }
    return toString;
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Operate Pinot Cluster Config. Sample usage: `pinot-admin.sh OperateClusterConfig -operation DELETE "
        + "-config pinot.broker.enable.query.limit.override`";
  }

  public OperateClusterConfigCommand setControllerHost(String host) {
    _controllerHost = host;
    return this;
  }

  public OperateClusterConfigCommand setControllerPort(String port) {
    _controllerPort = port;
    return this;
  }

  public OperateClusterConfigCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public OperateClusterConfigCommand setUser(String user) {
    _user = user;
    return this;
  }

  public OperateClusterConfigCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public OperateClusterConfigCommand setAuthToken(String authToken) {
    _authToken = authToken;
    return this;
  }

  public OperateClusterConfigCommand setConfig(String config) {
    _config = config;
    return this;
  }

  public OperateClusterConfigCommand setOperation(String operation) {
    _operation = operation;
    return this;
  }

  public String run()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }
    LOGGER.info("Executing command: " + toString());
    if (StringUtils.isEmpty(_config) && !_operation.equalsIgnoreCase("GET")) {
      throw new UnsupportedOperationException("Empty config: " + _config);
    }
    String clusterConfigUrl =
        _controllerProtocol + "://" + _controllerHost + ":" + _controllerPort + "/cluster/configs";
    List<Header> headers = makeAuthHeader(makeAuthToken(_authToken, _user, _password));
    switch (_operation.toUpperCase()) {
      case "ADD":
      case "UPDATE":
        String[] splits = _config.split("=");
        if (splits.length != 2) {
          throw new UnsupportedOperationException(
              "Bad config: " + _config + ". Please follow the pattern of [Config Key]=[Config Value]");
        }
        String request = JsonUtils.objectToString(Collections.singletonMap(splits[0], splits[1]));
        return sendRequest("POST", clusterConfigUrl, request, headers);
      case "GET":
        String response = sendRequest("GET", clusterConfigUrl, null, headers);
        JsonNode jsonNode = JsonUtils.stringToJsonNode(response);
        Iterator<String> fieldNamesIterator = jsonNode.fieldNames();
        String results = "";
        while (fieldNamesIterator.hasNext()) {
          String key = fieldNamesIterator.next();
          String value = jsonNode.get(key).textValue();
          results += String.format("%s=%s\n", key, value);
        }
        return results;
      case "DELETE":
        return sendRequest("DELETE", String.format("%s/%s", clusterConfigUrl, _config), null, headers);
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + _operation);
    }
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info(result);
    return true;
  }
}
