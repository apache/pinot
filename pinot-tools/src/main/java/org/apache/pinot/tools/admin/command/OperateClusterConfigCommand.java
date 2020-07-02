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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OperateClusterConfigCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperateClusterConfigCommand.class.getName());

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "http port for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-config", metaVar = "<string>", usage = "Cluster config to operate.")
  private String _config;

  @Option(name = "-operation", required = true, metaVar = "<string>", usage = "Operation to take for Cluster config, currently support GET/ADD/UPDATE/DELETE.")
  private String _operation;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
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
    String toString = "Operate ClusterConfig -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
        + " -operation " + _operation;
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
    return "Operate Pinot Cluster Config. Sample usage: `pinot-admin.sh OperateClusterConfig -operation DELETE -config pinot.broker.enable.query.limit.override`";
  }

  public OperateClusterConfigCommand setControllerHost(String host) {
    _controllerHost = host;
    return this;
  }

  public OperateClusterConfigCommand setControllerPort(String port) {
    _controllerPort = port;
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
      _controllerHost = NetUtil.getHostAddress();
    }
    LOGGER.info("Executing command: " + toString());
    if (StringUtils.isEmpty(_config) && !_operation.equalsIgnoreCase("GET")) {
      throw new UnsupportedOperationException("Empty config: " + _config);
    }
    String clusterConfigUrl = "http://" + _controllerHost + ":" + _controllerPort + "/cluster/configs";
    switch (_operation.toUpperCase()) {
      case "ADD":
      case "UPDATE":
        String[] splits = _config.split("=");
        if (splits.length != 2) {
          throw new UnsupportedOperationException(
              "Bad config: " + _config + ". Please follow the pattern of [Config Key]=[Config Value]");
        }
        String request = JsonUtils.objectToString(Collections.singletonMap(splits[0], splits[1]));
        return sendPostRequest(clusterConfigUrl, request);
      case "GET":
        String response = IOUtils.toString(new URI(clusterConfigUrl), StandardCharsets.UTF_8);
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
        return sendDeleteRequest(String.format("%s/%s", clusterConfigUrl, _config), null);
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
