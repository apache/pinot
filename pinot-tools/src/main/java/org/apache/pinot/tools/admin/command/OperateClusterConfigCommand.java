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
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "OperateClusterConfig", mixinStandardHelpOptions = true)
public class OperateClusterConfigCommand extends AbstractDatabaseBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperateClusterConfigCommand.class.getName());

  @CommandLine.Option(names = {"-config"}, description = "Cluster config to operate.")
  private String _config;

  @CommandLine.Option(names = {"-operation"}, required = true,
      description = "Operation to take for Cluster config, currently support GET/ADD/UPDATE/DELETE.")
  private String _operation;

  @Override
  public String getName() {
    return "OperateClusterConfig";
  }

  @Override
  public String toString() {
    String toString =
        "Operate ClusterConfig -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
            + " -controllerPort " + _controllerPort + " -operation " + _operation;
    if (_config != null) {
      toString += " -config " + _config;
    }
    return toString + super.toString();
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Operate Pinot Cluster Config. Sample usage: `pinot-admin.sh OperateClusterConfig -operation DELETE "
        + "-config pinot.broker.enable.query.limit.override`";
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
    LOGGER.info("Executing command: {}", toString());
    if (StringUtils.isEmpty(_config) && !_operation.equalsIgnoreCase("GET")) {
      throw new UnsupportedOperationException("Empty config: " + _config);
    }
    String normalizedOperation = _operation.toUpperCase(Locale.ROOT);
    try (PinotAdminClient adminClient = getPinotAdminClient()) {
      switch (normalizedOperation) {
        case "ADD":
        case "UPDATE":
          String[] splits = _config.split("=");
          if (splits.length != 2) {
            throw new UnsupportedOperationException(
                "Bad config: " + _config + ". Please follow the pattern of [Config Key]=[Config Value]");
          }
          String request = JsonUtils.objectToString(java.util.Collections.singletonMap(splits[0], splits[1]));
          return adminClient.getClusterClient().updateClusterConfig(request);
        case "GET":
          String response = adminClient.getClusterClient().getClusterConfigs();
          JsonNode jsonNode = JsonUtils.stringToJsonNode(response);
          StringBuilder results = new StringBuilder();
          jsonNode.fieldNames().forEachRemaining(key -> {
            String value = jsonNode.get(key).textValue();
            results.append(String.format("%s=%s%n", key, value));
          });
          return results.toString();
        case "DELETE":
          return adminClient.getClusterClient().deleteClusterConfig(_config);
        default:
          throw new UnsupportedOperationException("Unsupported operation: " + _operation);
      }
    } catch (PinotAdminException e) {
      throw new RuntimeException("Failed to operate on cluster config", e);
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
