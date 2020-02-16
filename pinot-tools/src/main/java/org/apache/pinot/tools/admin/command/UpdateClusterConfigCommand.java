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

import java.util.Collections;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpdateClusterConfigCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateClusterConfigCommand.class.getName());

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "http port for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-config", required = true, metaVar = "<string>", usage = "Cluster config to update.")
  private String _config;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "UpdateClusterConfig";
  }

  @Override
  public String toString() {
    return ("UpdateClusterConfig -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
        + " -config " + _config);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Update Pinot Cluster Config. Sample usage: `pinot-admin.sh UpdateClusterConfig -config pinot.broker.enable.query.limit.override=true`";
  }

  public UpdateClusterConfigCommand setControllerHost(String host) {
    _controllerHost = host;
    return this;
  }

  public UpdateClusterConfigCommand setControllerPort(String port) {
    _controllerPort = port;
    return this;
  }

  public UpdateClusterConfigCommand setConfig(String config) {
    _config = config;
    return this;
  }

  public String run()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }
    LOGGER.info("Executing command: " + toString());
    String[] splits = _config.split("=");
    if (splits.length != 2) {
      throw new UnsupportedOperationException("Bad config: " + _config + ". Please follow the pattern of [Config Key]=[Config Value]");
    }
    String request = JsonUtils.objectToString(Collections.singletonMap(splits[0], splits[1]));
    return sendPostRequest("http://" + _controllerHost + ":" + _controllerPort + "/cluster/configs", request);
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info("Result: " + result);
    return true;
  }
}
