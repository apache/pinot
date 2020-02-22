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

import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteClusterConfigCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteClusterConfigCommand.class.getName());

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "http port for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-config", required = true, metaVar = "<string>", usage = "Cluster config to delete.")
  private String _config;

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
    return ("UpdateClusterConfig -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
        + " -config " + _config);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Delete Pinot Cluster Config. Sample usage: `pinot-admin.sh DeleteClusterConfig -config pinot.broker.enable.query.limit.override`";
  }

  public DeleteClusterConfigCommand setControllerHost(String host) {
    _controllerHost = host;
    return this;
  }

  public DeleteClusterConfigCommand setControllerPort(String port) {
    _controllerPort = port;
    return this;
  }

  public DeleteClusterConfigCommand setConfig(String config) {
    _config = config;
    return this;
  }

  public String run()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }
    LOGGER.info("Executing command: " + toString());
    if (StringUtils.isEmpty(_config)) {
      throw new UnsupportedOperationException("Empty config: " + _config);
    }
    return sendDeleteRequest(
        String.format("http://%s:%s/cluster/configs/%s", _controllerHost, _controllerPort, _config), null);
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info("Result: " + result);
    return true;
  }
}
