/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


/**
 * Class to implement StartController command.
 *
 */
public class StartControllerCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartControllerCommand.class);

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>",
          usage = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Path to directory containging data.")
  private String _dataDir = "/tmp/PinotController";

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
          usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  public StartControllerCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public StartControllerCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  public StartControllerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public StartControllerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  @Override
  public String getName() {
    return "StartController";
  }

  @Override
  public String toString() {
    return ("StartControllerCommand -clusterName " + _clusterName + " -controllerHost " + _controllerHost
            + " -controllerPort " + _controllerPort + " -dataDir " + _dataDir + " -zkAddress " + _zkAddress);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Star the Pinot Controller Process at the specified port.";
  }

  @Override
  public boolean execute() throws Exception {
    final ControllerConf conf = new ControllerConf();

    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }
    conf.setControllerHost(_controllerHost);
    conf.setControllerPort(_controllerPort);
    conf.setDataDir(_dataDir);
    conf.setZkStr(_zkAddress);

    conf.setHelixClusterName(_clusterName);
    conf.setControllerVipHost(_controllerHost);

    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setValidationControllerFrequencyInSeconds(3600);

    LOGGER.info("Executing command: " + toString());
    final ControllerStarter starter = new ControllerStarter(conf);

    starter.start();

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".pinotAdminController.pid");
    return true;
  }
}
