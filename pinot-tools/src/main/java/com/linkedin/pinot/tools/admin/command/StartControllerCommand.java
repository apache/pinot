/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.tools.Command;
import java.io.File;
import org.apache.commons.configuration.ConfigurationException;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartController command.
 *
 */
public class StartControllerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartControllerCommand.class);

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>",
      usage = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Path to directory containging data.")
  private String _dataDir = TMP_DIR + "PinotController";

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @Option(name = "-configFileName", required = false, metaVar = "<FilePathName>",
      usage = "Controller Starter config file",
      forbids = { "-controllerHost", "-controllerPort", "-dataDir", "-zkAddress", "-clusterName" })
  private String _configFileName;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  // This can be set via the set method, or via config file input.
  private boolean _tenantIsolation = true;

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

  public StartControllerCommand setTenantIsolation(boolean tenantIsolation) {
    _tenantIsolation = tenantIsolation;
    return this;
  }

  public StartControllerCommand setConfigFileName(String configFileName) {
    _configFileName = configFileName;
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
    if (_configFileName != null) {
      return ("StartController -configFileName " + _configFileName);
    } else {
      return ("StartController -clusterName " + _clusterName + " -controllerHost " + _controllerHost
          + " -controllerPort " + _controllerPort + " -dataDir " + _dataDir + " -zkAddress " + _zkAddress);
    }
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Start the Pinot Controller Process at the specified port.";
  }

  @Override
  public boolean execute() throws Exception {
    try {
      if (_controllerHost == null) {
        _controllerHost = NetUtil.getHostAddress();
      }

      ControllerConf conf = readConfigFromFile(_configFileName);
      if (conf == null) {
        if (_configFileName != null) {
          LOGGER.error("Error: Unable to find file {}.", _configFileName);
          return false;
        }

        conf = new ControllerConf();

        conf.setControllerHost(_controllerHost);
        conf.setControllerPort(_controllerPort);
        conf.setDataDir(_dataDir);
        conf.setZkStr(_zkAddress);

        conf.setHelixClusterName(_clusterName);
        conf.setControllerVipHost(_controllerHost);
        conf.setTenantIsolationEnabled(_tenantIsolation);

        conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
        conf.setValidationControllerFrequencyInSeconds(3600);
      }

      LOGGER.info("Executing command: " + toString());
      final ControllerStarter starter = new ControllerStarter(conf);

      starter.start();

      String pidFile = ".pinotAdminController-" + String.valueOf(System.currentTimeMillis()) + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting controller, exiting.", e);
      System.exit(-1);
      return false;
    }
  }

  @Override
  ControllerConf readConfigFromFile(String configFileName) throws ConfigurationException {
    ControllerConf conf = null;

    if (configFileName == null) {
      return null;
    }

    File configFile = new File(_configFileName);
    if (!configFile.exists()) {
      return null;
    }

    conf = new ControllerConf(configFile);
    return (validateConfig(conf)) ? conf : null;
  }

  private boolean validateConfig(ControllerConf conf) {
    if (conf == null) {
      LOGGER.error("Error: Null conf object.");
      return false;
    }

    if (conf.getControllerHost() == null) {
      LOGGER.error("Error: missing hostname, please specify 'controller.host' property in config file.");
      return false;
    }

    if (conf.getControllerPort() == null) {
      LOGGER.error("Error: missing controller port, please specify 'controller.port' property in config file.");
      return false;
    }

    if (conf.getZkStr() == null) {
      LOGGER.error("Error: missing Zookeeper address, please specify 'controller.zk.str' property in config file.");
      return false;
    }

    if (conf.getHelixClusterName() == null) {
      LOGGER.error(
          "Error: missing helix cluster name, please specify 'controller.helix.cluster.name' property in config file.");
      return false;
    }

    return true;
  }
}
