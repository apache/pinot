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

import java.io.File;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartController command.
 *
 */
public class StartControllerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartControllerCommand.class);
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Path to directory containging data.")
  private String _dataDir = CURRENT_USER_DIR + "data/PinotController";

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @Option(name = "-controllerMode", required = false, metaVar = "<String>", usage = "Pinot controller mode.")
  private ControllerConf.ControllerMode _controllerMode = ControllerConf.ControllerMode.DUAL;

  @Option(name = "-configFileName", required = false, aliases = {"-config", "-configFile", "-controllerConfig", "-controllerConf"}, metaVar = "<FilePathName>", usage = "Controller Starter config file", forbids = {"-controllerHost", "-controllerPort", "-dataDir", "-zkAddress", "-clusterName", "-controllerMode"})
  private String _configFileName;

  // This can be set via the set method, or via config file input.
  private boolean _tenantIsolation = true;

  private Map<String, Object> _configOverrides = new HashMap<>();

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

  public StartControllerCommand setConfigOverrides(Map<String, Object> configs) {
    _configOverrides = configs;
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
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.CONTROLLER, getControllerConf());
      startServiceManagerCommand.execute();
      String pidFile = ".pinotAdminController-" + System.currentTimeMillis() + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting controller, exiting.", e);
      System.exit(-1);
      return false;
    }
  }

  private Map<String, Object> getControllerConf()
      throws ConfigurationException, SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    if (_configFileName != null) {
      properties.putAll(PinotConfigUtils.generateControllerConf(_configFileName));
    } else {
      if (_controllerHost == null) {
        _controllerHost = NetUtils.getHostAddress();
      }
      properties.putAll(PinotConfigUtils
          .generateControllerConf(_zkAddress, _clusterName, _controllerHost, _controllerPort, _dataDir, _controllerMode,
              _tenantIsolation));
    }
    if (_configOverrides != null) {
      properties.putAll(_configOverrides);
    }
    return properties;
  }
}
