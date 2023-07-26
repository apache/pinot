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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement StartController command.
 *
 */
@CommandLine.Command(name = "StartController")
public class StartControllerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartControllerCommand.class);

  @CommandLine.Option(names = {"-controllerMode"}, description = "Pinot controller mode.")
  private ControllerConf.ControllerMode _controllerMode = ControllerConf.ControllerMode.DUAL;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, description = "Print this message.")
  private boolean _help = false;

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false,
      description = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-dataDir"}, required = false, description = "Path to directory containging data.")
  private String _dataDir = PinotConfigUtils.TMP_DIR + "data/PinotController";

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster name.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @CommandLine.Option(names = {"-configFileName", "-config", "-configFile", "-controllerConfig", "-controllerConf"},
      required = false, description = "Controller Starter config file")
      // TODO support:
      // forbids = {"-controllerHost", "-controllerPort", "-dataDir", "-zkAddress", "-clusterName", "-controllerMode"})
  private String _configFileName;

  // This can be set via the set method, or via config file input.
  private boolean _tenantIsolation = true;

  @CommandLine.Option(names = {"-configOverride"}, required = false, split = ",")
  private Map<String, Object> _configOverrides = new HashMap<>();

  @Override
  public boolean getHelp() {
    return _help;
  }

  public ControllerConf.ControllerMode getControllerMode() {
    return _controllerMode;
  }

  public String getControllerHost() {
    return _controllerHost;
  }

  public String getControllerPort() {
    return _controllerPort;
  }

  public String getDataDir() {
    return _dataDir;
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public String getConfigFileName() {
    return _configFileName;
  }

  public boolean isTenantIsolation() {
    return _tenantIsolation;
  }

  public Map<String, Object> getConfigOverrides() {
    return _configOverrides;
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
      Map<String, Object> controllerConf = getControllerConf();
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.CONTROLLER, controllerConf);
      if (startServiceManagerCommand.execute()) {
        String pidFile = ".pinotAdminController-" + System.currentTimeMillis() + ".pid";
        savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting controller, exiting.", e);
    }
    System.exit(-1);
    return false;
  }

  protected Map<String, Object> getControllerConf()
      throws ConfigurationException, SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    if (_configFileName != null) {
      properties.putAll(PinotConfigUtils.generateControllerConf(_configFileName));
      ControllerConf conf = new ControllerConf(properties);

      // Override the zkAddress and clusterName to ensure ServiceManager is connecting to the right Zookeeper and
      // Cluster.
      // Configs existence is already verified.
      _zkAddress = conf.getZkStr();
      _clusterName = conf.getHelixClusterName();
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
