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

import static org.apache.pinot.common.utils.CommonConstants.Helix.PINOT_SERVICE_ROLE;

import java.io.File;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.service.PinotServiceManager;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartPinotService command.
 *
 */
public class StartServiceManagerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartServiceManagerCommand.class);
  private final List<Map<String, Object>> _bootstrapConfigurations = new ArrayList<>();
  private final String[] BOOTSTRAP_SERVICES = new String[]{"CONTROLLER", "BROKER", "SERVER"};

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;
  @Option(name = "-zkAddress", required = true, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;
  @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;
  @Option(name = "-port", required = true, metaVar = "<int>", usage = "Pinot service manager admin port, -1 means disable, 0 means a random available port.")
  private int _port;
  @Option(name = "-bootstrapConfigPaths", handler = StringArrayOptionHandler.class, required = false, usage = "A list of Pinot service config file paths. Each config file requires an extra config: 'pinot.service.role' to indicate which service to start.", forbids = {"-bootstrapServices"})
  private String[] _bootstrapConfigPaths;
  @Option(name = "-bootstrapServices", handler = StringArrayOptionHandler.class, required = false, usage = "A list of Pinot service roles to start with default config. E.g. CONTROLLER/BROKER/SERVER", forbids = {"-bootstrapConfigPaths"})
  private String[] _bootstrapServices = BOOTSTRAP_SERVICES;

  private PinotServiceManager _pinotServiceManager;

  public String getZkAddress() {
    return _zkAddress;
  }

  public StartServiceManagerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public StartServiceManagerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public int getPort() {
    return _port;
  }

  public StartServiceManagerCommand setPort(int port) {
    _port = port;
    return this;
  }

  public String[] getBootstrapConfigPaths() {
    return _bootstrapConfigPaths;
  }

  public StartServiceManagerCommand setBootstrapConfigPaths(String[] bootstrapConfigPaths) {
    _bootstrapConfigPaths = bootstrapConfigPaths;
    return this;
  }

  public String[] getBootstrapServices() {
    return _bootstrapServices;
  }

  public StartServiceManagerCommand setBootstrapServices(String[] bootstrapServices) {
    _bootstrapServices = bootstrapServices;
    return this;
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  public void setHelp(boolean help) {
    _help = help;
  }

  @Override
  public String getName() {
    return "StartPinotService";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder()
        .append("StartServiceManager -clusterName " + _clusterName + " -zkAddress " + _zkAddress + " -port " + _port);
    if (_bootstrapConfigPaths != null) {
      sb.append(" -bootstrapConfigPaths " + Arrays.toString(_bootstrapConfigPaths));
    } else if (_bootstrapServices != null) {
      sb.append(" -bootstrapServices " + Arrays.toString(_bootstrapServices));
    }

    return sb.toString();
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Start the Pinot Service Process at the specified port.";
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      _pinotServiceManager = new PinotServiceManager(_zkAddress, _clusterName, _port);
      _pinotServiceManager.start();
      if (_bootstrapConfigPaths != null) {
        for (String configPath : _bootstrapConfigPaths) {
          _bootstrapConfigurations.add(readConfigFromFile(configPath));
        }
      } else if (_bootstrapServices != null) {
        for (String service : _bootstrapServices) {
          ServiceRole serviceRole = ServiceRole.valueOf(service.toUpperCase());
          addBootstrapService(serviceRole, getDefaultConfig(serviceRole));
        }
      }
      for (Map<String, Object> properties : _bootstrapConfigurations) {
        startPinotService(properties);
      }
      String pidFile = ".pinotAdminService-" + System.currentTimeMillis() + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting pinot service, exiting.", e);
      System.exit(-1);
      return false;
    }
  }

  private Map<String, Object> getDefaultConfig(ServiceRole serviceRole)
      throws SocketException, UnknownHostException {
    switch (serviceRole) {
      case CONTROLLER:
        return PinotConfigUtils.generateControllerConf(_zkAddress, _clusterName, null, DEFAULT_CONTROLLER_PORT, null,
            ControllerConf.ControllerMode.DUAL, true);
      case BROKER:
        return PinotConfigUtils.generateBrokerConf(CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);
      case SERVER:
        return PinotConfigUtils.generateServerConf(null, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT,
            CommonConstants.Server.DEFAULT_ADMIN_API_PORT, null, null);
      default:
        throw new RuntimeException("No default config found for service role: " + serviceRole);
    }
  }

  private void startPinotService(Map<String, Object> properties) {
    startPinotService(ServiceRole.valueOf(properties.get(PINOT_SERVICE_ROLE).toString()), properties);
  }

  public boolean startPinotService(ServiceRole role, Map<String, Object> properties) {
    try {
      String instanceId = _pinotServiceManager.startRole(role, properties);
      LOGGER.info("Started Pinot [{}] Instance [{}].", role, instanceId);
    } catch (Exception e) {
      LOGGER.error(String.format("Failed to start a [ %s ] Service", role), e);
      return false;
    }
    return true;
  }

  public StartServiceManagerCommand addBootstrapService(ServiceRole role, Map<String, Object> properties) {
    properties.put(PINOT_SERVICE_ROLE, role.toString());
    
    _bootstrapConfigurations.add(properties);
    
    return this;
  }
}
